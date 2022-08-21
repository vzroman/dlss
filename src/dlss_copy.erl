%%----------------------------------------------------------------
%% Copyright (c) 2022 Faceplate
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------

-module(dlss_copy).

-include("dlss.hrl").
-include("dlss_copy.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  copy/2,copy/3,
  split/2, split/3,
  get_size/1
]).

%%=================================================================
%%	Remote API
%%=================================================================
-export([
  remote_copy_request/1,
  remote_batch/3
]).

-export([
  debug/2
]).

%%-----------------------------------------------------------------
%%  Internals
%%-----------------------------------------------------------------
init_props( Source )->
  All =
    maps:from_list(mnesia:table_info( Source, all )),
  maps:to_list( maps:with(?PROPS,All)).

init_source(Source, Module, Options)->
  S = Module:init_source( Source, Options ),
  S#source{name = Source,module = Module}.

init_target(Target, Source, Module, Options)->
  case lists:member( Target, dlss:get_local_segments()) of
    true ->
      % It's not add_copy, simple copy
      T = Module:init_target(Target,Options),
      T#target{ name = Target, module = Module, trick = false };
    _->
      % TRICK mnesia, Add a copy to this node before ask mnesia to add it
      Props = init_props(Source),
      Module:init_copy( Target, Props ),

      T0 = Module:init_target(Target,Options),
      T1 = T0#target{ name = Target, module = Module, trick = true },

      % Set the guard to rollback target if I die
      Me = self(),
      Guard = spawn_link(fun()->
        process_flag(trap_exit,true),
        receive
          {commit,Me}->ok;
          {rollback,Me}->ok;
          {'EXIT',Me,Reason}->
            ?LOGERROR("~p receiver died, reason ~p",[?LOG_LOCAL(Source,Target),Reason]),
            Module:drop_target( Target )
        end
      end),

      T1#target{guard = Guard}
  end.

commit_target(#target{module = Module, guard = Guard } = T)->
  Module:dump_target( T ),
  if
    is_pid( Guard ), Guard=/=self() -> Guard ! {commit,self()} ;
    true -> ok
  end.


rollback_target(#target{trick = true, module = Module,name = Target, guard = Guard} )->
  Module:drop_target( Target ),
  Guard ! {rollback,self()};
rollback_target(_T)->
  ok.

-record(acc,{acc, source_ref, batch, size, on_batch }).
fold(#source{module = Module} = SourceRef, OnBatch, InAcc)->

  Acc0 = #acc{
    source_ref = SourceRef,
    batch = [],
    acc = InAcc,
    size = 0,
    on_batch = OnBatch
  },

  case try Module:fold(SourceRef, fun iterator/2, Acc0)
  catch
   _:{stop,Stop}-> Stop;
   _:{final,Final}->{final,Final}
  end of
    #acc{batch = [], acc = FinalAcc}-> FinalAcc;
    #acc{batch = Tail, size = Size, acc = TailAcc, on_batch = OnBatch}->
      OnBatch(Tail, Size, TailAcc);
    {final,FinalAcc}-> FinalAcc
  end.
%----------------------WITH STOP KEY-----------------------------
iterator({K,V},#acc{
  source_ref = #source{module = Module, stop = Stop},
  batch = Batch,
  size = Size0
} = Acc)
  when Size0 < ?BATCH_SIZE, Stop =/= undefined, K < Stop->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{
  source_ref = #source{module = Module, stop = Stop},
  batch = Batch,
  on_batch = OnBatch,
  acc = InAcc0,
  size = Size0
} = Acc)
  when Stop =/= undefined, K < Stop->

  {Action,Size} = Module:action({K,V}),

  Acc#acc{batch = [Action], acc = OnBatch(Batch, Size0, InAcc0), size = Size};

% stop key reached
iterator(_Rec,#acc{source_ref = #source{stop = Stop}} = Acc)
  when Stop =/= undefined->
  throw({stop, Acc});

%----------------------NO STOP KEY-----------------------------
iterator({K,V},#acc{
  source_ref = #source{module = Module},
  batch = Batch,
  size = Size0
} = Acc)
  when Size0 < ?BATCH_SIZE->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{
  source_ref = #source{module = Module},
  batch = Batch,
  on_batch = OnBatch,
  size = Size0,
  acc = InAcc0
} = Acc) ->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action], acc =OnBatch(Batch, Size0, InAcc0), size = Size}.

%%=================================================================
%%	API
%%=================================================================
copy(Source, Target )->
  copy(Source, Target, #{}).
copy( Source, Target, Options0 )->

  Options = ?OPTIONS(Options0),
  {Module,ReadNode} = dlss_segment:module_node( Source ),

  if
    ReadNode =:= node() ->
      local_copy(Source, Target, Module, Options );
    true ->
      remote_copy(Source, Target, Module, Options )
  end.

%---------------------------------------------------------------------
% LOCAL COPY
%---------------------------------------------------------------------
local_copy( Source, Target, Module, #{
  hash := InitHash0
} = Options)->

  TargetRef = init_target( Target, Source, Module, Options),
  Log = ?LOG_LOCAL(Source,Target),

  OnBatch =
    fun(Batch, Size, Hash)->
      ?LOGINFO("~p write batch, size ~s, length ~s",[
        Log,
        ?PRETTY_SIZE(Size),
        ?PRETTY_COUNT(length(Batch))
      ]),
      Module:write_batch(Batch, TargetRef),
      crypto:hash_update(Hash, term_to_binary( Batch ))
    end,

  SourceRef = init_source( Source, Module, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  FinalHash0 = fold(SourceRef, OnBatch, InitHash ),
  FinalHash = crypto:hash_final( FinalHash0 ),

  commit_target( TargetRef ),

  ?LOGINFO("~p finish, hash ~s",[
    Log,
    ?PRETTY_HASH(FinalHash)]),

  FinalHash.

%---------------------------------------------------------------------
% REMOTE COPY
%---------------------------------------------------------------------
%                               Types
-record(r_copy,{ send_node, source, target, module, options, attempts, error }).
-record(r_acc,{ sender, target_ref, live, hash, tail_key, log }).
-record(s_acc,{ receiver, source_ref, hash, log, size, batch}).

remote_copy(Source, Target, Module, #{attempts:=Attempts} = Options )->
  remote_copy(#r_copy{
    send_node = dlss_segment:read_node( Source ),
    source = Source,
    target = Target,
    module = Module,
    options = Options,
    attempts = Attempts,
    error = undefined
  }).

remote_copy(#r_copy{ send_node = undefined })->
  throw(unavailable);
remote_copy(#r_copy{ send_node = Node }) when Node =:= node()->
  throw(already_copied);

remote_copy(#r_copy{
  send_node = SendNode,
  source = Source,
  target = Target,
  module = Module,
  attempts = Attempts,
  options = #{hash := InitHash0} = Options,
  error = undefined
} = State) when Attempts > 0->

  Log = ?LOG_RECEIVE(SendNode,Source,Target),
  ?LOGINFO("~p attempt",[Log]),

  TargetRef = init_target( Target, Source, Module, Options ),
  % Check if it's a live copy
  Live = prepare_live_copy( Source, Log ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  ?LOGINFO("~p init sender",[Log]),
  SenderAgs = #{
    receiver => self(),
    source => Source,
    log => ?LOG_SEND(Source,node(),Target),
    options => Options
  },
  Sender = spawn_link(SendNode, ?MODULE, remote_copy_request,[SenderAgs]),

  % The remote sender needs a confirmation of the previous batch before it sends the next one
  Sender ! {confirmed, self()},

  FinalHash=
    try receive_loop(#r_acc{
      sender = Sender,
      target_ref = TargetRef,
      live = Live,
      hash = InitHash,
      log = Log
    }) catch
      _:Error:Stack->
        ?LOGERROR("~p attempt failed ~p, left attempts ~p",[Log,Error,Attempts-1]),

        exit(Sender,shutdown),
        drop_live_copy(Source, Live ),
        rollback_target( TargetRef ),

        remote_copy(State#r_copy{attempts = Attempts - 1, error ={Error,Stack} })
    end,

  finish_live_copy( Live, TargetRef, Log ),

  commit_target( TargetRef ),

  ?LOGINFO("~p finish hash ~s", [Log, ?PRETTY_HASH( FinalHash )]),

  FinalHash;
remote_copy(#r_copy{source = Source, target = Target,error = {Error,Stack}})->
  ?LOGERROR("~p copy to ~p failed, no attempts left, last error ~p, stack ~p",[
    Source, Target, Error, Stack
  ]),
  throw( Error ).

%----------------------Receiver---------------------------------------
receive_loop(#r_acc{
  sender = Sender,
  target_ref = #target{module = Module} = TargetRef,
  hash = Hash0,
  log = Log
} = Acc0 )->
  receive
    {write_batch, Sender, ZipBatch, ZipSize, SenderHash }->

      ?LOGINFO("~p batch received size ~s, hash ~s",[
        Log,
        ?PRETTY_SIZE(ZipSize),
        ?PRETTY_HASH(SenderHash)
      ]),
      {BatchList,Hash} = unzip_batch( lists:reverse(ZipBatch), {[],Hash0}),

      % Check hash
      case crypto:hash_final(Hash) of
        SenderHash -> Sender ! {confirmed, self()};
        LocalHash->
          ?LOGERROR("~p invalid sender hash ~s, local hash ~s",[
            Log,
            ?PRETTY_HASH(SenderHash),
            ?PRETTY_HASH(LocalHash)
          ]),
          Sender ! {invalid_hash, self()},
          throw(invalid_hash)
      end,

      % Dump batch
      [TailKey|_] =
        [ begin
            Batch = binary_to_term( BatchBin ),
            BTailKey = Module:get_key( hd(Batch) ),
            ?LOGINFO("~p write batch size ~s, length ~p, last key ~p",[
              Log,
              ?PRETTY_SIZE(size( BatchBin )),
              ?PRETTY_COUNT(length(Batch)),
              Module:decode_key( BTailKey )
            ]),

            Module:write_batch(Batch, TargetRef),
            % Return batch tail key
            BTailKey
          end || BatchBin <- BatchList ],

      Acc = Acc0#r_acc{hash = Hash, tail_key = TailKey},

      % Roll over stockpiled live updates (if it's a live copy)
      roll_live_updates( Acc ),

      receive_loop( Acc0#r_acc{hash = Hash, tail_key = TailKey});

    {lock_timeout, Sender}->
      ?LOGINFO("~p sender is unable to set lock, source is under transformation, wait...."),
      receive_loop( Acc0 );
    {finish, Sender, SenderFinalHash }->
      % Finish
      ?LOGINFO("~p sender finished, final hash ~s",[Log, ?PRETTY_HASH(SenderFinalHash)]),
      case crypto:hash_final(Hash0) of
        SenderFinalHash ->
          % Everything is fine!
          SenderFinalHash;
        LocalFinalHash->
          ?LOGERROR("~p invalid sender final hash ~s, local final hash ~s",[
            Log,
            ?PRETTY_HASH(SenderFinalHash),
            ?PRETTY_HASH(LocalFinalHash)
          ]),
          throw(invalid_hash)
      end;
    {error,Sender,SenderError}->
      ?LOGERROR("~p sender error ~p",[Log,SenderError]),
      throw({interrupted,SenderError});
    {'EXIT',Sender,Reason}->
      throw({interrupted,Reason});
    {'EXIT',_Other,Reason}->
      throw({exit,Reason})
  end.

unzip_batch( [Zip|Rest], {Acc0,Hash0})->
  Batch = zlib:unzip( Zip ),
  Hash = crypto:hash_update(Hash0, Batch),
  unzip_batch(Rest ,{[Batch|Acc0], Hash});
unzip_batch([], Acc)->
  Acc.

%----------------------Sender---------------------------------------
remote_copy_request(#{
  receiver := Receiver,
  source := Source,
  log := Log,
  options := #{ hash:= InitHash0}= Options
})->
  ?LOGINFO("~p request options ~p", [
    Log, Options
  ]),

  % Monitor
  spawn_link(fun()->
    process_flag(trap_exit,true),
    receive
      {'EXIT',_,Reason} when Reason=:=normal; Reason =:= shutdown->
        ok;
      {'EXIT',_,Reason}->
        ?LOGERROR("~p interrupted, reason ~p",[Log,Reason])
    end
  end),

  ?LOGINFO("~p set source lock...",[Log]),
  Unlock = set_lock( Source, Log, Receiver ),

  ?LOGINFO("~p source locked, start copying",[Log]),
  {Module,_} = dlss_segment:module_node( Source ),

  SourceRef = init_source( Source, Module, Options ),
  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  InitState = #s_acc{
    receiver = Receiver,
    source_ref = SourceRef,
    hash = InitHash,
    log = Log,
    size = 0,
    batch = []
  },

  try
      #s_acc{ batch = TailBatch, hash = TailHash } = TailState =
        fold(SourceRef, fun remote_batch/3, InitState ),

      % Send the tail batch if exists
      case TailBatch of [] -> ok; _->send_batch( TailState ) end,

      FinalHash = crypto:hash_final( TailHash ),

      ?LOGINFO("~p finished, final hash ~p",[Log, ?PRETTY_HASH]),
      Receiver ! {finish, self(), FinalHash}

  catch
    _:Error:Stack->
      ?LOGERROR("~p error ~p, stack ~p",[Log,Error,Stack]),
      Receiver ! {error, self(), Error}
  after
      % Release the source lock
      Unlock()
  end.

set_lock(Source, Log, Receiver)->
  case dlss_storage:lock_segment(Source, _Lock = write, 5000) of
    {ok,Unlock} -> Unlock;
    {error,timeout}->
      ?LOGINFO("~p unable to set lock the source is under transformation wait...",[Log]),
      Receiver ! {lock_timeout, self()},
      set_lock(Source, Log, Receiver);
    {error, Other}->
      throw( Other )
  end.

% Zip and stockpile local batches until they reach ?REMOTE_BATCH_SIZE
remote_batch(Batch0, Size, #s_acc{
  size = TotalZipSize0,
  batch = ZipBatch,
  hash = Hash0,
  log = Log
} = State) when TotalZipSize0 < ?REMOTE_BATCH_SIZE->

  Batch = term_to_binary( Batch0 ),
  Hash = crypto:hash_update(Hash0, Batch),
  Zip = zlib:zip( Batch ),

  ZipSize = size(Zip),
  TotalZipSize = TotalZipSize0 + ZipSize,

  ?LOGINFO("~p add zip: size ~s, zip size ~p, total zip size ~p",[
    Log, ?PRETTY_SIZE(Size), ?PRETTY_SIZE(ZipSize), ?PRETTY_SIZE(TotalZipSize)
  ]),

  State#{
    size => TotalZipSize,
    batch => [Zip|ZipBatch],
    hash => Hash
  };

% The batch is ready, send it
remote_batch(Batch0, Size, #s_acc{
  receiver = Receiver
}=State)->
  % First we have to receive a confirmation of the previous batch
  receive
    {confirmed, Receiver}->
      send_batch( State ),
      remote_batch(Batch0, Size,State#s_acc{ batch = [], size = 0 });
    {invalid_hash,Receiver}->
      throw(invalid_hash)
  end.

send_batch(#s_acc{
  size = ZipSize,
  batch = ZipBatch,
  hash = Hash,
  receiver = Receiver,
  log = Log
})->

  BatchHash = crypto:hash_final(Hash),
  ?LOGINFO("~p send batch: zip size ~s, length ~p, hash ~s",[
    Log,
    ?PRETTY_SIZE(ZipSize),
    length(ZipBatch),
    ?PRETTY_HASH(BatchHash)
  ]),
  Receiver ! {write_batch, self(), ZipBatch, ZipSize, BatchHash }.


%%===========================================================================
%% LIVE COPY
%%===========================================================================
prepare_live_copy( Source, Log )->
  AccessMode = dlss_segment:access_mode( Source ),
  if
    AccessMode =:= read_write->
      ?LOGINFO("--------------------~s LIVE COPY----------------------------",[string:uppercase(Log)]),
      ?LOGINFO("~p subscribe....",[Source]),
      case dlss_subscription:subscribe( Source ) of
        ok->
          % Success
          ?LOGINFO("subscribed on ~p",[Source]),
          % Prepare the storage for live updates anyway to avoid excessive check during the copying
          ets:new(live,[private,ordered_set]);
        {error,Error}->
          throw({subscribe_error,Error})
      end;
    true->
      false
  end.

finish_live_copy(false = _Live, _TargetRef, _Log)->
  ok;
finish_live_copy( Live, TargetRef, Log)->
% Give away live updates to another process until the mnesia is ready
  give_away_live_updates(Live, TargetRef, Log),
  ets:delete( Live ).

drop_live_copy(_Source, false =_Live)->
  ok;
drop_live_copy(Source, Live)->
  dlss_subscription:unsubscribe( Source ),
  ets:delete( Live ).

roll_live_updates(#r_acc{ live = false })->
  ok;
roll_live_updates(#r_acc{
  target_ref = #target{ module = Module, name = Target } = TargetRef,
  live = Live,
  tail_key = TailKey,
  log = Log
})->

  % First we flush subscriptions and roll them over already stockpiled actions,
  % Then we take only those actions that are in the copy keys range already
  % because the next batch may not contain the update yet
  % and so will overwrite came live update.
  % Timeout 0 because we must to receive the next remote batch as soon as possible
  Updates = flush_subscriptions( Target, _Timeout = 0 ),
  ?LOGINFO("~p live updates count ~p",[
    Log,
    ?PRETTY_COUNT(length(Updates))
  ]),

  % Convert the updates into the actions
  Actions =
    [ Module:live_action(U) || U <- Updates ],

  % Put them into the live ets to sort them and also overwrite old updates to avoid excessive
  % writes to the copy
  true = ets:insert(Live, Actions),

  % Take out the actions that are in the copy range already
  Head = take_head(ets:first(Live), Live, TailKey),
  ?LOGINFO("~p actions to add to the copy ~p, stockpiled ~p",[
    Log,
    ?PRETTY_COUNT(length(Head)),
    ?PRETTY_COUNT(ets:info(Live,size))
  ]),

  Module:write_batch(Head, TargetRef).

flush_subscriptions(Target, Timeout)->
  receive
    {subscription, Target, Update}->
      [Update | flush_subscriptions( Target, Timeout )]
  after
    Timeout->[]
  end.

take_head(K, Live, TailKey ) when K =/= '$end_of_table', K =< TailKey->
  [{_,Action}] = ets:take(Live, K),
  [Action| take_head(ets:next(Live,K), Live, TailKey)];
take_head(_K, _Live, _TailKey)->
  [].

%---------------------------------------------------------------------
% The remote copy has finished, but there can be live updates
% in the queue that we must not lose. We cannot wait for them
% because the copier (storage server) have to add the table
% to the mnesia schema. Until it does it the updates will
% not go to the local copy. Therefore we start another process
% write tail updates to the copy
%---------------------------------------------------------------------
give_away_live_updates(Live, #target{ name = Target } = TargetRef, Log)->

  Giver = self(),
  Taker =
    spawn_link(fun()->
      ok = dlss_subscription:subscribe( Target ),
      Giver ! {ready, self() },

      ?LOGINFO("~p live updates has taken ~p",[Log,self()]),
      wait_ready(TargetRef, Log, dlss_segment:have( Target ))

    end),

  ?LOGINFO("~p give away live updates from ~p to ~p",[Log, self(), Taker]),

  receive {ready,Taker}->ok end,

  % From now the Taker receives updates I can unsubscribe, and wait
  % for my tail updates
  dlss_subscription:unsubscribe( Target ),

  ?LOGINFO("~p: roll over tail live updates",[Log]),
  roll_tail_updates( Live, TargetRef, Log).

roll_tail_updates( Live, #target{ module = Module, name = Target } = TargetRef, Log )->

  % Timeout because I have already unsubscribed and it's a finite process
  Updates = flush_subscriptions( Target, ?FLUSH_TAIL_TIMEOUT ),
  ?LOGINFO("~p tail updates count ~p",[
    Log,
    ?PRETTY_COUNT(length(Updates))
  ]),

  % Convert the updates into the actions
  Actions =
    [ Module:live_action(U) || U <- Updates ],

  % Put them into the live ets to sort them and also overwrite old updates to avoid excessive
  % writes to the copy
  true = ets:insert(Live, Actions),

  Tail = ets:tab2list( Live ),

  Module:write_batch(Tail, TargetRef).

wait_ready(#target{name = Target, module = Module} = TargetRef, Log, true) ->
  ?LOGINFO("~p copy attached to the schema, flush tail subscriptions",[Log]),

  dlss_subscription:unsubscribe( Target ),

  Updates = flush_subscriptions( Target, ?FLUSH_TAIL_TIMEOUT ),

  Actions =
    [ Module:live_action(U) || U <- Updates ],

  Module:write_batch(Actions, TargetRef),

  ?LOGINFO("~p live copy is ready",[Log]);

wait_ready(#target{name = Target, module = Module} = TargetRef, Log, false)->
  % The copy is not ready yet
  Updates = flush_subscriptions( Target, _Timeout = 0 ),

  Actions =
    [ Module:live_action(U) || U <- Updates ],

  ?LOGINFO("~p write tail updates count ~p",[
    Log,
    ?PRETTY_COUNT(length(Actions))
  ]),

  Module:write_batch(Actions, TargetRef),

  ?LOGINFO("~p live copy is not attached yet"),
  timer:sleep(?CHECK_LIVE_READY_TIMER),

  wait_ready(TargetRef, Log, dlss_segment:have( Target )).

%------------------SPLIT---------------------------------------
% Splits are always local
%--------------------------------------------------------------
%                 Types
-record(split_l,{module, key, hash, total_size}).
-record(split_r,{i, module, size, key}).

split( Source, Target )->
  split( Source, Target, #{}).
split( Source, Target, Options0 )->

  Options = ?OPTIONS( Options0 ),
  Module = get_module( Source ),

  SourceRef = init_source( Source, Module, Options ),
  TargetRef = init_target(Target, Source, Module, Options),

  % Target considered to be empty
  InitHash = crypto:hash_update(crypto:hash_init(sha256),<<>>),
  InitAcc = #split_l{
    module = Module,
    total_size = 0,
    hash = InitHash
  },

  % Init the reverse iterator
  Owner = self(),
  Reverse =
    spawn_link(fun()->
      Module:init_reverse(Source,
        fun
          ('$end_of_table')->
            Owner ! {'$end_of_table', self()};
          ({I,TSize,TKey})->
            Owner ! {started, self()},
            Acc0 = #split_r{i = I, module= Module, size= TSize, key=TKey},
            reverse_loop(Owner,Acc0)
        end)
    end),

  #split_l{ hash = FinalHash0, key = SplitKey0 } =
    receive
      {started, Reverse}->
        do_split(Reverse, Module, Source, Target, SourceRef, TargetRef, InitAcc );
      {'$end_of_table', Reverse}->
        ?LOGWARNING("~p is empty, skip split",[Source]),
        throw('$end_of_table')
    end,

  FinalHash = crypto:hash_final( FinalHash0 ),
  SplitKey = Module:decode_key(SplitKey0),

  Module:dump_source( SourceRef ),
  commit_target( TargetRef ),

  ?LOGINFO("split finish: source ~p, target ~p, split key ~p, hash ~s",[
    Source,
    Target,
    SplitKey,
    ?PRETTY_HASH( FinalHash )
  ]),

  {SplitKey,FinalHash}.

do_split(Reverse, Module, Source, Target, SourceRef, TargetRef, Acc0)->

  OnBatch =
    fun(Batch, Size, #split_l{hash = Hash, total_size = Total} = Acc)->

      % Enter the reverse loop
      HKey = Module:get_key(hd(Batch)),
      receive
        {key, TKey, Reverse} when TKey > HKey->
          Reverse ! { next, self() },

          ?LOGINFO("DEBUG: split ~p, target ~p, write batch size ~s, length ~s, total size ~s, hkey ~p, tkey ~p",[
            Source,
            Target,
            ?PRETTY_SIZE(Size),
            ?PRETTY_COUNT(length(Batch)),
            ?PRETTY_SIZE(Total+Size),
            Module:decode_key(HKey),
            Module:decode_key(TKey)
          ]),

          Module:write_batch(Batch, TargetRef),
          Module:drop_batch(Batch, SourceRef),

          Acc#split_l{ hash = crypto:hash_update(Hash, term_to_binary( Batch )), total_size = Total + Size};
        {key, _TKey, Reverse}->
          %% THIS IS THE MEDIAN!
          Reverse ! {stop, self() },
          throw({final,Acc#split_l{ key = Module:get_key( lists:last(Batch) )}});
        {'$end_of_table', Reverse}->
          % The segment is smaller than the batch size?
          throw({final,Acc#split_l{ key = Module:get_key( lists:last(Batch) )}})
      end
    end,

  % Run split
  do_copy(SourceRef, Module, OnBatch, Acc0).

reverse_loop(Owner, #split_r{i=I, module = Module, size = Size0, key = Key} = Acc)
  when Size0 < ?BATCH_SIZE->
  case Module:reverse(I, Key) of
    {K, Size} ->
      reverse_loop(Owner, Acc#split_r{size = Size0 + Size, key = K});
    '$end_of_table' ->
      % Can we reach here?
      Owner ! {'$end_of_table', self()}
  end;
% Batch size reached
reverse_loop(Owner, #split_r{key = Key} = Acc)->
  Owner ! {key, Key, self()},
  receive
    { next, Owner }->
      reverse_loop(Owner, Acc#split_r{size = 0});
    {stop, Owner }->
      ok
  end.

get_size( Segment )->

  Module = get_module( Segment ),
  ReadNode = dlss_segment:where_to_read(Segment),
  if
    ReadNode =:= node()->
      Module:get_size( Segment );

    true->
      case rpc:call(ReadNode, Module, get_size, [ Segment ]) of
        {badrpc, _Error} -> -1;
        Result -> Result
      end
  end.

debug(Storage, Count)->
  spawn(fun()->fill(Storage, Count) end).

fill(S,C) when C>0 ->
  if C rem 100000 =:= 0-> ?LOGINFO("DEBUG: write ~p",[C]); true->ignore end,
  dlss:dirty_write(S, {x, erlang:phash2({C}, 200000000), erlang:phash2({C}, 200000000)}, {y, binary:copy(integer_to_binary(C), 100)}),
  fill(S,C-1);
fill(_S,_C)->
  ok.


