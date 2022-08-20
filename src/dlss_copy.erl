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
  remote_copy_request/4,
  remote_batch/3
]).

-export([
  debug/2
]).

-record(acc,{acc, module, batch, size, on_batch, stop }).

-record(split_l,{module, key, hash, total_size}).
-record(split_r,{i, module, size, key}).

-define(OPTIONS(O),maps:merge(#{
  start_key =>undefined,
  end_key => undefined,
  hash => <<>>,
  sync => false,
  attempts => 3
}, O)).

%%-----------------------------------------------------------------
%%  Utilities
%%-----------------------------------------------------------------
get_module( Table )->
  #{ type := Type } = dlss_segment:get_info(Table),
  if
    Type =:= disc -> dlss_disc;
    Type =:= ramdisc -> dlss_ramdisc;
    true -> dlss_ram
  end.

init_props( Source )->
  All =
    maps:from_list(mnesia:table_info( Source, all )),
  maps:to_list( maps:with(?PROPS,All)).

get_read_node( Table )->
  Node = dlss_segment:where_to_read(Table),
  if
    Node =:= nowhere ->throw({unavailable,Table});
    true-> Node
  end.

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
            ?LOGERROR("~p local receiver died, reason ~p",[Target,Reason]),
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

%%=================================================================
%%	API
%%=================================================================
copy( Source, Target )->
  copy( Source, Target, #{}).
copy( Source, Target, Options0 )->
  Options = ?OPTIONS(Options0),

  Module = get_module( Source ),
  ReadNode = get_read_node( Source),

  if
    ReadNode =:= node() ->
      ?LOGINFO("~p copy to ~p, module ~p, options ~p",[Source, Target, Module, Options]),
      local_copy(Source, Target, Module, Options );
    true ->
      remote_copy(Source, Target, Module, Options )
  end.

local_copy( Source, Target, Module, #{
  hash := InitHash0
} = Options)->

  TargetRef = init_target( Target, Source, Module, Options),

  OnBatch =
    fun(Batch, Size, Hash)->
      ?LOGINFO("DEBUG: ~p write batch, size ~s, length ~s",[
        Target,
        ?PRETTY_SIZE(Size),
        ?PRETTY_COUNT(length(Batch))
      ]),
      Module:write_batch(Batch, TargetRef),
      crypto:hash_update(Hash, term_to_binary( Batch ))
    end,

  SourceRef = init_source( Source, Module, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  FinalHash0 = do_copy(SourceRef, Module, OnBatch, InitHash ),
  FinalHash = crypto:hash_final( FinalHash0 ),

  commit_target( TargetRef ),

  ?LOGINFO("finish local copying: source ~p, target ~p, hash ~s",[Source, Target, ?PRETTY_HASH(FinalHash)]),

  FinalHash.


remote_copy(Source, Target, Module, #{attempts := Attempts } = Options ) when Attempts > 0->
  try remote_copy_attempt( Source, Target, Module, Options )
  catch
    _:Error:Stack->
      ?LOGERROR("~p copy attempt failed, error ~p, stack ~p, left attempts ~p",[ Source, Error, Stack, Attempts - 1]),
      remote_copy( Source, Target, Module, Options#{ attempts => Attempts - 1, error => Error})
  end;
remote_copy(_Source, _Target, _Module, #{error := Error })->
  throw( Error ).

remote_copy_attempt( Source, Target, Module, #{
  hash := InitHash0
} = Options )->

  SendNode = get_read_node( Source ),
  TargetRef = init_target( Target, Source, Module, Options ),

  Receiver = self(),
  InitState = #{
    receiver => Receiver,
    receive_node => node(),
    send_node => SendNode,
    source => Source,
    target => Target,
    hash => InitHash0,
    size => 0,
    batch => []
  },

  Sender = spawn_link(SendNode, ?MODULE, remote_copy_request,[Source,Module,Options,InitState]),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  % The remote sender needs a confirmation of the previous batch before it sends the next one
  Sender ! {confirmed, self()},

  ?LOGINFO("~p: copy from ~p, module ~p, options ~p",[Source, SendNode, Module, Options]),

  % Check if it's a live copy
  Live = prepare_live_copy( Source ),

  %-------Enter the copy loop----------------------
  FinalHash =
    try receive_copy_loop(Sender, TargetRef, #{hash => InitHash, live =>Live})
    catch
      _:Error:Stack->
        drop_live_copy(Source, Live ),
        rollback_target( TargetRef ),
        case Error of
          invalid_hash->
            ?LOGERROR("~p invalid remote hash from ~p",[Source,SendNode]);
          {interrupted,Reason}->
            ?LOGERROR("~p copying from ~p interrupted, reason ~p",[Source,SendNode,Reason]);
          Other->
            ?LOGERROR("~p copying from ~p, unexpected error: ~p ",[Source,SendNode,Other])
        end,
        throw({Error,Stack})
    after
      exit(Sender,shutdown)
    end,

  finish_live_copy( Live, TargetRef ),

  commit_target( TargetRef ),

  ?LOGINFO("finish remote copying: source ~p, target ~p, hash ~s", [Source, Target, ?PRETTY_HASH( FinalHash )] ),

  FinalHash.

remote_copy_request(Source, Module, Options, #{
  receiver := Receiver,
  receive_node := Node
} = InitState)->

  ?LOGINFO("~p remote copy request ~p, options ~p, set write lock...", [Source, Node, Options]),

  % Set lock on the source segment
  Result =
    set_lock(Source, write, Receiver,fun()->
      ?LOGINFO("~p locked, start copying to ~p",[Source,Node]),
      try send_copy_loop(Source, Module, Options, InitState)
      catch
        _:Error:Stack->
          ?LOGERROR("~p copy to ~p error ~p, stack ~p",[Source,Node,Error,Stack]),
          {error,Error}
      end
    end),

  Receiver ! {finish, self(), Result}.

set_lock(Segment, Lock, Receiver, Transaction)->
  case dlss_storage:segment_transaction(Segment, Lock, Transaction, 5000) of
    {error, lock_timeout}->
      ?LOGINFO("~p unable to set lock ~p, it's under transformation wait..."),
      Receiver ! {lock_timeout, self()},
      set_lock(Segment, Lock, Receiver, Transaction);
    Result->
      Result
  end.

send_copy_loop(Source, Module, Options, #{
  hash := InitHash0,
  receive_node := Node
} = InitState0)->
  SourceRef = init_source( Source, Module, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),
  InitState = InitState0#{
    hash => InitHash
  },

  TailState =
    #{batch := TailBatch, hash := TailHash} = do_copy( SourceRef, Module, fun remote_batch/3, InitState ),

  % Send the tail batch if exists
  case TailBatch of [] -> ok; _->send_batch( TailState ) end,

  FinalHash = crypto:hash_final( TailHash ),

  ?LOGINFO("~p finish remote copy to ~p, final hash ~s",[Source, Node, ?PRETTY_HASH(FinalHash)]),

  {ok, FinalHash}.


% Zip and stockpile local batches until they reach ?REMOTE_BATCH_SIZE
remote_batch(Batch0, Size, #{
  size := TotalZipSize0,
  batch := ZipBatch,
  hash := Hash0,
  source := Source,
  target := Target,
  receive_node := Node
}=State) when TotalZipSize0 < ?REMOTE_BATCH_SIZE->

  Batch = term_to_binary( Batch0 ),
  Hash = crypto:hash_update(Hash0, Batch),
  Zip = zlib:zip( Batch ),

  ZipSize = size(Zip),
  TotalZipSize = TotalZipSize0 + ZipSize,

  ?LOGINFO("DEBUG: add zip: source ~p, target ~p:~p, size ~s, zip size ~p, total zip size ~p",[
    Source, Node,Target, ?PRETTY_SIZE(Size), ?PRETTY_SIZE(ZipSize), ?PRETTY_SIZE(TotalZipSize)
  ]),

  State#{
    size => TotalZipSize,
    batch => [Zip|ZipBatch],
    hash => Hash
  };

% The batch is ready, send it
remote_batch(Batch0, Size, #{
  receiver := Receiver
}=State)->
  % First we have to receive a confirmation of the previous batch
  receive
    {confirmed, Receiver}->
      send_batch( State ),
      remote_batch(Batch0, Size,State#{ batch=>[], size =>0 });
    {invalid_hash,Receiver}->
      throw(invalid_hash)
  end.

send_batch(#{
  size := ZipSize,
  batch := ZipBatch,
  hash := Hash,
  source := Source,
  target := Target,
  receiver := Receiver,
  receive_node := Node
})->

  BatchHash = crypto:hash_final(Hash),
  ?LOGINFO("send batch: source ~p, target ~p:~p, zip size ~s, length ~p, hash ~s",[
    Source,
    Node,Target,
    ?PRETTY_SIZE(ZipSize),
    length(ZipBatch),
    ?PRETTY_HASH(BatchHash)
  ]),
  Receiver ! {write_batch, self(), ZipBatch, ZipSize, BatchHash }.

unzip_batch( [Zip|Rest], {Acc0,Hash0})->
  Batch = zlib:unzip( Zip ),
  Hash = crypto:hash_update(Hash0, Batch),
  unzip_batch(Rest ,{[Batch|Acc0], Hash});
unzip_batch([], Acc)->
  Acc.

receive_copy_loop(Sender, #target{module = Module, name = Target} =TargetRef, #{
  hash := Hash0,
  live := Live,
  send_node := SendNode
}=Acc)->
  receive
    {write_batch, Sender, ZipBatch, ZipSize, SenderHash }->

      ?LOGINFO("~p: batch received from ~p, size ~s, hash ~s",[
        Target,
        SendNode,
        ?PRETTY_SIZE(ZipSize),
        ?PRETTY_HASH(SenderHash)
      ]),
      {BatchList,Hash} = unzip_batch( lists:reverse(ZipBatch), {[],Hash0}),

      % Check hash
      case crypto:hash_final(Hash) of
        SenderHash -> Sender ! {confirmed, self()};
        LocalHash->
          ?LOGERROR("~p: invalid sender ~p hash ~s, local hash ~s",[
            Target,
            SendNode,
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
            ?LOGINFO("~p: write batch size ~s, length ~p, last key ~p",[
              Target,
              ?PRETTY_SIZE(size( BatchBin )),
              ?PRETTY_COUNT(length(Batch)),
              Module:decode_key( BTailKey )
            ]),

            Module:write_batch(Batch, TargetRef),
            % Return batch tail key
            BTailKey
          end || BatchBin <- BatchList ],

      % Roll over stockpiled live updates
      if
        Live =/= false->
          ?LOGINFO("~p roll over live updates...",[Target]),
          roll_live_updates( Live, TargetRef, TailKey);
        true ->
          ignore
      end,

      receive_copy_loop(Sender, TargetRef, Acc#{hash => Hash, tail_key => TailKey});
    {lock_timeout, Sender}->
      ?LOGINFO("~p sender ~p was unable to set lock, segment is under transformation, wait...."),
      receive_copy_loop(Sender, TargetRef, Acc);
    {finish,Sender,{ok,SenderFinalHash}}->
      % Finish
      ?LOGINFO("~p: sender ~p finished, final hash ~s",[Target,SendNode, ?PRETTY_HASH(SenderFinalHash)]),
      case crypto:hash_final(Hash0) of
        SenderFinalHash ->
          SenderFinalHash;
        LocalFinalHash->
          ?LOGERROR("~p: invalid sender ~p final hash ~s, local final hash ~s",[
            Target,
            SendNode,
            ?PRETTY_HASH(SenderFinalHash),
            ?PRETTY_HASH(LocalFinalHash)
          ]),
          throw(invalid_hash)
      end;
    {finish,Sender,{error,SenderError}}->
      ?LOGERROR("~p sender ~p error ~p",[Target,SendNode,SenderError]),
      throw({interrupted,SenderError});
    {'EXIT',Sender,Reason}->
      throw({interrupted,Reason});
    {'EXIT',_Other,Reason}->
      throw({exit,Reason})
  end.

%%===========================================================================
%% LIVE COPY
%%===========================================================================
prepare_live_copy( Source )->
  AccessMode = dlss_segment:get_access_mode( Source ),
  if
    AccessMode =:= read_write->
      ?LOGINFO("---------------------LIVE COPY----------------------------"),
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

finish_live_copy(false = _Live, _TargetRef)->
  ok;
finish_live_copy( Live, TargetRef)->
% Give away live updates to another process until the mnesia is ready
  give_away_live_updates(Live, TargetRef),
  ets:delete( Live ).

drop_live_copy(_Source, false =_Live)->
  ok;
drop_live_copy(Source, Live)->
  dlss_subscription:unsubscribe( Source ),
  ets:delete( Live ).

roll_live_updates( Live, #target{ module = Module, name = Target } = TargetRef,  TailKey )->

  % First we flush subscriptions and roll them over already stockpiled actions,
  % Then we take only those actions that are in the copy keys range already
  % because the next batch may not contain the update yet
  % and so will overwrite came live update.
  % Timeout 0 because we must to receive the next remote batch as soon as possible
  Updates = flush_subscriptions( Target, _Timeout = 0 ),
  ?LOGINFO("~p live updates count ~p",[
    Target,
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
    Target,
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
give_away_live_updates(Live, #target{ name = Target } = TargetRef)->

  Giver = self(),
  Taker =
    spawn_link(fun()->
      ok = dlss_subscription:subscribe( Target ),
      Giver ! {ready, self() },

      ?LOGINFO("~p take live updates over ~p",[Target,self()]),
      Logger = spawn_link(fun()->not_ready(Target) end),
      wait_table_ready(TargetRef, Logger, dlss_segment:where_to_write( Target ), dlss_segment:get_access_mode(Target))

    end),

  ?LOGINFO("~p give away live updates from ~p to ~p",[Target, self(), Taker]),

  receive {ready,Taker}->ok end,

  % From now the Taker receives updates I can unsubscribe, and wait
  % for my tail updates
  dlss_subscription:unsubscribe( Target ),

  ?LOGINFO("~p: roll tail live updates",[Target]),
  roll_tail_updates( Live, TargetRef).

roll_tail_updates( Live, #target{ module = Module, name = Target } = TargetRef )->

  % Timeout because I have already unsubscribed and it's a finite process
  Updates = flush_subscriptions( Target, ?FLUSH_TAIL_TIMEOUT ),
  ?LOGINFO("~p tail updates count ~p",[
    Target,
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



wait_table_ready(#target{name = Target, module = Module} = TargetRef, Logger, Node, Access)
  when Node=:=node(); Access =:= read_only->
  ?LOGINFO("DEBUG: ~p copy is ready, flush tail subscriptions",[Target]),

  dlss_subscription:unsubscribe( Target ),

  Updates = flush_subscriptions( Target, ?FLUSH_TAIL_TIMEOUT ),

  Actions =
    [ Module:live_action(U) || U <- Updates ],

  Module:write_batch(Actions, TargetRef),

  exit(Logger, shutdown),

  ?LOGINFO("~p live copy is ready",[Target]);
wait_table_ready(#target{name = Target, module = Module} = TargetRef, Logger, _Node, _Access)->
  % The copy is not ready yet
  Updates = flush_subscriptions( Target, _Timeout = 0 ),

  Actions =
    [ Module:live_action(U) || U <- Updates ],

  ?LOGINFO("~p write tail updates count ~p",[
    Target,
    ?PRETTY_COUNT(length(Actions))
  ]),

  Module:write_batch(Actions, TargetRef),
  timer:sleep(3000),

  wait_table_ready(TargetRef, Logger, dlss_segment:where_to_write(Target), dlss_segment:get_access_mode(Target) ).

not_ready(Target)->
  ?LOGINFO("~p live copy is not ready yet",[Target]),
  timer:sleep(5000),
  not_ready( Target ).

do_copy(SourceRef, Module, OnBatch, InAcc)->

  Acc0 = #acc{
    module = Module,
    batch = [],
    acc = InAcc,
    size = 0,
    on_batch = OnBatch,
    stop = SourceRef#source.stop
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
iterator({K,V},#acc{module = Module, batch = Batch, size = Size0, stop = Stop} = Acc)
  when Size0 < ?BATCH_SIZE, Stop =/= undefined, K < Stop->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{module = Module, batch = Batch, on_batch = OnBatch, acc = InAcc0, size = Size0, stop = Stop} = Acc)
  when Stop =/= undefined, K < Stop->

  {Action,Size} = Module:action({K,V}),

  Acc#acc{batch = [Action], acc = OnBatch(Batch, Size0, InAcc0), size = Size};

% stop key reached
iterator(_Rec,#acc{stop = Stop} = Acc)
  when Stop =/= undefined->
  throw({stop, Acc});

%----------------------NO STOP KEY-----------------------------
iterator({K,V},#acc{module = Module, batch = Batch, size = Size0} = Acc)
  when Size0 < ?BATCH_SIZE->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{module = Module, batch = Batch, on_batch = OnBatch, size = Size0, acc = InAcc0} = Acc) ->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action], acc =OnBatch(Batch, Size0, InAcc0), size = Size}.

%------------------SPLIT---------------------------------------
% Splits are always local
%--------------------------------------------------------------
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
  case Module:prev(I, Key) of
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

get_size( Table )->

  Module = get_module( Table ),
  ReadNode = get_read_node( Table ),
  if
    ReadNode =:= node()->
      Module:get_size( Table );
    true->
      case rpc:call(ReadNode, Module, get_size, [ Table ]) of
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


