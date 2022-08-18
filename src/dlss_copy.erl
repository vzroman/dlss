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
  remote_copy_request/5,
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
    Type =:= disc -> dlss_copy_disc;
    true -> dlss_copy_ram
  end.

init_props( Source )->
  All =
    maps:from_list(mnesia:table_info( Source, all )),
  maps:to_list( maps:with(?PROPS,All)).

get_read_node( Table )->
 Node = mnesia:table_info( Table, where_to_read ),
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

remote_copy( Source, Target, Module, #{
  hash := InitHash0,
  attempts := Attempts
} = Options )->

  ReadNode = get_read_node( Source ),
  TargetRef = init_target( Target, Source, Module, Options ),

  Self = self(),
  InitState = #{
    source => Source,
    target => Target,
    hash => InitHash0,
    owner => Self,
    size => 0,
    batch => []
  },

  Worker = spawn_link(ReadNode, ?MODULE, remote_copy_request,[Self,Source,Module,Options,InitState]),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  % The remote worker needs a confirmation of the previous batch before it sends the next one
  Worker ! {confirmed, self()},

  % Subscribe to live updates if it's a live copy
  AccessMode = mnesia:table_info(Source, access_mode),
  if
    AccessMode =:= read_only->
      ?LOGINFO("~p passive copy from ~p, module ~p, options ~p",[Source, ReadNode, Module, Options]);
    true->
      ?LOGINFO("LIVE COPY! ~p live copy from ~p, module ~p, options ~p",[Source, ReadNode, Module, Options]),
      case dlss_segment:subscribe( Source ) of
        ok->
          % Unsubscribe normally done
          ?LOGINFO("DEBUG: subscribed on ~p",[Source]);
        {error,SubscribeError}->
          throw({unable_to_subscribe,Source,SubscribeError})
      end
  end,

  % Prepare the storage for live updates anyway to avoid excessive check during the copying
  Live = ets:new(live,[private,ordered_set]),

  %-------Enter the copy loop----------------------
  ?LOGINFO("copy ~p from ~p...",[Source,ReadNode]),
  FinalHash =
    try remote_copy_loop(Worker, TargetRef, #{hash => InitHash, live =>Live})
    catch
      _:Error->
        ets:delete( Live ),
        rollback_target( TargetRef ),
        case Error of
          invalid_hash->
            ?LOGERROR("~p invalid remote hash from ~p, left attempts ~p",[Source,ReadNode,Attempts-1]);
          {interrupted,Reason}->
            ?LOGERROR("~p copying from ~p interrupted, reason ~p, left attempts ~p",[Source,ReadNode,Reason,Attempts-1]);
          Other->
            ?LOGERROR("unexpected error on copying ~p from ~p, error ~p",[Source,ReadNode,Other]),
            throw(Other)
        end,
        if
          Attempts > 0->
            remote_copy( Source, Target, Module, Options#{ attempts => Attempts -1});
          true->
            throw( Error )
        end
    after
      exit(Worker,shutdown)
    end,

  % Give away live updates to another process until the mnesia is ready
  if
    AccessMode =/= read_only->
      give_away_live_updates(Live, TargetRef);
    true->
      ignore
  end,

  ets:delete( Live ),
  commit_target( TargetRef ),

  ?LOGINFO("finish remote copying: source ~p, target ~p, hash ~s", [Source, Target, ?PRETTY_HASH( FinalHash )] ),

  FinalHash.


remote_copy_request(Owner, Source, Module, Options, #{
  hash := InitHash0
} = InitState0)->

  ?LOGINFO("remote copy request on ~p, options ~p", [Source, Options]),

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

  ?LOGINFO("finish remote copy request on ~p, final hash ~s",[Source,?PRETTY_HASH(FinalHash)]),

  Owner ! {finish, self(), FinalHash}.

% Zip and stockpile local batches until they reach ?REMOTE_BATCH_SIZE
remote_batch(Batch0, Size, #{
  size := TotalZipSize0,
  batch := ZipBatch,
  hash := Hash0,
  source := Source,
  target := Target
}=State) when TotalZipSize0 < ?REMOTE_BATCH_SIZE->

  Batch = term_to_binary( Batch0 ),
  Hash = crypto:hash_update(Hash0, Batch),
  Zip = zlib:zip( Batch ),

  ZipSize = size(Zip),
  TotalZipSize = TotalZipSize0 + ZipSize,

  ?LOGINFO("DEBUG: add zip: source ~p, target ~p, size ~s, zip size ~p, total zip size ~p",[
   Source, Target, ?PRETTY_SIZE(Size), ?PRETTY_SIZE(ZipSize), ?PRETTY_SIZE(TotalZipSize)
  ]),

  State#{
    size => TotalZipSize,
    batch => [Zip|ZipBatch],
    hash => Hash
  };

% The batch is ready, send it
remote_batch(Batch0, Size, #{
  owner := Owner
}=State)->
 % First we have to receive a confirmation of the previous batch
  receive
   {confirmed, Owner}->
     send_batch( State ),
     remote_batch(Batch0, Size,State#{ batch=>[], size =>0 });
   {not_confirmed,Owner}->
     throw(invalid_hash)
  end.

send_batch(#{
  size := ZipSize,
  batch := ZipBatch,
  hash := Hash,
  source := Source,
  target := Target,
  owner := Owner
})->

  ?LOGINFO("send batch: source ~p, target ~p, zip size ~s, length ~p",[
    Source,
    Target,
    ?PRETTY_SIZE(ZipSize),
    length(ZipBatch)
  ]),
  Owner ! {write_batch, self(), ZipBatch, ZipSize, crypto:hash_final(Hash) }.

unzip_batch( [Zip|Rest], {Acc0,Hash0})->
  Batch = zlib:unzip( Zip ),
  Hash = crypto:hash_update(Hash0, Batch),
  unzip_batch(Rest ,{[Batch|Acc0], Hash});
unzip_batch([], Acc)->
  Acc.

remote_copy_loop(Worker, #target{module = Module, name = Target} =TargetRef, #{
  hash := Hash0,
  live := Live
}=Acc)->
  receive
    {subscription, Target, Update}->
       {K, Action} = Module:live_action( Update ),
       case Acc of
         #{ tail_key := TailKey } when TailKey =< K->
           % The live update key is in the copy keys range already we can safely put it to he copy
           ?LOGINFO("DEBUG: ~p live update key ~p, action ~p, write to the copy",[Target,Module:decode_key(K), Action]),
           Module:write_batch([Action],TargetRef);
         _->
           % Tail key either not defined yet or greater than the last batch tail key.
           % We can't write the action to the target because the next batch may not contain the update
           % and so will overwrite the live update.
           % Stockpile the action until the copy reaches the action key and then roll it over
           ?LOGINFO("DEBUG: ~p live update key ~p, action ~p, stockpile update",[Target,Module:decode_key(K), Action]),
           true = ets:insert(Live,{K,Action})
       end,
       remote_copy_loop(Worker, TargetRef, Acc);

     {write_batch, Worker, ZipBatch, ZipSize, WorkerHash }->

       ?LOGINFO("~p: batch received, size ~s, hash ~s",[Target,?PRETTY_SIZE(ZipSize),?PRETTY_HASH(WorkerHash)]),
       {BatchList,Hash} = unzip_batch( lists:reverse(ZipBatch), {[],Hash0}),

       % Check hash
       case crypto:hash_final(Hash) of
         WorkerHash -> Worker ! {confirmed, self()};
         LocalHash->
           ?LOGERROR("~p: invalid remote hash ~s, local hash ~s",[Target,?PRETTY_HASH(WorkerHash), ?PRETTY_HASH(LocalHash)]),
           Worker!{not_confirmed,self()},
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
       roll_live_updates(ets:first(Live), Live, TargetRef, TailKey),

       remote_copy_loop(Worker, TargetRef, Acc#{hash => Hash, tail_key => TailKey});
     {finish,Worker,WorkerFinalHash}->
       % Finish
       ?LOGINFO("~p: remote worker finished, final hash ~s",[Target, ?PRETTY_HASH(WorkerFinalHash)]),
       case crypto:hash_final(Hash0) of
         WorkerFinalHash ->
           WorkerFinalHash;
         LocalFinalHash->
           ?LOGERROR("~p: invalid remote final hash ~s, local final hash ~s",[
             Target,
             ?PRETTY_HASH(WorkerFinalHash),
             ?PRETTY_HASH(LocalFinalHash)
           ]),
           throw(invalid_hash)
       end;
     {'EXIT',Worker,Reason}->
       throw({interrupted,Reason});
     {'EXIT',_Other,Reason}->
       throw({exit,Reason})
 end.

roll_live_updates(K, Live, #target{name = Target, module = Module} = TargetRef, TailKey)
  when K=/='$end_of_table', K =< TailKey->

  [{_,Action}] = ets:lookup(Live, K),
  ?LOGINFO("DEBUG: ~p roll live update key ~p, action ~p",[Target, Module:decode_key(K), Action]),
  Module:write_batch([Action],TargetRef),

  roll_live_updates(ets:next(Live,K), Live, TargetRef, TailKey);
roll_live_updates(_K, _Live, _TargetRef, _TailKey)->
  ok.

give_away_live_updates(Live, #target{ name = Target } = TargetRef)->
  Owner = self(),
  Worker =
    spawn_link(fun()->
      ok = dlss_segment:subscribe( Target ),
      Owner ! {ready,self()},
      receive
        {start, Owner}->
          ?LOGINFO("DEBUG: ~p take live updates"),
          wait_table_ready(TargetRef, mnesia:table_info( Target, where_to_write ))
      end
    end),

  % From now the Worker receives updates
  dlss_segment:unsubscribe( Target ),

  ?LOGINFO("DEBUG: ~p roll tail live updates",[Target]),
  roll_tail_updates(ets:first(Live), Live, TargetRef),

  % Flush tail subscriptions
  flush_subscriptions(TargetRef),

  Worker ! {start, Owner}.

roll_tail_updates('$end_of_table', _Live, _TargetRef)->
   ok;
roll_tail_updates(K, Live, #target{name = Target, module = Module} = TargetRef) ->

   [{_,Action}] = ets:lookup(Live, K),
   ?LOGINFO("DEBUG: ~p roll tail update key ~p, action ~p",[Target, Module:decode_key(K), Action]),
   Module:write_batch([Action],TargetRef),

   roll_tail_updates(ets:next(Live,K), Live, TargetRef).

flush_subscriptions(#target{name = Target, module = Module}=TargetRef)->
  receive
    {subscription, Target, Update}->
      {K, Action} = Module:live_action( Update ),
      ?LOGINFO("DEBUG: ~p flush subscription key ~p, action ~p",[ Target, Module:decode_key(K), Action]),
      Module:write_batch([Action],TargetRef),
      flush_subscriptions(TargetRef)
  after
    0->ok
  end.

wait_table_ready(#target{name = Target} = TargetRef, Node) when Node =/= node()->
  ?LOGINFO("DEBUG: ~p is not ready yet",[Target]),
  % It's not ready yet
  flush_subscriptions(TargetRef),

  wait_table_ready(TargetRef, mnesia:table_info( Target, where_to_write ));

wait_table_ready(#target{name = Target} = TargetRef, Node) when Node=:=node()->
  ?LOGINFO("DEBUG: ~p copy is ready, flush tail subscriptions"),

  dlss_segment:unsubscribe( Target ),
  flush_subscriptions( TargetRef ),

  ?LOGINFO("~p live copy is ready").

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


