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
  purge_to/2,
  get_size/1
]).

%%=================================================================
%%	Remote API
%%=================================================================
-export([
  remote_copy_request/5
]).

-export([
  debug/2
]).

-record(acc,{acc, module, batch, size, on_batch, stop }).
-record(reverse,{i, module, head, tail, h_key, t_key, hash}).

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

init_target(Target, Source, Module, Options)->
 case lists:member( Target, dlss:get_local_segments()) of
   true ->
     % It's not add_copy, simple copy
     T = Module:init_target(Target,Options),
     T#target{ name = Target, trick = false };
   _->
     % TRICK mnesia, Add a copy to this node before ask mnesia to add it
     Props = init_props(Source),
     Module:init_copy( Target, Props ),

     T = Module:init_target(Target,Options),
     T#target{ name = Target, trick = true }
 end.

rollback_target(#target{trick = true, name = Target}, Module )->
  Module:drop_target( Target );
rollback_target(_T, _M)->
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
      AccessMode = mnesia:table_info(Source, access_mode),
      if
        AccessMode =:= read_only->
          ?LOGINFO("~p passive copy to ~p, module ~p, options ~p",[Source, Target, Module, Options]),
          remote_copy(Source, Target, Module, Options );
        true->
          ?LOGINFO("~p active copy to ~p, module ~p, options ~p",[Source, Target, Module, Options]),
          active_copy(Source, Target, Module, Options )
      end
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

  SourceRef = Module:init_source( Source, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  FinalHash = do_copy(SourceRef, Module, OnBatch, InitHash ),
  Module:dump_target( TargetRef ),

  ?LOGINFO("finish local copying: source ~p, target ~p, hash ~ts",[Source, Target, base64:encode( FinalHash )]),

  FinalHash.

remote_copy( Source, Target, Module, Options)->

  Trap = process_flag(trap_exit,true),

  FinalHash =
    try  do_remote_copy( Source, Target, Module, Options )
    after
      process_flag(trap_exit,Trap)
    end,

  ?LOGINFO("finish remote copying: source ~p, target ~p, hash ~ts", [Source, Target, base64:encode( FinalHash )] ),

  FinalHash.

do_remote_copy( Source, Target, Module, #{
  hash := InitHash0,
  attempts := Attempts
} = Options )->

  ReadNode = get_read_node( Source ),
  TargetRef = init_target( Target, Source, Module, Options ),

  Self = self(),
  OnBatch =
   fun(Batch0, Size, Hash0)->

     Batch = term_to_binary( Batch0 ),
     Hash = crypto:hash_update(Hash0, Batch),
     Zip = zlib:zip( Batch ),

     ?LOGDEBUG("send batch: source ~p, target ~p, size ~s, zip ~s, length ~s",[
       Source,
       Target,
       ?PRETTY_SIZE(Size),
       ?PRETTY_SIZE(size(Zip)),
       ?PRETTY_COUNT(length(Batch0))
     ]),

     Self ! {write_batch, self(), Zip, Size, crypto:hash_final( Hash)},
     receive
       {confirmed, Self}-> Hash
     end
   end,

  Worker = spawn_link(ReadNode, ?MODULE, remote_copy_request,[Self,Source,Module,OnBatch,Options]),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  ?LOGINFO("copy ~p from ~p...",[Source,ReadNode]),

  FinalHash =
    try remote_copy_loop(Worker, Module, TargetRef, InitHash)
    catch
      _:Error->
        rollback_target( TargetRef, Module ),
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
            do_remote_copy( Source, Target, Module, Options#{ attempts => Attempts -1});
          true->
            throw( Error )
        end
    after
      exit(Worker,shutdown)
    end,

  Module:dump_target( TargetRef ),

  FinalHash.

remote_copy_request(Owner, Source, Module, OnBatch,#{
  hash := InitHash0
} = Options)->

  ?LOGINFO("remote copy request on ~p, options ~p", [Source, Options]),

  SourceRef = Module:init_source( Source, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),
  FinalHash = do_copy( SourceRef, Module, OnBatch, InitHash ),

  ?LOGINFO("finish remote copy request on ~p",[Source]),

  unlink( Owner ),
  Owner ! {finish,self(),FinalHash}.

remote_copy_loop(Worker, Module, #target{name = Target} =TargetRef, Hash0)->
  receive
     {write_batch, Worker, Zip, Size, WorkerHash }->

       ?LOGDEBUG("~p batch received",[Target]),

       BatchBin = zlib:unzip( Zip ),
       Hash = crypto:hash_update(Hash0, BatchBin),

       case crypto:hash_final(Hash) of
         WorkerHash ->

           Worker ! {confirmed, self()},
           Batch = binary_to_term(BatchBin),

           ?LOGDEBUG("~p write batch size ~s, length ~p",[
             Target,
             ?PRETTY_SIZE(Size),
             ?PRETTY_COUNT(length(Batch))
           ]),

           Module:write_batch(Batch, TargetRef);
         _->
           throw(invalid_hash)
       end,
       remote_copy_loop(Worker, Module, TargetRef, Hash);
     {finish,Worker,WorkerFinalHash}->
       % Finish
       ?LOGDEBUG("~p remote worker finished",[Target]),
       case crypto:hash_final(Hash0) of
         WorkerFinalHash -> WorkerFinalHash;
         _-> throw(invalid_hash)
       end;
     {'EXIT',Worker,Reason}->
       throw({interrupted,Reason});
     {'EXIT',_Other,Reason}->
       throw({exit,Reason})
 end.


active_copy(Source, Target, Module, Options )->
  % TODO
  remote_copy(Source, Target, Module, Options).

do_copy(SourceRef, Module, OnBatch, InAcc)->

  Acc0 = #acc{
    module = Module,
    batch = [],
    acc = InAcc,
    size = 0,
    on_batch = OnBatch,
    stop = SourceRef#source.stop
  },

  FinalHash =
    case try Module:fold(SourceRef, fun iterator/2, Acc0)
         catch
           _:{stop,InTailAcc}-> InTailAcc
         end of
      #acc{batch = [], acc = InFinalAcc} ->
        InFinalAcc;
      #acc{batch = Tail, size = Size, acc = InFinalAcc, on_batch = OnBatch}->
        OnBatch(Tail, Size, InFinalAcc)
    end,

   crypto:hash_final( FinalHash ).

%----------------------WITH STOP KEY-----------------------------
iterator({K,V},#acc{module = Module, batch = Batch, size = Size0, stop = Stop} = Acc)
 when Size0 < ?BATCH_SIZE, Stop =/= undefined, Stop < K->

 {Action,Size} = Module:action({K,V}),
 Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{module = Module, batch = Batch, on_batch = OnBatch, acc = InAcc, size = Size0, stop = Stop} = Acc)
 when Stop =/= undefined, Stop < K->

 {Action,Size} = Module:action({K,V}),
 Acc#acc{batch = [Action], acc = OnBatch(Batch, Size0, InAcc), size = Size};

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
iterator({K,V},#acc{module = Module, batch = Batch, on_batch = OnBatch, size = Size0, acc = InAcc} = Acc) ->

 {Action,Size} = Module:action({K,V}),
 Acc#acc{batch = [Action], acc = OnBatch(Batch, Size0, InAcc), size = Size}.

%------------------SPLIT---------------------------------------
% Splits are always local
%--------------------------------------------------------------
split( Source, Target )->
  split( Source, Target, #{}).
split( Source, Target, Options0 )->

  Options = ?OPTIONS( Options0 ),
  Module = get_module( Source ),

  #reverse{ h_key = SplitKey, hash = FinalHash0 } =
    Module:init_reverse(Source,
      fun
        (empty)->
          ?LOGWARNING("~p is empty, nothing to split"),
          <<>>;
        ({I,TSize,TKey})->

          TargetRef = Module:init_target(Target, Options),

          % Target considered to be empty
          InitHash = crypto:hash_update(crypto:hash_init(sha256),<<>>),

          Acc0 = #reverse{i=I, module = Module, head = 0, t_key = TKey ,tail = TSize, hash = InitHash},

          OnBatch =
            fun(Batch, Size, #reverse{hash = Hash,head = H} = Acc)->
              HKey = Module:get_key(hd(Batch)),
              NextAcc = reverse_loop(Acc#reverse{head = H + Size, h_key = HKey }),

              ?LOGDEBUG("~p write batch, size ~s, length ~s",[
                Target,
                ?PRETTY_SIZE(Size),
                ?PRETTY_COUNT(length(Batch))
              ]),

              Module:write_batch(Batch, TargetRef),
              NextAcc#reverse{ hash = crypto:hash_update(Hash, term_to_binary( Batch ))}
            end,

          SourceRef = Module:init_source( Source, Options ),
          % Run split
          do_copy(SourceRef, Module, OnBatch, Acc0)
      end),
  FinalHash = crypto:hash_final( FinalHash0 ),
  ?LOGINFO("split finish: source ~p, target ~p, split key ~p, hash ~ts",[
    Source,
    Target,
    Module:decode_key(SplitKey),
    base64:encode( FinalHash )
  ]),

  FinalHash.

%% THIS IS THE MEDIAN!
reverse_loop(#reverse{h_key = HKey, t_key = TKey}=Acc) when HKey >= TKey->
  throw({stop,Acc});

reverse_loop(#reverse{i=I, module = Module, head = H,tail = T,t_key = TKey} = Acc) when T < H->
 case Module:prev(I,TKey) of
   {K,Size} ->
     reverse_loop(Acc#reverse{tail = T + Size, t_key = K});
   '$end_of_table' ->
     throw({stop,Acc})
 end;
% We reached the head size
reverse_loop(Acc)->
  Acc.

purge_to( Source, Key )->

  Module = get_module( Source ),
  Options = ?OPTIONS(#{start_key => undefined, end_key => Key}),

  SourceRef = Module:init_source(Source, Options),
  Module:purge_head( SourceRef ).

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
  spawn(fun()->fill(Storage,Count) end).
fill(S,C) when C >0 ->
  if C rem 100000 =:= 0-> ?LOGINFO("DEBUG: write ~p",[C]); true->ignore end,
  dlss:dirty_write(S, {x, erlang:phash2(C, os:system_time(second))}, {y, binary:copy(integer_to_binary(C), 100)}),
  fill(S,C-1);
fill(_S,_C)->
  ok.


