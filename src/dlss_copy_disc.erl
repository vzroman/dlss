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

-module(dlss_copy_disc).

-include("dlss.hrl").
-include("dlss_eleveldb.hrl").

-record(source,{ref,fold,start,on_batch}).

-define(BATCH_SIZE, 8192).

-define(DECODE_VALUE(V),element(3,mnesia_eleveldb:decode_val(V))).

%%=================================================================
%%	API
%%=================================================================
-export([
  init_source/3,
  fold/2
]).

init_source( Source, OnBatch, #{
  start_key := StartKey,
  end_key := EndKey,
  not_match := Not
})->

  Ref = ?REF( Source ),

  Iterator =
    fun
      ({K,V} = Rec, {Batch, Hash, Size}) when Size < ?BATCH_SIZE->
        {[Rec|Batch], Hash, Size + size(K) + size(V)};
      ({K,V}=Rec,{Batch, Hash, _Size})->
        {[Rec],OnBatch( Batch, Hash ), size(K)+size(V) }
    end,

  Filter =
    if
      Not =:= undefined -> Iterator;
      true->
        EncodedNot = ?ENCODE_VALUE(Not),
        fun
          ({_K,V} = Rec, Acc) when V =/= EncodedNot -> Iterator(Rec,Acc);
          (_,Acc) -> Acc
        end
    end,

  Fold =
    if
      EndKey =:= undefined-> Filter;
      true->
        EncodedEndKey = ?ENCODE_KEY( EndKey ),
        fun
          ({K,_V} = Rec, Acc) when K =< EncodedEndKey-> Filter(Rec, Acc);
          (_, Acc)-> throw({stop,Acc})
        end
    end,

  Start =
    if
      StartKey =:= undefined -> ?DATA_START;
      true -> ?ENCODE_KEY( StartKey )
    end,

  #source{
    ref = Ref,
    fold = Fold,
    start = Start,
    on_batch = OnBatch
  }.

fold(#source{ref = Ref, fold = Fold, start = Start, on_batch = OnBatch}, Acc0)->
  try eleveldb:fold(Ref,Fold, Acc0, [{first_key, Start}])
  catch
    _:{stop,Acc}-> Acc
  end.

do_fold(SourceRef, Fun, Acc0, StartKey)->
  try eleveldb:fold(SourceRef,Fun, Acc0, [{first_key, StartKey}])
  catch
    _:{stop,Acc}-> Acc
  end.



copy( Source, Target )->
  copy( Source, Target, #{}).
copy( Source, Target, Options0 )->
  Options = maps:merge(#{
    start_key =>undefined,
    end_key => undefined,
    not_match => undefined,
    hash => <<>>,
    sync => false,
    attempts => 3
  }, Options0),

  ReadNode = get_read_node( Source),
  if
    ReadNode =:= node() ->
      local_copy( Source, Target, Options );
    true ->
      remote_copy(Source, Target, Options )
  end.

get_read_node( Table )->
  Node = mnesia:table_info( Table, where_to_read ),
  if
    Node =:= nowhere ->throw({unavailable,Table});
    true-> Node
  end.

local_copy( Source, Target, #{
  trace := Trace,
  sync := Sync
} = Options)->

  Trace(info,["start local copying: source ~p, target ~p, options: ~p",Source,Target,Options]),

  SourceRef = get_tab_ref( Source ),
  TargetRef = get_tab_ref( Target ),

  WriteBatch =
    fun(Batch, Hash)->
      Trace(debug,["~p write batch",Target]),
      write_batch(Batch, TargetRef, Sync),
      crypto:hash_update(Hash, term_to_binary( Batch ))
    end,

  FinalHash = do_copy(SourceRef, WriteBatch, Options ),
  Trace(info,"finish local copying: source ~p, target ~p, hash ~ts", Source, Target, base64:encode( FinalHash ) ),

  FinalHash.

remote_copy( Source, Target, #{
  trace := Trace
} = Options )->

  Trace(info,["start remote copying: source ~p, target ~p, options: ~p",Source,Target,Options]),

  TargetRef = get_tab_ref( Target ),

  Trap = process_flag(trap_exit,true),

  FinalHash =
    try  do_remote_copy( Source, TargetRef, Options )
    after
      process_flag(trap_exit,Trap)
    end,

  Trace(info,"finish remote copying: source ~p, target ~p, hash ~ts", Source, Target, base64:encode( FinalHash ) ),

  FinalHash.

do_remote_copy( Source, TargetRef, #{
  hash := InitHash0,
  sync := Sync,
  trace := Trace,
  attempts := Attempts
} = Options )->

  ReadNode = get_read_node( Source ),

  Self = self(),
  WriteBatch =
    fun(Batch, Hash0)->
      Hash = crypto:hash_update(Hash0, term_to_binary( Batch )),
      Self ! {write_batch, self(), Batch, crypto:hash_final( Hash)},
      receive
        {confirmed, Self}-> Hash
      end
    end,

  OnBatch =
    fun(Batch) -> write_batch( Batch, TargetRef, Sync) end,

  Worker = spawn_link(ReadNode, ?MODULE, remote_copy_request,[Source,WriteBatch,Options]),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  Trace(info,["copy ~p from ~p",Source,ReadNode]),
  try remote_copy_loop(Worker, OnBatch, InitHash)
  catch
    _:invalid_hash when Attempts > 0 ->
      Trace(warning,["invalid remote hash, retry...."]),
      do_remote_copy( Source, TargetRef, Options#{ attempts => Attempts -1});
    _:{interrupted,Reason} when Attempts > 0->
      Trace(warning,["copying interrupted, reason ~p, retry....",Reason]),
      do_remote_copy( Source, TargetRef, Options#{ attempts => Attempts -1})
  end.

remote_copy_loop(Worker, OnBatch, Hash0)->
  receive
    {write_batch, Worker, Batch, WorkerHash }->
      Hash = crypto:hash_update(Hash0, term_to_binary( Batch )),
      case crypto:hash_final(Hash) of
        WorkerHash ->
          Worker ! {confirmed, self()},
          OnBatch( Batch );
        _->
          throw(invalid_hash)
      end,
      remote_copy_loop(Worker, OnBatch, Hash);
    {finish,Worker}->
      Hash0;
    {'EXIT',Worker,Reason}->
      throw({interrupted,Reason});
    {'EXIT',_Other,Reason}->
      throw({exit,Reason})
  end.

remote_copy_request(Owner, Source, WriteBatch, #{
  trace := Trace
}=Options)->

  Trace(info,["remote copy request on ~p, options ~p", Source, Options ]),
  SourceRef = get_tab_ref( Source ),

  do_copy( SourceRef, WriteBatch, Options ),

  Trace(info,["finish remote copy request on ~p", Source ]),

  Owner ! {finish,self()}.




do_copy(SourceRef, WriteBatch, #{
  start_key := StartKey,
  end_key := EndKey,
  not_match := Not,
  hash := InitHash0
})->

  Iterator =
    fun
      (Rec, {Batch, Hash, Size}) when Size < 1000->
        {[Rec|Batch], Hash, Size + 1};
      (Rec,{Batch, Hash, _Size})->
        WriteBatch( Batch, Hash ),
        { [Rec], WriteBatch( Batch ), 1 }
    end,

  Filter =
    if
      Not =:= undefined -> Iterator;
      true->
        fun
          ({_K,V} = Rec, Acc) when V =/= Not -> Iterator(Rec,Acc);
          (_,Acc) -> Acc
        end
    end,

  Fold =
    if
      EndKey =:= undefined-> Filter;
      true->
        fun
          ({K,_V} = Rec, Acc) when K =< EndKey-> Filter(Rec, Acc);
          (_, Acc)-> throw({stop,Acc})
        end
    end,

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  FinalHash =
    case do_fold(SourceRef, Fold, {[],InitHash,0}, StartKey)
    of
      {[], Hash, 0} -> Hash;
      {Tail, Hash, _Size} -> WriteBatch(Tail, Hash)
    end,

  crypto:hash_final( FinalHash ).


do_fold(SourceRef, Fun, Acc0, StartKey)->
  try ?leveldb:fold(SourceRef,Fun, Acc0, [{first_key, StartKey}])
  catch
    _:{stop,Acc}-> Acc
  end.

get_tab_ref( Tab )->
  {ext, Alias, _} = mnesia_lib:storage_type_at_node( node(), Tab ),
  {Ref, _Type, _RecName} = get_ref(Alias, Tab),
  Ref.

write_batch( Records, TargetRef, Sync )->
  ?leveldb:write(TargetRef,[{put,K,V} || {K,V} <- Records], [{sync, Sync}]).