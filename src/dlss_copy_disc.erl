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

-record(source,{ref,iterator,start,on_batch}).
-record(target,{name,ref,sync,trick}).

-define(BATCH_SIZE, 8192).

% The encoded @deleted@ value. Actually this is {[],[],'@deleted@'}
% because mnesia_eleveldb uses this format
-define(DELETED, <<131,104,3,106,106,100,0,9,64,100,101,108,101,116,101,100,64>>).

%%=================================================================
%%	API
%%=================================================================
-export([
  init_source/3,
  init_target/3,
  dump_target/1,
  rollback_target/1,
  fold/2,
  write_batch/2
]).

init_source( Source, OnBatch, #{
  start_key := StartKey,
  end_key := EndKey
})->

  Ref = ?REF( Source ),

  Iterator =
    if
      EndKey =:= undefined->
        fun(Rec,Acc)-> iterator(Rec,Acc,OnBatch) end;
      true->
        Stop = ?ENCODE_KEY( EndKey ),
        fun(Rec,Acc)-> iterator(Rec,Acc,OnBatch,Stop) end
    end,

  Start =
    if
      StartKey =:= undefined -> ?DATA_START;
      true -> ?ENCODE_KEY( StartKey )
    end,

  #source{
    ref = Ref,
    iterator = Iterator,
    start = Start,
    on_batch = OnBatch
  }.

init_target(Target, Props, #{
  sync := Sync
})->
  TargetRef =
    case lists:member( Target, dlss:get_local_segments()) of
      true ->
        % It's not add_copy, just merge
        #target{ref = ?REF(Target), trick = false};
      _->
        % TRICK mnesia, Add copy to this node
        #target{ref = init_copy( Target, Props ), trick = true}
    end,
  TargetRef#target{
    name = Target,
    sync = Sync
  }.

init_copy(Target, Props)->
  Alias = mnesia_eleveldb:default_alias(),
  case mnesia_eleveldb:create_table(Alias, Target, Props) of
    ok->
      case mnesia_eleveldb:load_table(Alias, Target, {dumper,create_table}, Props) of
        {ok,_}->ok;
        ok->ok;
        Other -> throw( Other )
      end;
    {error,Error}->
      throw(Error)
  end.

dump_target( _TargetRef )->
  ok.

rollback_target( #target{ref = _Ref})->
  ok.

fold(#source{ref = Ref, iterator = Iter, start = Start, on_batch = OnBatch}, Acc0)->
  case try eleveldb:fold(Ref,Iter, Acc0, [{first_key, Start}])
  catch
    _:{stop,Acc}-> Acc
  end of
    {[], Hash, 0} -> Hash;
    {Tail, Hash, _Size} -> OnBatch(Tail, Hash)
  end.

%----------------------NO STOP KEY-----------------------------
iterator({K,V},{Batch, Hash, Size},_OnBatch)
  when Size < ?BATCH_SIZE->
  if
    V=:=?DELETED->
      {[{delete, K}|Batch], Hash, Size + size(K)};
    true->
      {[{put,K,V}|Batch], Hash, Size + size(K)+size(V)}
  end;

iterator({K,V},{Batch, Hash, _Size},OnBatch)->
  if
    V=:=?DELETED->
      {[{delete, K}], OnBatch(Batch,Hash), size(K)};
    true->
      {[{put,K,V}], OnBatch(Batch,Hash), size(K)+size(V)}
  end.

%----------------------WITH STOP KEY-----------------------------
iterator({K,V},{Batch, Hash, Size},_OnBatch, Stop)
  when Size < ?BATCH_SIZE, K < Stop->
  if
    V=:=?DELETED->
      {[{delete, K}|Batch], Hash, Size + size(K)};
    true->
      {[{put,K,V}|Batch], Hash, Size + size(K)+size(V)}
  end;

iterator({K,V},{Batch, Hash, _Size},OnBatch,Stop)
  when K < Stop->
  if
    V=:=?DELETED->
      {[{delete, K}], OnBatch(Batch,Hash), size(K)};
    true->
      {[{put,K,V}], OnBatch(Batch,Hash), size(K)+size(V)}
  end;
iterator(_Rec,Acc, _OnBatch, _Stop)->
  throw( Acc ).


write_batch(Batch, #target{ref = Ref,sync = Sync})->
  eleveldb:write(Ref,Batch, [{sync, Sync}]).

