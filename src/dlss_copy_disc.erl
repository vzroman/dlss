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
-include("dlss_copy.hrl").
-include("dlss_eleveldb.hrl").


% The encoded @deleted@ value. Actually this is {[],[],'@deleted@'}
% because mnesia_eleveldb uses this format
-define(DELETED, <<131,104,3,106,106,100,0,9,64,100,101,108,101,116,101,100,64>>).

%%=================================================================
%%	API
%%=================================================================
-export([
  init_source/2,
  init_target/2,
  init_copy/2,
  dump_target/1,
  drop_target/1,
  fold/3,
  action/1,
  write_batch/2,
  init_reverse/2,
  prev/2,
  get_key/1,
  decode_key/1
]).

init_source( Source, #{
  start_key := StartKey,
  end_key := EndKey
})->

  Ref = ?REF( Source ),

  Start =
    if
      StartKey =:= undefined -> ?DATA_START;
      true -> ?ENCODE_KEY( StartKey )
    end,
  Stop =
    if
      EndKey =:= undefined->
        undefined;
      true->
        ?ENCODE_KEY( EndKey )
    end,

  #source{
    ref = Ref,
    start = Start,
    stop = Stop
  }.

init_target(Target, #{
  sync := Sync
})->
  #target{ ref = ?REF(Target), sync = Sync }.

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

drop_target( Target )->
  mnesia_eleveldb:delete_table(mnesia_eleveldb:default_alias(), Target).

fold(#source{ref = Ref, start = Start}, Iterator, Acc0)->
  eleveldb:fold(Ref,Iterator, Acc0, [{first_key, Start}]).

action({K,?DELETED})->
  {{delete, K},size(K)};
action({K,V})->
  {{put,K,V},size(K)+size(V)}.

write_batch(Batch, #target{ref = Ref,sync = Sync})->
  eleveldb:write(Ref,Batch, [{sync, Sync}]).

init_reverse( Source, Fun )->
  Ref = ?REF( Source ),
  {ok, I} = eleveldb:iterator(Ref, []),
  try
    case ?MOVE(I,last) of
      {ok,?INFO_TAG,_}->Fun(empty);
      {ok,K,V}-> Fun({I,size(K)+size(V),K})
    end
  after
    catch eleveldb:iterator_close(I)
  end.

get_key({put,K,_V})->K;
get_key({delete,K})->K.

decode_key(K)->?DECODE_KEY(K).

prev(I,_K)->
  case ?PREV(I) of
    {ok, K, V}->
      Size = size(K) + size(V),
      {K, Size};
    {error, invalid_iterator}->
      % End of the table (start)
      '$end_of_table';
    {error, iterator_closed}->
      throw(iterator_closed)
  end.




