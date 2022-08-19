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

-module(dlss_disc).

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
  dump_source/1,
  dump_target/1,
  drop_target/1,
  fold/3,
  action/1,
  live_action/1,
  write_batch/2,
  drop_batch/2,
  init_reverse/2,
  prev/2,
  get_key/1,
  decode_key/1,
  get_size/1
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
  % Remove mount point if it's rolled back during the previous copy attempt
  MP = mnesia_eleveldb:data_mountpoint( Target ),
  os:cmd("rm -rf " ++ MP),

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

dump_source( _SourceRef )->
  % Give it some rest time to settle down
  timer:sleep( 10000 ),
  ok.
dump_target( #target{name = Target, trick = true} )->
  mnesia_eleveldb:sync_close_table(mnesia_eleveldb:default_alias(), Target);
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

live_action({write, {K,V}})->
  K1 = ?ENCODE_KEY(K),
  {K1, {put, K1,?ENCODE_VALUE(V)} };
live_action({delete,K})->
  K1 = ?ENCODE_KEY(K),
  {K1,{delete,K1}}.


write_batch(Batch, #target{ref = Ref,sync = Sync})->
  eleveldb:write(Ref,Batch, [{sync, Sync}]).

drop_batch(Batch0,#source{ref = Ref})->
  Batch =
    [case R of {put,K,_}->{delete,K};_-> R end || R <- Batch0],
  eleveldb:write(Ref,Batch, [{sync, false}]).

init_reverse( Source, Fun )->
  Ref = ?REF( Source ),
  {ok, I} = eleveldb:iterator(Ref, []),
  try
    case ?MOVE(I,last) of
      {ok,?INFO_TAG,_}->Fun('$end_of_table');
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

get_size( Table)->
  get_size( Table, 10 ).
get_size( Table, Attempts ) when Attempts > 0->
  MP = mnesia_eleveldb:data_mountpoint( Table ),
  S = list_to_binary(os:cmd("du -s --block-size=1 "++MP)),
  case binary:split(S,<<"\t">>) of
    [Size|_]->
      try binary_to_integer( Size )
      catch _:_->
        % Sometimes du returns error when there are some file transformations
        timer:sleep(200),
        get_size( Table, Attempts - 1 )
      end;
    _ ->
      timer:sleep(200),
      get_size( Table, Attempts - 1 )
  end;
get_size( _Table, 0 )->
  -1.





