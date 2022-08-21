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

-module(dlss_segment).

-include("dlss.hrl").

%%=================================================================
%%	READ/WRITE API
%%=================================================================
-export([
  read/3,dirty_read/2,
  write/3,dirty_write/2,
  delete/3,dirty_delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	SEARCH API
%%=================================================================
-export([
  search/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1,

  have/1,
  nodes/1,
  ready_nodes/1,

  get_info/1,
  get_local_segments/0,

  access_mode/1
]).

%%=================================================================
%%	STORAGE SERVER API
%%=================================================================
-export([
  create/2,
  remove/1,

  add_copy/1,
  remove_copy/1,

  split/3,
  merge/5,

  source_node/1,

  access_mode/2
]).

%%=================================================================
%%	SUBSCRIPTIONS API
%%=================================================================
-export([
  subscribe/1,
  unsubscribe/1
]).

%%=================================================================
%%	ENGINE
%%=================================================================
-define(SCHEMA,dlss_segment_schema).

-define(LOOKUP(K),
  case ets:lookup(?SCHEMA,K) of
    [{_,_@V}]->_@V;
    _-> ?UNDEFINED
  end).

-define(START_OF_TABLE,'$start_of_table').
-define(END_OF_TABLE,'$start_of_table').

-define(NOT_AVAILABLE,
  if
    ?FUNCTION_NAME=:=dirty_write;?FUNCTION_NAME=:=write->
      not_available;
    ?FUNCTION_NAME=:=next;?FUNCTION_NAME=:=prev;?FUNCTION_NAME=:=first;?FUNCTION_NAME=:=last->
      ?END_OF_TABLE;
    true->[]
  end).

-define(REC2KV(R),{R#kv.key,R#kv.value}).
-define(KV2REC(KV),#kv{key = element(1,KV),value = element(2,KV)}).

-define(RECs2KVs(Rs),[?REC2KV(R) || R<-Rs]).
-define(KVs2RECs(KVs),[?KV2REC(KV) || KV<-KVs]).

-define(OK(R),
  if
    ?FUNCTION_NAME=:=dirty_read;?FUNCTION_NAME=:=search;?FUNCTION_NAME=:=read->?RECs2KVs(R);
    true->R
  end).

-define(READY,
  case ?LOOKUP(ready_nodes) of
    ?UNDEFINED->[];
    _@R->_@R
  end).

-define(NODES(S), ?LOOKUP({S,nodes})).

-define(READY(Ns),Ns -- (Ns -- ?READY)).

-define(HAVE(S),
  case ?LOOKUP({S,have}) of
    ?UNDEFINED->false;
    _->true
  end).

-define(M(S),?LOOKUP({S,module})).
-define(F,
  if
    ?FUNCTION_NAME=:=dirty_read->read;
    ?FUNCTION_NAME=:=dirty_write->write;
    ?FUNCTION_NAME=:=dirty_delete->delete;
    true->?FUNCTION_NAME
  end
).
-define(A,
  if
    ?FUNCTION_NAME=:=read->2;
    ?FUNCTION_NAME=:=write->2;
    ?FUNCTION_NAME=:=delete->2;
    true->?FUNCTION_ARITY
  end).

-define(LOCAL(M,F), fun M:F/?A).

-define(RPC(N,M,F,T),
  if
    ?A=:=0->fun()->T(N,M,F,[]) end;
    ?A=:=1->fun(A)->T(N,M,F,[A]) end;
    ?A=:=2->fun(A1,A2)->T(N,M,F,[A1,A2]) end
  end).

-define(TYPE(S),
  _@M = ?M(S),
  if
    _@M =:= dlss_ram->ram;
    _@M =:= dlss_ramdisc->ramdisc;
    _@M =:= dlss_disc -> disc
  end
).

%------------entry points------------------------------------------
-define(read(S),
  case ?HAVE(S) of
    true->?LOCAL(?M(S),?F(S));
    _->?RPC(?READY(?NODES(S)),?M(S),?F,fun dlss_rpc:call_one/4)
  end).

-define(write(S),
  if
    ?FUNCTION_NAME=:=dirty_write;?F->
      ?RPC(?READY(?NODES(S)),?M(S),?F,fun dlss_rpc:call_any/4);
    true->
      ?RPC(?READY(?NODES(S)),?M(S),?F,fun dlss_rpc:call_all/4)
  end).


%%=================================================================
%%	READ/WRITE API
%%=================================================================
dirty_read( Segment, Keys )->
  case (?read(Segment))( Keys) of
    {error,_}->?NOT_AVAILABLE;
    Recs -> ?RECs2KVs(Recs)
  end.

read( Segment, Keys, Lock)->
  % Transactions are still on mnesia
  mnesia_read(Keys, Segment, Lock).
mnesia_read([K|Rest], Segment, Lock)->
  case mnesia:read(Segment,K,Lock) of
    []->mnesia_read(Segment,Rest,Lock);
    [Rec]->[?REC2KV(Rec)|read(Segment,Rest,Lock)]
  end.

dirty_write(Segment,KVs)->
  case (?write(Segment))( KVs ) of
    {error,_}->?NOT_AVAILABLE;
    ok ->
      [ dlss_subscription:notify(Segment,{write,KV}) || KV <- KVs],
      ok
  end.

write(Segment,KVs,Lock)->

  % Transactions are still on mnesia
  mnesia_write(KVs,Segment,Lock),

  % Mnesia terminates on the transaction on write error,
  % Batch either in or we won't be here
  dlss_backend:on_commit(fun()->
    [ dlss_subscription:notify(Segment,{write,KV}) ||KV <- KVs],
    ok
  end),
  ok.
mnesia_write(KVs,Segment,Lock)->
  [mnesia:write(Segment, ?KV2REC(KV), Lock) || KV <- KVs].

dirty_delete(Segment,Keys)->
  case (?write(Segment))(Keys) of
    {error,_}->?NOT_AVAILABLE;
    ok->
      [dlss_subscription:notify(Segment,{delete,K}) || K<-Keys],
      ok
  end.
delete(Segment,Keys,Lock)->
  [ mnesia:delete(Segment,K,Lock) || K <- Keys],

  dlss_backend:on_commit(fun()->
    [dlss_subscription:notify(Segment,{delete,K}) || K<-Keys],
    ok
  end),
  ok.

%%=================================================================
%%	ITERATOR
%%=================================================================
first(Segment)->
  case (?read(Segment))() of
    {error,_}->?NOT_AVAILABLE;
    First->First
  end.

last(Segment)->
  case (?read(Segment))() of
    {error,_}->?NOT_AVAILABLE;
    Last -> Last
  end.

next(Segment,Key)->
  case (?read(Segment))( Key ) of
    {error,_} -> ?NOT_AVAILABLE;
    Next -> Next
  end.

prev(Segment,Key)->
  case (?read(Segment))( Key ) of
    {error,_}->?NOT_AVAILABLE;
    Prev -> Prev
  end.

%%=================================================================
%%	SEARCH
%%=================================================================
search(Segment,Options0)->
  Options = maps:map(
    fun
      (_K,?START_OF_TABLE)->?UNDEFINED;
      (_K,?END_OF_TABLE)-> ?UNDEFINED;
      (ms,MS)->ets:match_spec_compile(match_pattern( MS ))
    end, Options0),

  case (?read(Segment))(maps:merge(#{
    start = ?UNDEFINED,
    stop := ?UNDEFINED,
    ms := ?UNDEFINED
  }, Options)) of
    {error,_} -> ?NOT_AVAILABLE;
    Records -> ?RECs2KVs( Records )
  end.

match_pattern([{KV,G,R}|Rest])->
  [{?KV2REC(KV),G,R} | match_pattern( Rest )].

%%=================================================================
%%	INFO
%%=================================================================
get_size( Segment )->
  (?read(Segment))( Segment ).

have( Segment )->
  ?HAVE( Segment ).

nodes( Segment )->
  ?NODES( Segment ).

ready_nodes( Segment )->
  ?READY(?NODES(Segment)).

get_info(Segment)->
  #{
    type => ?TYPE(Segment),
    nodes => ?NODES(Segment),
    local => mnesia:table_info(Segment,local_content)
  }.

get_local_segments()->
  [ S || [S] <- ets:match(?SCHEMA, {'$1',have})].

access_mode( Segment )->
  mnesia:table_info( Segment, access_mode ).

access_mode( Segment , Mode )->
  case mnesia:change_table_access_mode(Segment, Mode) of
    {atomic,ok} -> ok;
    {aborted,{already_exists,Segment,Mode}}->ok;
    {aborted,Reason}->{error,Reason}
  end.

%%=================================================================
%%	STORAGE SERVER (Only)
%%=================================================================
create(Segment,#{nodes := Nodes} = Params0)->

  % The segment can be created only on ready nodes. The nodes that are
  % not active now will add it later during synchronization
  ReadyNodes = ?READY( Nodes ),

  if
    length(ReadyNodes) > 0 ->
      Params = Params0#{
        nodes => Nodes
      },

      Attributes = mnesia_attributes(Params),
      case mnesia:create_table(Segment,[
        {attributes,record_info(fields,kv)},
        {record_name,kv},
        {type,ordered_set}|
        Attributes
      ]) of
        {atomic, ok } -> ok;
        {aborted, Reason } -> {error, Reason}
      end;
    true ->
      { error, nodes_not_ready }
  end.

remove( Segment )->
  % Mnesia crashes on deleting read_only tables
  case access_mode( Segment, read_write ) of
    ok ->
      case mnesia:delete_table(Segment) of
        {atomic,ok}->ok;
        {aborted,Reason}-> {error, Reason }
      end;
    SetModeError ->
      SetModeError
  end.


add_copy( Segment )->
  case get_info( Segment ) of
    #{local:=true}-> {error,local_only};
    _->
      % ATTENTION! Mnesia trick, we do copy ourselves and tell mnesia to add it
      {ok, Unlock} = dlss_storage:lock_segment( Segment, _Lock=write ),
      try
        Hash = dlss_copy:copy(Segment,Segment,?M(Segment)),
        mnesia_attach( Segment ),
        i_have( Segment ),
        Hash
      catch _:E:S->
        drop_copy( Segment ),
        {error,{E,S}}
      after
        Unlock()
      end
  end.

mnesia_attach( Segment )->
  % mnesia crashes when attaching read_only copies
  Access = access_mode( Segment ),

  Type = ?TYPE(Segment),

  MnesiaType =
    if
      Type =:= ram      -> ram_copies;
      Type =:= ramdisc  -> disc_copies;
      Type =:= disc     -> leveldb_copies
    end,
  try {atomic,ok} = mnesia:add_table_copy(Segment, node(), MnesiaType)
  after
    access_mode(Segment,Access)
  end.

i_have( Segment )->
  ok.

drop_copy( Segment )->
  ok.


remove_copy(Segment)->
  % mnesia crashes when detaching read_only copies
  Access = access_mode( Segment ),

  try {atomic,ok} = mnesia:del_table_copy(Segment,node())
  catch
    _:E:S->{error,{E,S}}
  after
    access_mode(Segment,Access)
  end.

source_node( Segment )->
  case ?HAVE(Segment) of
    true -> node();
    _->
      case ?READY(?NODES(Segment)) of
        []->?UNDEFINED;
        Nodes -> ?RAND( Nodes )
      end
  end.

%%=================================================================
%%	SUBSCRIPTIONS API
%%=================================================================
subscribe( Segment )->

  Self = self(),
  % We need to subscribe to all nodes, every node can do updates

  case dlss_rpc:call_all(?READY(?NODES(Segment)), dlss_subscription, subscribe, [ Segment, self() ] ) of
    error ->
      % All or no one
      unsubscribe( Segment ),
  end,

  Results =
    [{N,
      case rpc:call(N, dlss_subscription, subscribe, [ Segment, Self ]) of
        {badrpc, Error} -> {error, Error};
        Result -> Result
      end} || N <- Nodes ],
  case [{N,E} || {N,{error,E}} <- Results ] of
    []-> ok;
    Errors->
      % All or no one
      unsubscribe( Segment ),
      {error, Errors}
  end.

unsubscribe( Segment )->
  Self = self(),
  [ rpc:call(N, dlss_subscription, unsubscribe, [ Segment, Self ]) || N <- dlss:get_ready_nodes()],
  drop_notifications( Segment ),
  ok.

drop_notifications(Segment)->
  receive
    {subscription, Segment, _Action}->
      drop_notifications(Segment)
  after
    100->
      % Ok, we tried
      ok
  end.

%%============================================================================
%%	Internal helpers
%%============================================================================
mnesia_attributes(#{
  type:=Type,
  nodes:=Nodes,
  local:=IsLocal
})->
  TypeAttr=
    case Type of
      ram->[
        {disc_copies,[]},
        {ram_copies,Nodes}
      ];
      ramdisc->[
        {disc_copies,Nodes},
        {ram_copies,[]}
      ];
      disc->
        [{leveldb_copies,Nodes}]
    end,

  LocalContent=
    if
      IsLocal->[{local_content,true}];
      true->[]
    end,
  TypeAttr++LocalContent.



