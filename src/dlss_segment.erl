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
-include("dlss_schema.hrl").

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
  search/2,
  match/2
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
%%	NODES API
%%=================================================================
-export([
  create/3,
  delete/1,

  i_have/1,
  n_has/2,
  n_doesnt_have/2
]).

%%=================================================================
%%	ENGINE
%%=================================================================

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

%------------entry points------------------------------------------
-define(read(S),
  case ?I_HAVE(S) of
    true->?LOCAL(?S_MODULE(S),?F(S));
    _->?RPC(?S_READY_NODES(S),?S_MODULE(S),?F,fun dlss_rpc:call_one/4)
  end).

-define(write(S),
  if
    ?FUNCTION_NAME=:=dirty_write->
      case ?I_HAVE(S) of
        true ->
          ?RPC(?S_READY_NODES(S),?S_MODULE(S),?F,fun dlss_rpc:cast_all/4);
        _->
          ?RPC(?S_READY_NODES(S),?S_MODULE(S),?F,fun dlss_rpc:call_any/4)
      end;
    true->
      ?RPC(?S_READY_NODES(S),?S_MODULE(S),?F,fun dlss_rpc:call_all/4)
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

match(Segment, Pattern)->
  case (?read(Segment))( ?KV2REC(Pattern) ) of
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
  ?I_HAVE( Segment ).

nodes( Segment )->
  ?S_NODES( Segment ).

ready_nodes( Segment )->
  ?S_READY_NODES(Segment).

get_info(Segment)->
  #{
    type => ?S_TYPE(Segment),
    nodes => ?S_NODES(Segment),
    local => mnesia:table_info(Segment,local_content)
  }.

get_local_segments()->
  [ S || [S] <- ?SCHEMA_MATCH({'$1',have})].

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
create(Segment,#{nodes := Nodes,type:=Type} = Params)->

  % The segment can be created only on ready nodes. The nodes that are
  % not active now will add it later during synchronization
  ReadyNodes = ?X_NODES( Nodes, ?READY_NODES ),

  if
    length(ReadyNodes) > 0 ->
      try
        mnesia_create( Segment, Params#{ nodes => ReadyNodes } ),

        Module =
          if
            Type =:= ram->dlss_ram;
            Type =:= ramdisc->dlss_ramdisc;
            Type =:= disc -> dlss_disc
          end,

        case dlss_rpc:call_all(?READY_NODES, ?MODULE, create, [Segment,Module,ReadyNodes] ) of
          {ok,_}->
            ok;
          {error,Error}->
            throw({dlss_rpc_error,Error})
        end
      catch
        _:E:S->
          dlss_rpc:cast_all( ReadyNodes, ?MODULE, delete, [Segment] ),
          mnesia_remove( Segment ),
          {error,{E,S}}
      end;
    true ->
      { error, none_is_ready }
  end.

mnesia_create( Segment, Params)->

  Attributes = mnesia_attributes(Params),
  case mnesia:create_table(Segment,[
    {attributes,record_info(fields,kv)},
    {record_name,kv},
    {type,ordered_set}|
    Attributes
  ]) of
    {atomic, ok } -> ok;
    {aborted, Reason } -> throw({mnesia_error,Reason})
  end.


remove( Segment )->
  case dlss_rpc:call_all(?READY_NODES, ?MODULE, delete, [Segment] ) of
    {ok,_}->
      case mnesia_remove( Segment ) of
        ok-> ok;
        {error,MnesiaError}->
          {error,{mnesia_error,MnesiaError}}
      end;
    {error,DlssError}->
      {error,{dlss_error,DlssError}}
  end.

mnesia_remove( Segment )->

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
      case ?I_HAVE(Segment) of
        true -> {error, already_have};
        _->
          % ATTENTION! Mnesia trick, we do copy ourselves and tell mnesia to add it
          {ok, Unlock} = dlss_storage:lock_segment( Segment, _Lock=write ),
          try
            Hash = dlss_copy:copy(Segment,Segment,?S_MODULE(Segment)),
            mnesia_attach( Segment ),
            i_have( Segment ),
            Hash
          catch _:E:S->
            dlss_copy:rollback_copy( Segment, ?S_MODULE(Segment) ),
            {error,{E,S}}
          after
            Unlock()
          end
      end
  end.

mnesia_attach( Segment )->
  % mnesia crashes when attaching read_only copies
  Access = access_mode( Segment ),

  Type = ?S_TYPE(Segment),

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

remove_copy( Segment )->
  case dlss_rpc:call_all(?READY_NODES--[node()], ?MODULE, n_doesnt_have, [Segment,node()]) of
    {ok,_}->
      case mnesia_remove_copy( Segment ) of
        ok->
          ?S_REMOVE( Segment ),
          ok;
        {error,MnesiaError}->
          ?LOGERROR("~p remove copy mnesia error ~p",[ MnesiaError ]),
          {error,{mnesia_error,MnesiaError}}
      end;
    {error,DlssError}->
      ?LOGERROR("~p remove copy dlss error ~p",[ DlssError ]),
      {error,{mnesia_error,DlssError}}
  end.

mnesia_remove_copy( Segment )->
  % mnesia crashes when detaching read_only copies
  Access = access_mode( Segment ),
  try
    case mnesia:del_table_copy(Segment,node()) of
      {atomic,ok}->ok;
      {aborted,Reason}->{error,Reason}
    end
  after
    access_mode(Segment,Access)
  end.

split(Source, Target, InitHash)->
  dlss_copy:split(Source, Target, ?S_MODULE(Source), #{ hash => InitHash }).

merge(Source, Target, FromKey, EndKey, InitHash)->
  StartKey =
    case FromKey of
      ?START_OF_TABLE -> ?UNDEFINED;
      _-> FromKey
    end,
  _EndKey=
    case EndKey of
      ?END_OF_TABLE -> ?UNDEFINED;
      _ -> EndKey
    end,

  dlss_copy:copy(Source,Target,?S_MODULE(Source),#{
    start_key =>StartKey,
    end_key => _EndKey,
    hash => InitHash
  }).

source_node( Segment )->
  ?S_SOURCE( Segment ).

%%=================================================================
%%	SCHEMA API
%%=================================================================
create(Segment,Module,Nodes)->
  ?S_CREATE(Segment,Module,Nodes).

delete( Segment )->
  ?S_DELETE( Segment ).

i_have( Segment )->
  case dlss_rpc:call_all(?READY_NODES, ?MODULE, n_has, [Segment, node()] ) of
    {ok,_}->
      % Everybody knows now
      ?S_ADD(Segment);
    {error,Error}->
      dlss_rpc:cast_all(?S_NODES(Segment),?MODULE, n_doesnt_have,[Segment,node()] ),
      throw(Error)
  end.

n_has( Segment, Node )->
  ?S_ADD_NODE( Segment, Node ).

n_doesnt_have( Segment, Node )->
  ?S_REMOVE_NODE( Segment, Node ).

%%=================================================================
%%	SUBSCRIPTIONS API
%%=================================================================
subscribe( Segment )->
  % We need to subscribe to all nodes, every node can do updates
  case dlss_rpc:call_all(?S_READY_NODES(Segment), dlss_subscription, subscribe, [ Segment, self() ] ) of
    {ok,_} -> ok;
    {error,Error}->
      % All or no one
      unsubscribe( Segment ),
      {error,Error}
  end.

unsubscribe( Segment )->
  dlss_rpc:cast_all(?S_READY_NODES(Segment), dlss_subscription, unsubscribe, [Segment, self()] ),
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



