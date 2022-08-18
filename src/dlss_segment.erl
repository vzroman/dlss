%%----------------------------------------------------------------
%% Copyright (c) 2020 Faceplate
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
-include("dlss_eleveldb.hrl").

%%=================================================================
%%	STORAGE READ/WRITE API
%%=================================================================
-export([
  read/2,read/3,dirty_read/2,
  write/3,write/4,dirty_write/3,
  delete/2,delete/3,dirty_delete/2
]).

%%=================================================================
%%	STORAGE ITERATOR API
%%=================================================================
-export([
  first/1,dirty_first/1,
  last/1,dirty_last/1,
  next/2,dirty_next/2,
  prev/2,dirty_prev/2,
  %----OPTIMIZED SCANNING------------------
  select/2,dirty_select/2,
  dirty_scan/3,dirty_scan/4,
  do_dirty_scan/4
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/2,
  remove/1,
  get_info/1,
  get_local_segments/0,
  is_empty/1,
  add_node/2,
  remove_node/2,
  get_size/1,
  get_access_mode/1,
  set_access_mode/2,
  get_active_nodes/1,
  in_read_write_mode/2
]).

%%=================================================================
%%	Subscriptions API
%%=================================================================
-export([
  start_link/1
]).

-define(MAX_SIZE(Type),
  if
    Type=:=leveldb_copies -> 1 bsl 128;
    true-> (1 bsl 59) - 1 % Experimentally set that it's the max continuation limit for ets
  end).

%%=================================================================
%%	STORAGE SEGMENT API
%%=================================================================
%-------------ITERATOR----------------------------------------------
first(Segment)->
  mnesia:first(Segment).
dirty_first(Segment)->
  mnesia:dirty_first(Segment).

last(Segment)->
  mnesia:last(Segment).
dirty_last(Segment)->
  mnesia:dirty_last(Segment).

next(Segment,Key)->
  mnesia:next(Segment,Key).
dirty_next(Segment,Key)->
  mnesia:dirty_next(Segment,Key).

prev(Segment,Key)->
  mnesia:prev(Segment,Key).
dirty_prev(Segment,Key)->
  mnesia:dirty_prev(Segment,Key).

%-------------INTERVAL SCAN----------------------------------------------
dirty_scan(Segment,From,To)->
  dirty_scan(Segment,From,To,infinity).
dirty_scan(Segment,From,To,Limit)->
  Node = mnesia:table_info( Segment, where_to_read ),
  if
    Node =:= nowhere ->[];
    Node =:= node()->
      do_dirty_scan(Segment,From,To,Limit);
    true->
      case rpc:call(Node, ?MODULE, do_dirty_scan, [ Segment,From,To,Limit ]) of
        {badrpc, _Error} ->[];
        Result -> Result
      end
  end.

do_dirty_scan(Segment,From,To,Limit)->

  % Find out the type of the storage
  Type=mnesia_lib:storage_type_at_node(node(),Segment),
  ?LOGDEBUG("type ~p",[Type]),

  % Choose an algorithm
  if
    Type =:= {ext,leveldb_copies,mnesia_eleveldb}->
      disc_scan(Segment,From,To,Limit);
    To =:= '$end_of_table'->
      dirty_select(Segment, Type, From, To, Limit );
    is_number(Limit)->
      ?LOGDEBUG("------------SAFE SCAN: from ~p, to ~p, limit ~p-------------",[From,To,Limit]),
      safe_scan(Segment,From,To,Limit);
    true->
      ?LOGDEBUG("------------SAFE SCAN NO LIMIT: ~p, from ~p, to ~p-------------",[Segment,From,To]),
      safe_scan(Segment,From,To)
  end.

%----------------------DISC SCAN ALL TABLE, NO LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table','$end_of_table',Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN ALL TABLE, NO LIMIT-------------"),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?DATA_START),I) end);

%----------------------DISC SCAN ALL TABLE, WITH LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table','$end_of_table',Limit)->
  ?LOGDEBUG("------------DISC SCAN ALL TABLE, LIMIT ~p-------------",[Limit]),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?DATA_START),I,Limit) end);

%----------------------DISC SCAN FROM START TO KEY, NO LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table',To,Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM START TO KEY ~p, NO LIMIT-------------",[To]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?DATA_START),I,?ENCODE_KEY(To)) end);

%----------------------DISC SCAN FROM START TO KEY, WITH LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table',To,Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM START TO KEY ~p, WITH LIMIT ~p-------------",[To,Limit]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?DATA_START),I,?ENCODE_KEY(To),Limit) end);

%----------------------DISC SCAN FROM KEY TO END, NO LIMIT-----------------------------------------
disc_scan(Segment,From,'$end_of_table',Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO END, NO LIMIT-------------",[From]),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?ENCODE_KEY(From)),I) end);

%----------------------DISC SCAN FROM KEY TO END, WITH LIMIT-----------------------------------------
disc_scan(Segment,From,'$end_of_table',Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO END, WITH LIMIT ~p-------------",[From,Limit]),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?ENCODE_KEY(From)),I,Limit) end);

%----------------------DISC SCAN FROM KEY TO KEY, NO LIMIT-----------------------------------------
disc_scan(Segment,From,To,Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO ~p, NO LIMIT-------------",[From,To]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?ENCODE_KEY(From)),I,?ENCODE_KEY(To)) end);

%----------------------DISC SCAN FROM KEY TO KEY, WITH LIMIT-----------------------------------------
disc_scan(Segment,From,To,Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO ~p, WITH LIMIT ~p-------------",[From,To,Limit]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?ENCODE_KEY(From)),I,?ENCODE_KEY(To),Limit) end).

fold(Segment,Fold) ->
  Ref = ?REF(Segment),
  {ok, Itr} = eleveldb:iterator(Ref, []),
  try Fold(Itr)
  after
    catch eleveldb:iterator_close(Itr)
  end.

%-------------NO LIMIT, NO STOP KEY----------------------------
do_fold({ok,K,V},I)->
  [{?DECODE_KEY(K),?DECODE_VALUE(V)}|do_fold(?NEXT(I),I)];
do_fold(_,_I)->
  [].

%-------------WITH LIMIT, NO STOP KEY----------------------------
do_fold({ok,K,V},I,Limit) when Limit>0 ->
  [{?DECODE_KEY(K),?DECODE_VALUE(V)}|do_fold(?NEXT(I),I,Limit-1)];
do_fold(_,_I,_Limit)->
  [].

%-------------NO LIMIT, WITH STOP KEY----------------------------
do_fold_to({ok,K,V},I,Stop) when K=<Stop->
  [{?DECODE_KEY(K),?DECODE_VALUE(V)}|do_fold_to(?NEXT(I),I,Stop)];
do_fold_to(_,_I,_S)->
  [].

%-------------WITH LIMIT, WITH STOP KEY----------------------------
do_fold_to({ok,K,V},I,Stop,Limit) when K=<Stop, Limit>0->
  [{?DECODE_KEY(K),?DECODE_VALUE(V)}|do_fold_to(?NEXT(I),I,Stop,Limit-1)];
do_fold_to(_,_I,_S,_L)->
  [].

%----------------------SAFE SCAN NO LIMIT-----------------------------------------
safe_scan(Segment,'$start_of_table',To)->
  do_safe_scan(mnesia:dirty_first(Segment), Segment, To );
safe_scan(Segment,From,To)->
  do_safe_scan(From, Segment, To ).

do_safe_scan(Key, Segment, To) when Key =/= '$end_of_table', Key =< To->
  case mnesia:dirty_read(Segment,Key) of
    [#kv{value = Value}]->[{Key,Value} | do_safe_scan( mnesia:dirty_next(Segment,Key), Segment, To)];
    _->do_safe_scan( mnesia:dirty_next(Segment,Key), Segment, To )
  end;
do_safe_scan(_,_,_)->
  [].

%----------------------SAFE SCAN WITH LIMIT-----------------------------------------
safe_scan(Segment,'$start_of_table',To,Limit)->
  do_safe_scan(mnesia:dirty_first(Segment), Segment, To, Limit );
safe_scan(Segment,From,To, Limit)->
  do_safe_scan(From, Segment, To, Limit ).

do_safe_scan(Key, Segment, To, Limit) when Limit > 0, Key =/= '$end_of_table', Key =< To->
  case mnesia:dirty_read(Segment,Key) of
    [#kv{value = Value}]->[{Key,Value} | do_safe_scan( mnesia:dirty_next(Segment,Key), Segment, To, Limit - 1 )];
    _->do_safe_scan( mnesia:dirty_next(Segment,Key), Segment, To, Limit )
  end;
do_safe_scan(_,_,_,_)->
  [].

%---------------------------DIRTY SELECT ALL TABLE, NO LIMIT---------------------------
dirty_select(Segment, _Type, '$start_of_table', '$end_of_table', Limit)
  when not is_number(Limit)->

  ?LOGDEBUG("------------DIRTY SELECT ALL TABLE, NO LIMIT-------------"),
  MS=
    [{#kv{key='$1',value='$2'},[],[{{'$1','$2'}}]}],
  mnesia:dirty_select(Segment, MS);
%---------------------------DIRTY SELECT ALL TABLE, WITH LIMIT---------------------------
dirty_select(Segment, Type, '$start_of_table', '$end_of_table', Limit) ->

  ?LOGDEBUG("------------DIRTY SELECT ALL TABLE, LIMIT ~p-------------",[Limit]),
  MS=
    [{#kv{key='$1',value='$2'},[],[{{'$1','$2'}}]}],
  case mnesia_lib:db_select_init(Type,Segment,MS,Limit) of
    {Result, _Cont}->
      Result;
    '$end_of_table'->
      []
  end;

%---------------------------DIRTY SELECT FROM START TO KEY, NO LIMIT---------------------------
dirty_select(Segment, _Type, '$start_of_table', To, Limit)
  when not is_number(Limit)->

  ?LOGDEBUG("------------DIRTY SELECT FROM START TO KEY ~p, NO LIMIT-------------",[To]),
  MS=
    [{#kv{key='$1',value='$2'},[{'=<','$1',{const,To}}],[{{'$1','$2'}}]}],
  mnesia:dirty_select(Segment, MS);

%---------------------------DIRTY SELECT FROM START TO KEY WITH LIMIT---------------------------
dirty_select(Segment, Type, '$start_of_table', To, Limit) ->

  ?LOGDEBUG("------------DIRTY SELECT FROM START TO KEY ~p WITH LIMIT ~p-------------",[To,Limit]),
  MS=
    [{#kv{key='$1',value='$2'},[{'=<','$1',{const,To}}],[{{'$1','$2'}}]}],

  case mnesia_lib:db_select_init(Type,Segment,MS,Limit) of
    {Result, _Cont}->
      Result;
    '$end_of_table'->
      []
  end;

%---------------------------DIRTY SELECT FROM KEY TO END, NO LIMIT---------------------------
dirty_select(Segment, Type, From, '$end_of_table', Limit)
  when not is_number(Limit)->

  ?LOGDEBUG("------------DIRTY SELECT FROM KEY ~p to END, NO LIMIT-------------",[From]),

  MS=
    [{#kv{key='$1',value='$2'},[],[{{'$1','$2'}}]}],

  case init_continuation( Segment, Type, From, MS, ?MAX_SIZE(Type) ) of
    {Head, '$end_of_table'}->
      [Head];
    {Head, Cont}->
      [Head | eval_continuation(Type,Cont,MS)];
    '$end_of_table' ->
      [];
    Cont->
      eval_continuation(Type,Cont,MS)
  end;
%---------------------------DIRTY SELECT FROM KEY TO END WITH LIMIT---------------------------
dirty_select(Segment, Type, From, '$end_of_table', Limit) ->

  ?LOGDEBUG("------------DIRTY SELECT FROM KEY ~p TO END WITH LIMIT ~p-------------",[From,Limit]),

  MS=
    [{#kv{key='$1',value='$2'},[],[{{'$1','$2'}}]}],

  case init_continuation( Segment, Type, From, MS, Limit) of
    {Head, '$end_of_table'}->
      [Head];
    {Head, Cont}->
      [Head | eval_continuation(Type,Cont,MS)];
    '$end_of_table' ->
      [];
    Cont->
      eval_continuation(Type,Cont,MS)
  end;
%---------------------------DIRTY SELECT FROM KEY TO KEY, NO LIMIT---------------------------
dirty_select(Segment, Type, From, To, Limit)
  when not is_number(Limit)->

  MS=
    [{#kv{key='$1',value='$2'},[{'=<','$1',{const,To}}],[{{'$1','$2'}}]}],

  ?LOGDEBUG("------------DIRTY SELECT FROM KEY ~p to KEY ~p, NO LIMIT-------------",[From,To]),
  case init_continuation( Segment, Type, From, MS, ?MAX_SIZE(Type)) of
    {Head, '$end_of_table'}->
      [Head];
    {Head, Cont}->
      [Head | eval_continuation(Type,Cont,MS)];
    '$end_of_table' ->
      [];
    Cont->
      eval_continuation(Type,Cont,MS)
  end;
%---------------------------DIRTY SELECT FROM KEY TO KEY, WITH LIMIT---------------------------
dirty_select(Segment, Type, From, To, Limit) ->

  MS=
    [{#kv{key='$1',value='$2'},[{'=<','$1',{const,To}}],[{{'$1','$2'}}]}],

  ?LOGDEBUG("------------DIRTY SELECT FROM KEY ~p to KEY ~p, LIMIT ~p-------------",[From,To,Limit]),
  case init_continuation( Segment, Type, From, MS, Limit) of
    {Head, '$end_of_table'}->
      [Head];
    {Head, Cont}->
      [Head | eval_continuation(Type,Cont,MS)];
    '$end_of_table' ->
      [];
    Cont->
      eval_continuation(Type,Cont,MS)
  end.

init_continuation( Segment, Type, From, MS, Limit )->
  case mnesia_lib:db_select_init(Type,Segment,MS,1) of
    {[{Key,_} = Head], Cont} when Key >= From->
      % The first key is greater than or equals the From key, take it
      {Head, decrement(Cont, -Limit)};
    {[_First],Cont0}->
      % The first key is less than the From key. This the point for the trick.
      % Replace the key with the From key in the continuation

      % Check if the from key exists
      case mnesia:dirty_read(Segment,From) of
        [#kv{value = Value}]->
          {{From,Value}, trick_continuation( Cont0, From, Limit - 1) };
        _->
          % No value for the From key
          trick_continuation( Cont0, From, Limit)
      end;
    '$end_of_table'->
      '$end_of_table';
    {[],'$end_of_table'}->
      % The segment is empty
      '$end_of_table'
  end.

trick_continuation({Segment,_KeyToReplace,Par3,_Limit,Ref,Par6,Par7,Par8}, Key, Limit )->
  % This is the form of ets ordered_set continuation
  {Segment,Key,Par3,Limit,Ref,Par6,Par7,Par8}.

decrement({Segment,Key,Par3,Limit,Ref,Par6,Par7,Par8}, Decr )->
  Rest = Limit - Decr,
  if
    Rest > 0 -> {Segment,Key,Par3,Rest,Ref,Par6,Par7,Par8};
    true -> '$end_of_table'
  end.

eval_continuation(Type,Cont,MS)->
  case mnesia_lib:db_select_cont(Type,Cont,MS) of
    {Result,'$end_of_table'} ->
      Result;
    {Result,NextCont0}->
      case decrement(NextCont0,length(Result)) of
        '$end_of_table'-> Result;
        NextCont-> Result ++ eval_continuation( Type, NextCont, MS )
      end;
    '$end_of_table' ->
      [];
    Result->
      Result
  end.

%-------------SELECT----------------------------------------------
select(Segment,MS)->
  mnesia:select(Segment,MS).
dirty_select(Segment,MS)->
  mnesia:dirty_select(Segment,MS).

%-------------READ----------------------------------------------
read( Segment, Key )->
  read( Segment, Key, _Lock = read).
read( Segment, Key, Lock)->
  case mnesia:read(Segment,Key,Lock) of
    [#kv{value = Value}]->Value;
    _->not_found
  end.
dirty_read(Segment,Key)->
  case mnesia:dirty_read(Segment,Key) of
    [#kv{value = Value}]->Value;
    _->not_found
  end.

%-------------WRITE----------------------------------------------
write(Segment,Key,Value)->
  write(Segment,Key,Value, _Lock = none).
write(Segment,Key,Value,Lock)->
  mnesia:write(Segment,#kv{key = Key,value = Value}, Lock).

dirty_write(Segment,Key,Value)->
  mnesia:dirty_write(Segment,#kv{key = Key,value = Value}).

%-------------DELETE----------------------------------------------
delete(Segment,Key)->
  delete(Segment,Key,_Lock=none).
delete(Segment,Key,Lock)->
  mnesia:delete(Segment,Key,Lock).
dirty_delete(Segment,Key)->
  mnesia:dirty_delete(Segment,Key).

%%=================================================================
%%	Service API
%%=================================================================
create(Name,#{nodes := Nodes0} = Params0)->
  % The segment can be created only on ready nodes. The nodes that are
  % not active now will add it later by storage supervisor
  ReadyNodes = dlss:get_ready_nodes(),
  Nodes = ordsets:intersection(ordsets:from_list(Nodes0), ordsets:from_list(ReadyNodes)),

  if
    length(Nodes) > 0 ->
      Params = Params0#{
        nodes => Nodes
      },

      Attributes = table_attributes(Params),
      case mnesia:create_table(Name,[
        {attributes,record_info(fields,kv)},
        {record_name,kv},
        {type,ordered_set}|
        Attributes
      ]) of
        {atomic, ok } -> ok;
        {aborted, Reason } -> {error, Reason}
      end;
    true ->
      { error, none_of_the_nodes_is_ready }
  end.

remove(Name)->
  case set_access_mode( Name, read_write ) of
    ok ->
      case mnesia:delete_table(Name) of
        {atomic,ok}->ok;
        {aborted,Reason}-> {error, Reason }
      end;
    SetModeError ->
      SetModeError
  end.

get_info(Segment)->
  Local = mnesia:table_info(Segment,local_content),
  { Type, Nodes }=get_nodes(Segment),
  #{
    type => Type,
    local => Local,
    nodes => Nodes
  }.

get_local_segments()->
  [T || T <- mnesia:system_info( local_tables ),
    case atom_to_binary(T, utf8) of
      <<"dlss_schema">> -> false;
      <<"dlss_",_/binary>> -> true;
      _ -> false
    end].

is_empty(Segment)->
  case dirty_first(Segment) of
    '$end_of_table'->true;
    _->false
  end.

add_node(Segment,Node)->
  % ATTENTION! Mnesia trick, we do copy ourselves and tell mnesia to add it
  if
    Node =:= node()->
      case try dlss_copy:copy(Segment,Segment)
      catch _:E->{error,E} end of
        Hash when is_binary( Hash )->
          add_node(Segment, Node, mnesia:table_info(Segment, access_mode));
        Error -> Error
      end;
    true->
      case rpc:call(Node, dlss_copy, copy, [ Segment,Segment ]) of
        {badrpc, Error} -> {error,Error};
        _ -> add_node(Segment, Node, mnesia:table_info(Segment, access_mode))
      end
  end.
add_node(Segment, Node, read_only)->
  in_read_write_mode(Segment, fun()->
    add_node(Segment, Node, read_write )
  end);
add_node(Segment,Node, _AccessMode)->
  #{ type:=Type } = get_info(Segment),
  MnesiaType =
    if
      Type =:= ram      -> ram_copies;
      Type =:= ramdisc  -> disc_copies;
      Type =:= disc     -> leveldb_copies
    end,
  case mnesia:add_table_copy(Segment,Node,MnesiaType) of
    {atomic,ok} -> ok;
    {aborted,Reason}->{error,Reason}
  end.

remove_node(Segment,Node)->
  remove_node(Segment, Node, mnesia:table_info(Segment, access_mode)).
remove_node(Segment, Node, read_only)->
  in_read_write_mode(Segment, fun()->
    remove_node(Segment, Node, read_write )
  end);
remove_node(Segment, Node, _AccessMode)->
  case get_active_nodes( Segment ) of
    [ Node ]->
      {error, only_copy};
    _ ->
      case mnesia:del_table_copy(Segment,Node) of
        {atomic,ok}->ok;
        {aborted,Reason}->{error,Reason}
      end
  end.

in_read_write_mode(Segment,Fun)->
  % There is a bug in mnesia with adding/removing copies of read_only tables.
  % Mnesia tries to set lock on the table, for that it takes
  % where_to_wlock nodes but for read_only tables this list is empty
  % and mnesia throws a error { no_exists, Tab }.
  case set_access_mode( Segment, read_write ) of
    ok ->
      Result = Fun(),
      set_access_mode( Segment, read_only ),
      Result;
    AccessError -> AccessError
  end.
%----------Calculate the size (bytes) occupied by the segment-------
get_size(Segment)->
  dlss_copy:get_size( Segment ).

get_access_mode( Segment )->
  mnesia:table_info( Segment, access_mode ).

set_access_mode( Segment , Mode )->
  case mnesia:change_table_access_mode(Segment, Mode) of
    {atomic,ok} -> ok;
    {aborted,{already_exists,Segment,Mode}}->ok;
    {aborted,Reason}->{error,Reason}
  end.

get_active_nodes( Segment )->
  mnesia:table_info(Segment, active_replicas).

%%============================================================================
%%	Internal helpers
%%============================================================================
get_nodes(Segment)->
  Nodes=[{T,mnesia:table_info(Segment,CT)}||{CT,T}<-[
    {disc_copies,ramdisc},
    {ram_copies,ram},
    {leveldb_copies,disc}
  ]],
  case [{T,N}||{T,N}<-Nodes,N=/=[]] of
    [Result]->Result;
    _->throw(invalid_storage_type)
  end.

table_attributes(#{
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

%%=================================================================
%%	Subscriptions API
%%=================================================================
start_link( Segment )->
  ?LOGINFO("~p register service process ~p",[Segment, self()]),
  register(Segment, self()),

  wait_loop(#{}).

wait_loop(Subscriptions)->

