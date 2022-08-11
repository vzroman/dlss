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

%%=================================================================
%%	STORAGE READ/WRITE API
%%=================================================================
-export([
  read/2,read/3,dirty_read/2,
  write/3,write/4,dirty_write/3,
  delete/2,delete/3,dirty_delete/2,
  dirty_counter/3
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

-record(iter,{cont,limit}).
-record(cont,{cont,type,ms}).

-define(DEFAULT_BATCH,1000).

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

  % Choose an algorithm
  if
    To =:= '$end_of_table'; Type =:= leveldb_copies->
      dirty_select(Segment, Type, From, To, Limit );
    is_number(Limit)->
      ?LOGDEBUG("------------SAFE SCAN: from ~p, to ~p, limit ~p-------------",[From,To,Limit]),
      safe_scan(Segment,From,To,Limit);
    true->
      ?LOGDEBUG("------------SAFE SCAN NO LIMIT: from ~p, to ~p-------------",[From,To]),
      safe_scan(Segment,From,To)
  end.

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

%---------------------------DIRTY SELECT FROM START, NO LIMIT---------------------------
dirty_select(Segment, _Type, '$start_of_table', To, Limit)
  when not is_number(Limit)->

  ?LOGDEBUG("------------SAFE SCAN ALL TABLE, NO LIMIT-------------"),
  MS=
    [{#kv{key='$1',value='$2'},[],[{{'$1','$2'}}]}],
  mnesia:dirty_select(Segment, MS);



  case init_iterator(Segment, Type, From, To, Limit) of
    '$end_of_table' -> [];
    Iterator -> iterate(Iterator)
  end.

init_iterator(Segment, Type, '$start_of_table', To, Limit ) when is_number(Limit)->


  case init_continuation( Segment, Type, From ) of
    {Head, Cont}->
      [Head| iterate()]
  end,
  MS=
    [{#kv{key='$1',value='$2'},[],[{{'$1','$2'}}]}],

  Iterator = #iter{
    ms = MS,
    type = Type,
    limit = Limit,
    batch = BatchSize,
    to = To,
    acc = []
  },

  case mnesia_lib:db_select_init(Type,Segment,MS,1) of
    {[{Key,_}],_Cont} when To =/='$end_of_table', Key > To->
      % there are no keys less than To
      '$end_of_table';
    {[{Key,_} = Head],Cont} when From =:= '$start_of_table'; Key >= From->
      % The first key is greater than or equals the From key, take it
      decrement( Iterator#iter{ cont = Cont,acc = [Head]}, 1);
    {[_Entry],Cont0}->
      % The first key is less than the From key. This the point for the trick.
      % Replace the key with the From key in the continuation
      Cont = init_continuation( Cont0, From ),

      % define the head
      case mnesia:dirty_read(Segment,From) of
        [#kv{value = Value}]->
          % There is a value for the From key
          decrement( Iterator#iter{cont = Cont, acc = [{From,Value}]}, 1 );
        _->
          % No value for the From key
          Iterator#iter{ cont = Cont }
      end;
    '$end_of_table'->
      '$end_of_table';
    {[],'$end_of_table'}->
      % The segment is empty
      '$end_of_table'
  end.

init_continuation({Segment,_KeyToReplace,Par3,Limit,Ref,Par6,Par7,Par8}, Key )->
  % This is the form of ets ordered_set continuation
  {Segment,Key,Par3,Limit,Ref,Par6,Par7,Par8};
init_continuation({_KeyToReplace,Limit,Fun}, Key )->
  % This is the form of ets ordered_set continuation
  {Key,Limit,Fun}.

decrement(#iter{ limit = Limit } = Iter, Decr ) when is_number(Limit)->
  Iter#iter{ limit = Limit - Decr };
decrement(Iter, _Decr)->
  Iter.

prepare_continuation({Segment,Key,Par3,_Limit,Ref,Par6,Par7,Par8}, Limit)->
  {Segment,Key,Par3,Limit,Ref,Par6,Par7,Par8};
prepare_continuation({Key,_Limit,Fun}, Limit)->
  {Key,Limit,Fun}.

iterate(#iter{cont=Cont,limit = Limit, acc = Acc}) when Limit =<0 ; Cont =:= '$end_of_table' ->
  Acc;
iterate(#iter{
  cont=Cont0,
  ms=MS,
  type=Type,
  limit = Limit,
  batch = Batch,
  to = To,
  acc = Acc0
}=Iter)->

  % Prepare the continuation
  Size =
    if
      Limit > Batch -> Batch;
      true -> Limit
    end,
  Cont = prepare_continuation( Cont0, Size ),

  case mnesia_lib:db_select_cont(Type,Cont,MS) of
    {Entries0, NextCont} ->
      Entries = filter_entries( Entries0, To ),
      Acc = if length( Acc0 ) > 0 -> Acc0 ++ Entries; true -> Entries end,
      if
        length( Entries ) =:= Size->
          % The batch is full, continue
          iterate( decrement(Iter#iter{cont = NextCont, acc = Acc}, Size) );
        true ->
          % There are no more keys
          Acc
      end;
    '$end_of_table'->
      Acc0
  end.

filter_entries( Entries, To ) when To =:= '$end_of_table'->
  Entries;
filter_entries( Entries, To )->
  do_filter_entries( Entries, To ).
do_filter_entries([{Key,_}=E|Rest], To) when Key =< To->
  [E | filter_entries(Rest, To)];
do_filter_entries(_Rest, _To)->
  [].

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

dirty_counter( Segment, Key, Incr )->
  mnesia:dirty_update_counter( Segment, Key, Incr ).

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
  add_node(Segment, Node, mnesia:table_info(Segment, access_mode)).
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
  Memory = mnesia:table_info(Segment,memory),
  case get_info(Segment) of
    #{type := disc} -> Memory;
    _ ->
      % for ram and ramdisc tables the mnesia returns a number of allocated words
      erlang:system_info(wordsize) * Memory
  end.

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