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

-behaviour(gen_server).

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
  dirty_scan/3
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  get_info/1,
  is_empty/1,
  add_node/2,
  remove_node/2,
  get_size/1,
  get_split_key/2
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  start/1,
  start_link/1,
  stop/1
]).
%%=================================================================
%%	OTP
%%=================================================================
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state,{segment,cycle}).

-define(DEFAULT_SCAN_CYCLE,5000).

-define(MAX_SCAN_INTERVAL_BATCH,1000).

-define(PROCESS(Segment), list_to_atom( atom_to_list(Segment) ++ "_sup" ) ).

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

  % Find out the type of the storage
  StorageType=mnesia_lib:storage_type_at_node(node(),Segment),

  % Define where to stop
  StopGuard=
    if
      To=:='$end_of_table' ->[];  % Nowhere
      true -> [{'=<','$1',{const,To}}]    % Stop at the To key
    end,

  MS=[{#kv{key='$1',value='$2'},StopGuard,[{{'$1','$2'}}]}],

  % Initialize the continuation
  case mnesia_lib:db_select_init(StorageType,Segment,MS,1) of
    {[],'$end_of_table'}->[]; % The segment is empty or there are no keys less than To

    {[{FirstKey,FirstValue}],Cont}->

      % Define the from which to start
      {StartKey,Head}=
        if
          From=:='$start_of_table' ->{FirstKey,[{FirstKey,FirstValue}]} ;
          true ->{From,[]}
        end,

      % Initialize the continuation with the key to start from
      Cont1=init_continuation(Cont,StartKey,Limit),

      % Run the search
      Head++run_continuation(Cont1,StorageType,MS,Limit,[])
  end.

init_continuation('$end_of_table',_StartKey,_Limit)->
  '$end_of_table';
init_continuation({Segment,_LastKey,Par3,_Limit,Ref,Par6,Par7,Par8},StartKey,Limit)->
  % This is the form of ets ordered_set continuation
  {Segment,StartKey,Par3,Limit,Ref,Par6,Par7,Par8};
init_continuation({_LastKey,_Limit,Fun},StartKey,Limit)->
  % This is the form of mnesia_leveldb continuation
  {StartKey,Limit,Fun}.

run_continuation('$end_of_table',_StorageType,_MS,_Limit,Acc)->
  lists:append(lists:reverse(Acc));
run_continuation(_Cont,_StorageType,_MS,Limit,Acc) when Limit=<0->
  lists:append(lists:reverse(Acc));
run_continuation(Cont,StorageType,MS,Limit,Acc)->
  % Run the search
  {Result,Cont1}=mnesia_lib:db_select_cont(StorageType,Cont,MS),
  % Update the acc
  Acc1=
    case Result of
      []->Acc;
      _->[Result|Acc]
    end,
  % Update the limit
  Limit1=
    if
      is_integer(Limit)-> Limit-length(Result);
      true -> Limit
    end,
  run_continuation(Cont1,StorageType,MS,Limit1,Acc1).

%-------------SELECT----------------------------------------------
select(Segment,MS)->
  mnesia:select(Segment,MS).
dirty_select(Segment,MS)->
  mnesia:dirty_select(Segment,MS).

%-------------READ----------------------------------------------
read( Segment, Key )->
  read( Segment, Key, _Lock = none).
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
get_info(Segment)->
  Local = mnesia:table_info(Segment,local_content),
  { Type, Nodes }=get_nodes(Segment),
  #{
    type => Type,
    local => Local,
    nodes => Nodes
  }.

is_empty(Segment)->
  case dirty_first(Segment) of
    '$end_of_table'->true;
    _->false
  end.

add_node(Segment,Node)->
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
  case mnesia:del_table_copy(Segment,Node) of
    {atomic,ok}->ok;
    {aborted,Reason}->{error,Reason}
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

record_size(Term) ->
  size(term_to_binary(Term)).

%----------Calculate the size (bytes) occupied by the segment-------
get_split_key(Segment,Size)->
  get_split_key( mnesia:dirty_first(Segment), Segment, Size, 0).
get_split_key( '$end_of_table' , _Segment, _Size, Acc)->
  { error, { total_size, Acc } };
get_split_key( Key , Segment, Size, Acc) when Acc < Size->
  Acc1=
    case mnesia:dirty_read(Segment,Key) of
      [ Item ] -> Acc + record_size( Item );
      _ -> Acc
    end,
  get_split_key( mnesia:dirty_next( Segment, Key ), Segment, Size, Acc1 );
get_split_key( Key , _Segment, _Size, _Acc)->
  % If we are here then the Acc is bigger than the Size.
  % It means we reached the requested size limit and the Key
  % is the sought key
  Key.

%%=================================================================
%%	API
%%=================================================================
start(Segment)->
  dlss_schema_sup:start_segment(Segment).

start_link(Segment)->
  gen_server:start_link({local, ?PROCESS(Segment) }, ?MODULE, [Segment], []).

stop(Segment)->
  case whereis(?PROCESS(Segment)) of
    PID when is_pid(PID)->
      dlss_schema_sup:stop_segment(PID);
    _ ->
      { error, not_started }
  end.

%%=================================================================
%%	OTP
%%=================================================================
init([Segment])->

  ?LOGINFO("starting segment server for ~p pid ~p",[Segment,self()]),

  Cycle=?ENV(segment_scan_cycle, ?DEFAULT_SCAN_CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{
    segment = Segment,
    cycle = Cycle
  }}.

handle_call(_Params, _From, State) ->
  {reply, {ok,undefined}, State}.

handle_cast({stop,From},State)->
  From!{stopped,self()},
  {stop, normal, State};
handle_cast(_Request,State)->
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(loop,#state{
  segment = Segment,
  cycle = Cycle
}=State)->

  case dlss_storage:segment_params(Segment) of
    {ok, Params} ->
      {ok,_}=timer:send_after(Cycle,loop),
      loop( Segment, Params ),
      {noreply,State};
    { error, Error} ->
      { stop, Error, State }
  end.


terminate(Reason,#state{segment = Segment})->
  ?LOGINFO("terminating segment server ~p, reason ~p",[Segment,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%============================================================================
%%	The Loop
%%============================================================================
loop( Segment, #{ level := 0 } )->
  % The segment is the root segment in its storage.
  % We watch its size and when it reaches the limit we
  % create a new root segment for the storage
  {ok, Segment}.


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