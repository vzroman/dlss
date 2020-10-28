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
%%	STORAGE SEGMENT API
%%=================================================================
-export([
  dirty_first/1,
  dirty_next/2,
  dirty_read/2,
  dirty_scan/3,
  dirty_write/2,dirty_write/3,
  dirty_delete/2
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  get_info/1
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/1
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

%%=================================================================
%%	STORAGE SEGMENT API
%%=================================================================
dirty_first(Segment)->
  mnesia:dirty_first(Segment).
dirty_next(Segment,Pattern)->
  mnesia:dirty_next(Segment,Pattern).

dirty_read(Segment,Key)->
  case mnesia:dirty_read(Segment,Key) of
    [#kv{value = Value}]->Value;
    _->not_found
  end.

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

dirty_write(Segment,{Key,Value})->
  dirty_write(Segment,Key,Value).
dirty_write(Segment,Key,Value)->
  mnesia:dirty_write(Segment,#kv{key = Key,value = Value}).

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


%%=================================================================
%%	API
%%=================================================================
start_link(Segment)->
  % The process is registered locally to not to conflict
  % with the processes that handle the same segment on  other nodes.
  % This gives us a more explicit way to address a segment handler
  % of the explicitly defined node and do a load balancing for dirty
  % mode operations
  gen_server:start_link({local,?MODULE}, ?MODULE, [Segment], []).



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
  {ok,_}=timer:send_after(Cycle,loop),
  {noreply,State}.

terminate(Reason,#state{segment = Segment})->
  ?LOGINFO("terminating segment server ~p, reason ~p",[Segment,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


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