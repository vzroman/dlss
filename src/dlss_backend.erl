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

-module(dlss_backend).

-include("dlss.hrl").

-behaviour(gen_server).

%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/0
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

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).
-export([
  init_backend/0,
  stop/0
]).
-endif.


-define(DEFAULT_START_TIMEOUT, 600000). % 10 min.
-define(WAIT_SCHEMA_TIMEOUT,5000).
-define(ATTACH_TIMEOUT,600000). %10 min.
-define(DEFAULT_MASTER_CYCLE, 1000).

-record(state,{
  cycle
}).

%%=================================================================
%%	API
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

%%=================================================================
%%	OTP
%%=================================================================
init([])->

  ?LOGINFO("starting backend ~p",[self()]),

  init_backend(),

  dlss_node:set_status(node(),ready),

  Cycle=?ENV(master_node_cycle,?DEFAULT_MASTER_CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{cycle = Cycle}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("backend got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.


handle_cast(Request,State)->
  ?LOGWARNING("backend got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(Message,State)->
  ?LOGWARNING("backend got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGINFO("terminating backend reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%=================================================================
%%	Backend initialization
%%=================================================================
init_backend()->
  % Create mnesia schema
  IsFirstStart=
    case mnesia:create_schema([node()]) of
      ok->
        true;
      {error,{_,{already_exists,_}}}->
        false;
      {error,Error}->
        ?LOGERROR("FATAL! Unable to create the schema ~p",[Error]),
        ?ERROR(Error)
    end,

  %% Next steps need the mnesia started
  ?LOGINFO("starting mnesia"),
  % We want to see in the console what's happening
  mnesia:set_debug_level(debug),
  ok=mnesia:start(),
  % Register leveldb backend. !!! Many thanks to Klarna and Basho developers
  mnesia_eleveldb:register(),

  ?LOGINFO("dlss initalization"),
  if
    IsFirstStart ->
      ?LOGINFO("schema is not defined yet"),

      AsNode=?ENV("AS_NODE",as_node,"false"),
      if
        AsNode=:="true";AsNode=:=true ->
          ?LOGINFO("node is starting as slave"),

          ?LOGINFO("restarting mnesia"),
          mnesia:stop(),
          ok=mnesia:delete_schema([node()]),
          ok=mnesia:start(),
          mnesia_eleveldb:register(),

          ?LOGINFO("waiting for the schema from the master node..."),
          wait_for_schema(),

          ?LOGINFO("waiting for segemnts availability..."),
          wiat_segments(?ENV(start_timeout,?DEFAULT_START_TIMEOUT));
        true ->
          ?LOGINFO("node is starting as master"),
          create_schema()
      end;
    true ->
      IsForced=?ENV("FORCE",force,"false"),
      if
        IsForced=:="true";IsForced=:=true ->
          ?LOGWARNING("starting in FORCED mode"),
          set_forced_mode(),

          ?LOGWARNING("restarting mnesia"),
          mnesia:stop(),
          ok=mnesia:start(),
          mnesia_eleveldb:register(),

          ?LOGINFO("waiting for schema availability..."),
          mnesia:wait_for_tables([schema,dlss_schema],?WAIT_SCHEMA_TIMEOUT),

          ?LOGINFO("waiting for segemnts availability..."),
          wiat_segments(?ENV(start_timeout,?DEFAULT_START_TIMEOUT));
        true ->
          ?LOGINFO("node is starting in normal mode"),

          ?LOGINFO("waiting for schema availability..."),
          mnesia:wait_for_tables([schema,dlss_schema],?WAIT_SCHEMA_TIMEOUT),

          ?LOGINFO("waiting for segemnts availability..."),
          wiat_segments(?ENV(start_timeout,?DEFAULT_START_TIMEOUT))
      end
  end,

  mnesia:set_debug_level(none),
  ?LOGINFO("dlss is ready").

create_schema()->
  mnesia:create_table(dlss_schema,[
    {attributes, record_info(fields, kv)},
    {record_name, kv},
    {type,ordered_set},
    {disc_copies,[node()]},
    {ram_copies,[]}
  ]).

stop()->
  mnesia:stop().


wiat_segments(Timeout)->
  Segments=dlss:get_segments(),
  mnesia:wait_for_tables(Segments,Timeout).

set_forced_mode()->
  case mnesia:set_master_nodes([node()]) of
    ok->ok;
    {error,Error}->
      ?LOGERROR("error set master node ~p, error ~p",[node(),Error]),
      ?ERROR(Error)
  end.

wait_for_schema()->
  % Wait master node to attach this node to the schema
  wait_for_master(),
  % Copy mnesia schema
  case mnesia:change_table_copy_type(schema,node(),disc_copies) of
    {atomic,ok}->ok;
    {aborted,Reason1}->
      ?LOGERROR("unable to copy mnesia schema ~p",[Reason1]),
      ?ERROR(Reason1)
  end,

  % copy dlss schema
  ?LOGINFO("waiting for dlss schema availability..."),
  mnesia:wait_for_tables([dlss_schema],?WAIT_SCHEMA_TIMEOUT),
  case mnesia:add_table_copy(dlss_schema,node(),disc_copies) of
    {atomic,ok}->ok;
    {aborted,Reason2}->
      ?LOGERROR("unable to copy dlss schema ~p",[Reason2]),
      ?ERROR(Reason2)
  end.

wait_for_master()->
  Node=node(),
  case mnesia:system_info(running_db_nodes) of
    [Node]->
      ?LOGINFO("...waiting attach"),
      timer:sleep(5000),
      wait_for_master();
    Nodes when length(Nodes)>1->
      % If there are more than one node in the schema then the schema has arrived
      ok
  end.





