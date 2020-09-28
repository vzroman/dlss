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

-define(DEFAULT_START_TIMEOUT, 600000). % 10 min.
-define(WAIT_SCHEMA_TIMEOUT,5000).
-define(ATTACH_TIMEOUT,600000). %10 min.

-record(state,{
  cycle,
  processes
}).

%%=================================================================
%%	API
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE}, [], []).

%%=================================================================
%%	OTP
%%=================================================================
init([])->

  ?LOGINFO("starting backend ~p",[self()]),

  init_backend(),

  {ok,undefined}.

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
%%	Storage initialization
%%=================================================================
init_backend()->
  case {is_defined(),?ENV("FORCE",force,"false")} of
    % Backend exists, no forced init, just wait it is up
    {true,"false"}->
      ?LOGINFO("waiting backend to init"),
      ok=start(),
      ?LOGINFO("backend started");
    % Starting the storage in the forced mode
    {true,"true"}->
      ?LOGWARNING("starting backend in the forced mode"),

      % Prepare mnesia
      mnesia:create_schema([node()]),
      ok=mnesia:start(),

      % Forcing the local copy loading from the disk
      forced_start(),

      wiat_segments(?ENV(start_timeout,?DEFAULT_START_TIMEOUT)),

      ?LOGINFO("backend started");
    % Backend is not defined yet
    {false,_}->
      case ?ENV("AS_NODE",as_node,"false") of
        % Starting as master. Create system objects
        "false"->
          ?LOGINFO("the schema is not defined yet, create a new one"),
          mnesia:create_schema([node()]),
          ok=start(),

          ?LOGINFO("backend started");
        % Starting as node. Waiting the master to attach backend
        "true"->
          ?LOGINFO("the schema is not defined yet, the node is starting AS_NODE"),
          ok=mnesia:start(),

          ?LOGINFO("waiting for the master node to copy the schema"),
          wait_schema(),

          ?LOGINFO("waiting for the segments avaiability"),
          wiat_segments(?ENV(start_timeout,?DEFAULT_START_TIMEOUT)),

          ?LOGINFO("backend started")
      end
  end.

%% Check if the schema is already defined for the node
is_defined()->
  % TODO. What is the better way to define if the schema is already created
  case mnesia:create_schema([node()]) of
    ok->
      mnesia:delete_schema([node()]),
      false;
    {error,{_,{already_exists,_}}}->
      true
  end.

start()->

  ok=mnesia:start(),

  % Register leveldb backend. !!! Many thanks to Klarna and Basho developers
  mnesia_eleveldb:register(),

  % Wait tables to recover
  wiat_segments(?ENV(start_timeout,?DEFAULT_START_TIMEOUT)).

wiat_segments(Timeout)->
  Segments=
    [T||T<-mnesia:system_info(tables),
      case ?A2B(T) of
        <<"dlss_",_/binary>>->true;
        _->false
      end],
  mnesia:wait_for_tables(Segments,Timeout).

forced_start()->
  Node=node(),
  mnesia:wait_for_tables([schema],?WAIT_SCHEMA_TIMEOUT),
  case mnesia:table_info(schema,master_nodes) of
    [Node]->ok;
    _->
      case mnesia:set_master_nodes([Node]) of
        ok->
          mnesia:stop(),
          mnesia:start();
        {error,Error}->
          ?LOGERROR("error set master node ~p, error ~p",[Node,Error])
      end
  end.

wait_schema()->
  Node=node(),
  case mnesia:system_info(running_db_nodes) of
    [Node]->
      timer:sleep(1000),
      wait_schema();
    Nodes when length(Nodes)>1->
      {atomic,ok}=mnesia:change_table_copy_type(schema,node(),disc_copies)
  end.





