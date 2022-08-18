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

-module(dlss_storage_sup).

-include("dlss.hrl").

-behaviour(supervisor).

%%=================================================================
%%	OTP
%%=================================================================
-export([
  start_supervisor/0,
  start_server/0
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  start_storage/1,
  stop_storage/1
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

-define(SUPERVISOR,?MODULE).
-define(SERVER, list_to_atom(atom_to_list(?MODULE) ++"_srv" )).

-define(CYCLE,1000).
-define(MIN_ULIMIT_N, 200000).
-record(state,{ storages }).

%%=================================================================
%%	API
%%=================================================================
start_storage(Storage)->
  supervisor:start_child(?SUPERVISOR,[Storage]).

stop_storage(Storage)->
  case whereis(Storage) of
    PID when is_pid(PID)->
      supervisor:terminate_child(?SUPERVISOR,PID);
    _ ->
      { error, not_started }
  end.

%%=================================================================
%%	OTP
%%=================================================================
start_supervisor() ->
  supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, [ supervisor ]).

start_server() ->
  gen_server:start_link({local,?SERVER},?MODULE, [ server ],[]).

init([ supervisor ]) ->

  ?LOGINFO("starting storage supervisor ~p",[self()]),

  ChildConfig = #{
    id => dlss_storage_srv,
    start => { dlss_storage_srv, start_link, [] },
    restart => permanent,
    shutdown => ?ENV(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=> worker,
    modules=>[ dlss_storage_srv ]
  },

  Supervisor = #{
    strategy => simple_one_for_one,
    intensity => ?ENV(max_restarts, ?DEFAULT_MAX_RESTARTS),
    period => ?ENV(max_period, ?DEFAULT_STOP_TIMEOUT)
  },

  {ok, { Supervisor, [ ChildConfig ]}};

init([ server ]) ->
  ?LOGINFO("starting storage supervisor server ~p",[self()]),

  % Enter the loop
  self()!loop,

  {ok,#state{ storages = []}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("storage supervisor server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.


handle_cast(Request,State)->
  ?LOGWARNING("storage supervisor server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(loop,#state{ storages = Started0}=State)->

  % Keep the loop
  {ok,_}=timer:send_after(?CYCLE,loop),

  % Check open files limit
  check_ulimit_n(),

  % Scanning procedure
  Started=
    try sync_schema(Started0)
    catch
      _:Error:Stack->
        ?LOGERROR("storage supervisor server error ~p, stack ~p",[Error,Stack]),
        Started0
    end,

  {noreply,State#state{storages = Started}}.

terminate(Reason,_State)->
  ?LOGINFO("terminating storage supervisor server reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

check_ulimit_n()->
  case get_ulimit_n() of
    Limit when is_integer(Limit)->
      if
        Limit < ?MIN_ULIMIT_N->
          ?LOGWARNING("The Open Files Limit is lower than recommended, please set it with 'ulimit -n ~p' before starting the application",[
            ?MIN_ULIMIT_N
          ]);
        true ->
          ok
      end;
    _->
      % Is it an unsupported OS type? What to do?
      ignore
  end.

get_ulimit_n()->
  case os:type() of
    {unix,_Any}->
      Limit0 = os:cmd("ulimit -n"),
      case re:run(Limit0,"^(?<Limit>\\d+).*",[{capture,['Limit'],list}]) of
        {match,[Limit2]}->
          list_to_integer(Limit2);
        _->
          ?LOGWARNING("unable to find out the open files limit settings"),
          undefined
      end;
    _->
      undefined
  end.

%%============================================================================
%%	The loop
%%============================================================================
sync_schema( Started )->
  ?LOGDEBUG("start storage schema synchronization"),

  % The service checks monitors the schema for configured storages
  Storages = dlss_storage:get_storages(),

  % Start new storages
  [ start_storage( S ) || S <- Storages -- Started ],

  % Stop no longer supervised storages
  [ stop_storage(S) || S <- Started -- Storages ],

  Storages.
