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

-module(dlss_schema_scanner).

-include("dlss.hrl").

-behaviour(gen_server).

-define(MIN_ULIMIT_N, 200000).

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

-define(DEFAULT_SCAN_CYCLE,5000).

-record(state,{ cycle, segments }).

%%=================================================================
%%	API
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE}, ?MODULE,[], []).

%%=================================================================
%%	OTP
%%=================================================================
init([])->

  ?LOGINFO("starting schema scanner ~p",[self()]),

  Cycle=?ENV(supervisor_scan_cycle, ?DEFAULT_SCAN_CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{
    cycle = Cycle,
    segments = []
  }}.

handle_call(Request, From, State) ->
  ?LOGWARNING("schema scanner got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.


handle_cast(Request,State)->
  ?LOGWARNING("schema scanner got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(loop,#state{
  cycle = Cycle,
  segments = Started
}=State)->

  % Keep the loop
  {ok,_}=timer:send_after(Cycle,loop),

  % Check open files limit
  check_ulimit_n(),

  % Scanning procedure
  Started1=
    try scan_storages(Started)
    catch
        _:Error:Stack->
          ?LOGERROR("schema scanner error ~p, stack ~p",[Error,Stack]),
          Started
    end,

  {noreply,State#state{segments = Started1}}.

terminate(Reason,_State)->
  ?LOGINFO("terminating schema scanner reason ~p",[Reason]),
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
scan_storages(Started)->
  ?LOGDEBUG("start schema scanning"),

  % The service checks monitors the schema for configured storages
  Storages = dlss_storage:get_storages(),

  % Start new segments
  [ dlss_storage_supervisor:start(S) || S <- Storages -- Started ],

  % Stop no longer supervised segments
  [ dlss_storage_supervisor:stop(S) || S <- Started -- Storages ],

  Storages.

