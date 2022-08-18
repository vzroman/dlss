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

-module(dlss_segment_sup).

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
  start_segment/1,
  stop_segment/1
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
-record(state,{ segments }).

%%=================================================================
%%	API
%%=================================================================
start_segment(Segment)->
  supervisor:start_child(?SUPERVISOR,[Segment]).

stop_segment(Segment)->
  case whereis(Segment) of
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

  ?LOGINFO("starting segments supervisor ~p",[self()]),

  ChildConfig = #{
    id => dlss_segment_srv,
    start => { dlss_segment_srv, start_link, [] },
    restart => permanent,
    shutdown => ?ENV(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=> worker,
    modules=>[ dlss_segment_srv ]
  },

  Supervisor = #{
    strategy => simple_one_for_one,
    intensity => ?ENV(max_restarts, ?DEFAULT_MAX_RESTARTS),
    period => ?ENV(max_period, ?DEFAULT_STOP_TIMEOUT)
  },

  {ok, { Supervisor, [ ChildConfig ]}};

init([ server ]) ->
  ?LOGINFO("starting segments supervisor server ~p",[self()]),

  % Enter the loop
  Started = sync_schema([]),

  {ok,_}=timer:send_after(?CYCLE,loop),

  {ok,#state{ segments = Started}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("segments supervisor server got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.


handle_cast(Request,State)->
  ?LOGWARNING("segments supervisor server got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(loop,#state{ segments = Started0}=State)->

  % Keep the loop
  {ok,_}=timer:send_after(?CYCLE,loop),

  % Scanning procedure
  Started=
    try sync_schema(Started0)
    catch
      _:Error:Stack->
        ?LOGERROR("segments supervisor server error ~p, stack ~p",[Error,Stack]),
        Started0
    end,

  {noreply,State#state{segments = Started}}.

terminate(Reason,_State)->
  ?LOGINFO("terminating segments supervisor server reason ~p",[Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%============================================================================
%%	The loop
%%============================================================================
sync_schema( Started )->
  ?LOGDEBUG("start segments schema synchronization"),

  % The service checks monitors the schema for configured storages
  Segments = dlss_segment:get_local_segments(),

  % Start new storages
  [ start_segment( S ) || S <- Segments -- Started ],

  % Stop no longer supervised storages
  [ stop_segment(S) || S <- Started -- Segments ],

  Segments.
