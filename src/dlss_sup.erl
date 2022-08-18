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

-module(dlss_sup).

-include("dlss.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->

  Backend=#{
    id=>dlss_backend,
    start=>{dlss_backend,start_link,[]},
    restart=>permanent,
    shutdown=>?ENV(stop_timeout, ?DEFAULT_STOP_TIMEOUT),
    type=>worker,
    modules=>[dlss_backend]
  },

  SchemaSupervisor=#{
    id=>dlss_schema_sup,
    start=>{dlss_schema_sup,start_link,[]},
    restart=>permanent,
    shutdown=>infinity,
    type=>supervisor,
    modules=>[dlss_schema_sup]
  },

  Supervisor=#{
    strategy=>one_for_one,
    intensity=>?ENV(max_restarts, ?DEFAULT_MAX_RESTARTS),
    period=>?ENV(max_period, ?DEFAULT_MAX_PERIOD)
  },

  {ok, {Supervisor, [
    Backend,
    SchemaSupervisor
  ]}}.


