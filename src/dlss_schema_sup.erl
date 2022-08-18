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

-module(dlss_schema_sup).

-include("dlss.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([
  on_init/0,
  init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

on_init()->
  spawn_link(fun()-> register(dlss_schema,self()), schema_updates() end).

schema_updates()->
  % At mean time we are not interested in live schema updates
  receive _-> schema_updates() end.

init([]) ->

  StorageSupervisor=#{
    id=>dlss_storage_sup,
    start=>{dlss_storage_sup,start_supervisor,[]},
    restart=>permanent,
    shutdown=>infinity,
    type=>supervisor,
    modules=>[dlss_storage_sup]
  },

  StorageSupervisorServer=#{
    id=>dlss_storage_sup_srv,
    start=>{dlss_storage_sup,start_server,[]},
    restart=>permanent,
    shutdown=>infinity,
    type=>worker,
    modules=>[dlss_storage_sup]
  },

  SegmentSupervisor=#{
    id=>dlss_segment_sup,
    start=>{dlss_segment_sup,start_supervisor,[]},
    restart=>permanent,
    shutdown=>infinity,
    type=>supervisor,
    modules=>[dlss_segment_sup]
  },

  SegmentSupervisorServer=#{
    id=>dlss_segment_sup_srv,
    start=>{dlss_segment_sup,start_server,[]},
    restart=>permanent,
    shutdown=>infinity,
    type=>worker,
    modules=>[dlss_segment_sup]
  },

  Supervisor=#{
    strategy=>one_for_one,
    intensity=>?ENV(max_restarts, ?DEFAULT_MAX_RESTARTS),
    period=>?ENV(max_period, ?DEFAULT_MAX_PERIOD)
  },

  {ok, {Supervisor, [
    StorageSupervisor,
    StorageSupervisorServer,
    SegmentSupervisor,
    SegmentSupervisorServer
  ]}}.


