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

-module(dlss).

%%=================================================================
%%	APPLICATION API
%%=================================================================
-export([
  %-----Service API-------
  get_storages/0,
  get_segments/0,get_segments/1
]).

%-----------------------------------------------------------------
%	Get list of all dlss storages
%-----------------------------------------------------------------
get_storages()->
  dlss_storage:get_storages().
%-----------------------------------------------------------------
%	Get list of all dlss segments
%-----------------------------------------------------------------
get_segments()->
  dlss_storage:get_segments().

%-----------------------------------------------------------------
%	Get list of dlss segments for the Storage
%-----------------------------------------------------------------
get_segments(Storage)->
  dlss_storage:get_segments(Storage).


