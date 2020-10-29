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
  get_segments/0,get_segments/1,
  add_storage/2,add_storage/3,
  remove_storage/1
]).

-export([
  %-----Data API-------
  transaction/1,transaction_sync/1
]).

%%---------------------------------------------------------------
%%	SERVICE API
%%---------------------------------------------------------------
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

add_storage(Name,Type)->
  dlss_storage:add(Name,Type).
add_storage(Name,Type,Options)->
  dlss_storage:add(Name,Type,Options).

remove_storage(Name)->
  dlss_storage:remove(Name).

%%---------------------------------------------------------------
%%	DATA API
%%---------------------------------------------------------------
%-----------------------------------------------------------------
%	Wrap the procedure into the ACID transaction
%-----------------------------------------------------------------
transaction(Fun)->
  % We use the mnesia engine to deliver the true distributed ACID transactions
  case mnesia:transaction(Fun) of
    {atomic,FunResult}->{ok,FunResult};
    {aborted,Reason}->{error,Reason}
  end.

% Sync transaction wait all changes are applied
transaction_sync(Fun)->
  case mnesia:sync_transaction(Fun) of
    {atomic,FunResult}->{ok,FunResult};
    {aborted,Reason}->{error,Reason}
  end.


