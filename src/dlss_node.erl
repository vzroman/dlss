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

-module(dlss_node).

-include("dlss.hrl").
-include("dlss_schema.hrl").

-record(node,{node}).

%%=================================================================
%%	API
%%=================================================================
-export([
  add/1,
  remove/1,
  set_status/2,
  get_status/1,
  get_nodes/0,
  get_ready_nodes/0
]).

%%=================================================================
%%	API
%%=================================================================
add( Node )->
  case dlss_backend:add_node(Node) of
    true->
      set_status( Node, down ), %DEPRECATED

      ?N_ADD( Node ),
      ok;
    _->
      error
  end.

remove( Node )->
  dlss_storage:remove_node( Node ),
  ?N_REMOVE(Node),

  dlss_backend:remove_node(Node),
  dlss_segment:dirty_delete(dlss_schema,#node{node=Node}),  %DEPRECATED
  ok.

set_status(Node,Status)->
  if
    Status=:=ready-> ?N_UP( Node );
    Status=:=down-> ?N_DOWN( Node)
  end,
  dlss_segment:dirty_write(dlss_schema, #node{node=Node},Status). %DEPRECATED

get_status(Node)->
  case lists:member(Node,?READY_NODES ) of
    true->ready;
    _-> down
  end.

get_nodes()->
  ?NODES.

get_ready_nodes()->
  ?READY_NODES.