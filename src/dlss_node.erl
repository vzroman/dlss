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
      set_status( Node, down ),
      dlss_segment:add_node( Node ),
      ok;
    _->
      error
  end.

remove( Node )->
  dlss_storage:remove_node( Node ),
  dlss_segment:remove_node( Node ),
  dlss_backend:remove_node(Node),
  dlss_segment:dirty_delete(dlss_schema,#node{node=Node}),
  ok.

set_status(Node,Status)->
  dlss_segment:node_status(Node, Status),
  dlss_segment:dirty_write(dlss_schema, #node{node=Node},Status).

get_status(Node)->
  case dlss_segment:dirty_read(dlss_schema, #node{node=Node}) of
    not_found->{ error, invalid_node };
    Status -> Status
  end.

get_nodes()->
  MS=[{
    #kv{key = #node{node = '$1' }, value = '_' },
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

get_ready_nodes()->
  MS=[{
    #kv{key = #node{node = '$1' }, value = ready },
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).