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
-module(dlss_segment_SUITE).

-include("dlss_test.hrl").
-include("dlss.hrl").

%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  add_remove_node/1
]).


all()->
  [
    add_remove_node
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  start_nodes(['srv1@127.0.0.1']),
  Config.
end_per_suite(_Config)->
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.


init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

add_remove_node(_Config)->

  ok.

%%==================================================================
%%  Internal helpers
%%==================================================================
start_nodes(Nodes)->
  start_nodes(Nodes,_Acc = []).
start_nodes([],Acc) -> Acc;
start_nodes([N|Rest],Acc) ->

  ct:pal("cwd ~p",[file:get_cwd()]),
  VM =""
    ++" -mnesia dir 'DB"++atom_to_list(N)++"'"
    ++" -pa ../../../default/lib/*/ebin"
    ++ " -config ../../../test/distributed_tests/node.config",

  {ok, Node} = ct_slave:start(N,[
    {username,"roman"},
    {password,"system"},
    {kill_if_fail, true},
    {monitor_master, true},
    {init_timeout, 3000},
    {startup_timeout, 3000},
    {startup_functions, [{dlss_backend, init_backend, [ #{as_node => true} ]}]},
    {erl_flags, VM}
  ]),
  ct:pal("~p started", [Node]),
  start_nodes(Rest,[Node|Acc]).

% VM="-setcookie dlss -mnesia dir DB_node  -pa _build/default/lib/*/ebin".
% ct_slave:start('srv1.127.0.0.1',[{username,"roman"},{password,"system"},{erl_flags, VM}])





