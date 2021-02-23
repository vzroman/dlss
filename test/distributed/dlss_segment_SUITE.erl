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

%%===================================================================
%% TODO. Distributed tests are not functional. The issues are:
%%  * the ct_slave.erl module has a bug at line 186.
%%    The
%%      spawn_remote_node(Host, Node, Options)
%%        should be replaced with
%%      spawn_remote_node(Host, ENode, Options)
%%  * The VM environment and args are not automatically provided. It is a quite chore
%%    to support and maintain it manually.
%%
%%--------------------------------------------------------------------
%%
%% The approach described at
%%  http://cabol.github.io/erlang/common-test/distribued-apps/2015/12/11/testing-distributed-apps-with-common-test/
%% also does not work in the rebar3 environment. To be able to run tests you also have to maintain the environment
%% and args for erlang VM
%%
%%--------------------------------------------------------------------
%%
%% At mean time I decided to leave the idea with distributed for better times. Currently to be able
%% to run a distributed test you should:
%%  * fix and recompile ct_slave.erl
%%  * prepare the multinode ready test specification (https://learnyousomeerlang.com/common-test-for-uncommon-tests)
%%  * compile the project with ./rebar3 compile (ct_master:run does not do it for you)
%%  * start the erlang VM with appropriate env and args (i.e. erl -name master@127.0.0.1 -pa _build/default/lib/*/ebin)
%%  * execute the tests with ct_master:run (i.e. ct_master:run("test/distributed_tests/test.spec"))

%%=======================================================================
%% DEBUG:
%%  * ./test_shell
%%  * erl -name slave@127.0.0.1 -setcookie dlss -pa _build/default/lib/*/ebin
%%  * slv: dlss_backend:init_backend(#{as_node=>true})
%%  * mst: dlss_backend:add_node('slave@127.0.0.1')
%%  * mst: dlss_storage:add(storage1,disc),
%%  * mst: dlss_storage:spawn_segment(dlss_storage1_1)
%%  * mst: dlss_storage:spawn_segment(dlss_storage1_1,{x,50})
%%  * mst: dlss_segment:add_node(dlss_storage1_3,'slave@127.0.0.1')
%%  * mst/slv: dlss_segment:get_info(dlss_storage1_3)
%%  -----remove---------------
%%  * slv: dlss_segment:remove_node(dlss_storage1_3,'slave@127.0.0.1')
%%  * mst/slv: dlss_segment:get_info(dlss_storage1_3)

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
  %start_nodes(['srv1@127.0.0.1']),
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



% ct_master:run("test/distributed_tests/test.spec")


%%==================================================================
%%  Internal helpers
%%==================================================================
%%start_nodes(Nodes)->
%%  start_nodes(Nodes,_Acc = []).
%%start_nodes([],Acc) -> Acc;
%%start_nodes([N|Rest],Acc) ->
%%
%%  ct:pal("cwd ~p",[file:get_cwd()]),
%%  VM =""
%%    ++" -mnesia dir 'DB"++atom_to_list(N)++"'"
%%    ++" -pa ../../../default/lib/*/ebin"
%%    ++ " -config ../../../test/distributed_tests/node.config",
%%
%%  {ok, Node} = ct_slave:start(N,[
%%    {username,"roman"},
%%    {password,"system"},
%%    {kill_if_fail, true},
%%    {monitor_master, true},
%%    {init_timeout, 3000},
%%    {startup_timeout, 3000},
%%    {startup_functions, [{dlss_backend, init_backend, [ #{as_node => true} ]}]},
%%    {erl_flags, VM}
%%  ]),
%%  ct:pal("~p started", [Node]),
%%  start_nodes(Rest,[Node|Acc]).

% VM="-mnesia dir DB_node  -pa _build/default/lib/*/ebin".
% ct_slave:start('srv1@127.0.0.1',[{username,"roman"},{password,"system"},{erl_flags, VM}])

% Cmd = "erl -detached -noinput -setcookie dlss -name srv1@127.0.0.1 -mnesia dir DB_node  -pa _build/default/lib/*/ebin"
% open_port({spawn, Cmd}, [stream,{env,[]}]).




