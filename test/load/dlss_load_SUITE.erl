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
-module(dlss_load_SUITE).

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
  add_data_disc/1,
  add_data_ram/1
]).


all()->
  [
    add_data_disc,
    add_data_ram
  ].

groups()-> [].

%% Init system storages
init_per_suite(Config)->
  dlss_backend:init_backend(),
  ok=dlss_storage:add(storage_disc,disc),
  ok=dlss_storage:add(storage_ram,ram),
  Config.
end_per_suite(_Config)->
  dlss_storage:remove(storage_disc),
  dlss_storage:remove(storage_ram),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),
  dlss_backend:stop(),
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.


init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

add_data_disc(_Config)->

  disc=dlss_storage:get_type(storage_disc),
  Cnt = 1414,

  [ ok = dlss:dirty_write(storage_disc, {x, V}, {y, binary:copy(<<"1">>, V)}) || V <- lists:seq(1, Cnt) ],
  [ begin
    W = binary:copy(<<"1">>, V),
    {y, W} = dlss:dirty_read(storage_disc, {x, V})
    end  || V <- lists:seq(1, Cnt) ],
  ok.



add_data_ram(_Config)->

  ram=dlss_storage:get_type(storage_ram),
  Cnt = 632,

  [ ok = dlss:dirty_write(storage_ram, {x, V}, {y, binary:copy(<<"1">>, V)}) || V <- lists:seq(1, Cnt) ],
  [ begin
      W = binary:copy(<<"1">>, V),
      {y, W} = dlss:dirty_read(storage_ram, {x, V})
    end  || V <- lists:seq(1, Cnt) ],

  ok.

