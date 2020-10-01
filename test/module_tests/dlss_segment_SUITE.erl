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
  test_order/1
]).

%-----Performance-----------
-export([
  test_performance_disc_write/1,
  test_performance_disc_read/1,
  test_performance_disc_delete/1
]).

all()->
  [
    test_order,
    {group,performance}
  ].

groups()->
  [{performance,
    [sequence],
    [
      test_performance_disc_write,
      test_performance_disc_read,
      test_performance_disc_delete
    ]
  }].

%% Init system storages
init_per_suite(Config)->
  dlss_backend:init_backend(),
  dlss:add_storage(order_test,disc),
  [Segment]=dlss:get_segments(order_test),
  [
    {storage,order_test},
    {segment,Segment}
    |Config
  ].
end_per_suite(Config)->
  Storage=?GET(storage,Config),
  dlss:remove_storage(Storage),
  dlss_backend:stop(),
  ok.


init_per_group(performance,Config)->
  Count=lists:seq(1,100000),
  [{count,Count}|Config];

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.


init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

test_order(Config)->
  Segment=?GET(segment,Config),


  ok=dlss_segment:dirty_write(Segment,{1,2},{value,1}),
  {value,1}=dlss_segment:dirty_read(Segment,{1,2}),

  ok=dlss_segment:dirty_delete(Segment,{1,2}),
  undefined=dlss_segment:dirty_read(Segment,{1,2}),

  ok.

%%------------------------------------------------------------------
%% Performance tests
%%------------------------------------------------------------------
test_performance_disc_write(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  [ok=dlss_segment:dirty_write(Segment,{I,2},{value,I})||I<-Count],

  ok.

test_performance_disc_read(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  [{value,I}=dlss_segment:dirty_read(Segment,{I,2})||I<-Count],

  ok.

test_performance_disc_delete(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  [{value,I}=dlss_segment:dirty_read(Segment,{I,2})||I<-Count],

  ok.



