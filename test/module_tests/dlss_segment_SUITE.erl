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
  test_order/1
]).

%-----Performance-----------
-export([
  test_performance_disc_write/1,
  test_performance_disc_read/1,
  test_performance_disc_scan/1,
  test_performance_disc_select/1,
  test_performance_disc_delete/1,

  test_performance_dets_write/1,
  test_performance_dets_read/1,
  test_performance_dets_delete/1
]).

all()->
  [
    test_order
    ,{group,performance_leveldb}
    ,{group,performance_dets}
  ].

groups()->
  [{performance_leveldb,
    [sequence],
    [
      test_performance_disc_write,
      test_performance_disc_read,
      test_performance_disc_select,
      test_performance_disc_scan,
      test_performance_disc_delete
    ]
  },{performance_dets,
    [sequence],
    [
      test_performance_dets_write,
      test_performance_dets_read,
      test_performance_dets_delete
    ]
  }].

%% Init system storages
init_per_suite(Config)->
  dlss_backend:init_backend(),
  dlss:add_storage(order_test,disc),
  Count=1000000,
  [Segment]=dlss:get_segments(order_test),
  [
    {storage,order_test},
    {segment,Segment},
    {count,Count}
    |Config
  ].
end_per_suite(Config)->
  Storage=?GET(storage,Config),
  dlss:remove_storage(Storage),
  dlss_backend:stop(),
  ok.


init_per_group(performance_dets,Config)->
  {atomic,ok}= mnesia:create_table(test_dets,[
    {attributes,record_info(fields,kv)},
    {record_name,kv},
    {type,set},
    {disc_only_copies, [node()]}
  ]),
  [
    {dets,test_dets}
    |Config
  ];

init_per_group(_,Config)->
  Config.

end_per_group(performance,Config)->
  Segment=?GET(dets,Config),
  {atomic,ok}=mnesia:delete_table(Segment),
  ok;
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
  not_found=dlss_segment:dirty_read(Segment,{1,2}),

  ok.

%%------------------------------------------------------------------
%% Performance tests
%%------------------------------------------------------------------
test_performance_disc_write(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    ok=dlss_segment:dirty_write(Segment,{I,2},{value,I})
  end,0,Count),

  ok.

test_performance_disc_read(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
  end,0,Count),

  ok.

test_performance_disc_select(Config)->
  Segment=?GET(segment,Config),
  Count=?GET(count,Config),
  Step=Count div 100,
  From=Step*40,
  To=Step*85,
  Total=To-From,
  Result=mnesia:dirty_select(Segment,[{#kv{key='$1',value='$2'},[{'>=','$1',{{From+1,2}}},{'=<','$1',{{To,2}}}],[{{'$1','$2'}}]}]),
  Total=length(Result),
  ok.


test_performance_disc_scan(Config)->
  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  Step=Count div 100,
  From=Step*40,
  To=Step*85,
  Total=To-From,
  Result=dlss_segment:dirty_scan(Segment,{From+1,2},{To,2}),
  Total=length(Result),

  ok.

test_performance_disc_delete(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
  end,0,Count),

  ok.

test_performance_dets_write(Config)->

  Segment=?GET(dets,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    ok=dlss_segment:dirty_write(Segment,{I,2},{value,I})
  end,0,Count),

  ok.

test_performance_dets_read(Config)->

  Segment=?GET(dets,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
  end,0,Count),

  ok.

test_performance_dets_delete(Config)->

  Segment=?GET(dets,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
  end,0,Count),

  ok.

for(_F,From,To) when From>=To->
  ok;
for(F,From,To)->
  F(From),
  for(F,From+1,To).

