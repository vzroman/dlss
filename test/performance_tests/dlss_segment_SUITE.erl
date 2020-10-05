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

-define(DEFAULT_COUNT,1000000).

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

%-----Performance-----------
-export([
  test_leveldb_write/1,
  test_leveldb_read/1,
  test_leveldb_scan/1,
  test_leveldb_select/1,
  test_leveldb_delete/1,

  test_dets_write/1,
  test_dets_read/1,
  test_dets_delete/1
]).

all()->
  [
    {group,leveldb}
    ,{group,dets}
  ].

groups()->
  [{leveldb,
    [sequence],
    [
      test_leveldb_write,
      test_leveldb_read,
      test_leveldb_select,
      test_leveldb_scan,
      test_leveldb_delete
    ]
  },{dets,
    [sequence],
    [
      test_dets_write,
      test_dets_read,
      test_dets_delete
    ]
  }].

%% Init system storages
init_per_suite(Config)->
  dlss_backend:init_backend(),
  Count=?ENV("COUNT",count,?DEFAULT_COUNT),
  Count1=
    if
      is_list(Count) -> list_to_integer(Count);
      true -> Count
    end,
  [
    {count,Count1}
    |Config
  ].
end_per_suite(_Config)->
  dlss_backend:stop(),
  ok.

init_per_group(leveldb,Config)->
  dlss:add_storage(performance_test,disc),
  [Segment]=dlss:get_segments(performance_test),
  [
    {storage,performance_test},
    {segment,Segment}
    |Config
  ];
init_per_group(dets,Config)->
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

end_per_group(leveldb,Config)->
  Storage=?GET(storage,Config),
  dlss:remove_storage(Storage),
  ok;
end_per_group(dets,Config)->
  Segment=?GET(dets,Config),
  {atomic,ok}=mnesia:delete_table(Segment),
  ok;
end_per_group(_,_Config)->
  ok.


init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

test_leveldb_write(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    ok=dlss_segment:dirty_write(Segment,{I,2},{value,I})
  end,0,Count),

  ok.

test_leveldb_read(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
  end,0,Count),

  ok.

test_leveldb_select(Config)->
  Segment=?GET(segment,Config),
  Count=?GET(count,Config),
  Step=Count div 100,
  From=Step*40,
  To=Step*85,
  Total=To-From,
  Result=mnesia:dirty_select(Segment,[{#kv{key='$1',value='$2'},[{'>','$1',{{From,2}}},{'=<','$1',{{To,2}}}],[{{'$1','$2'}}]}]),
  Total=length(Result),
  ok.


test_leveldb_scan(Config)->
  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  Step=Count div 100,
  From=Step*40,
  To=Step*85,
  Total=To-From,
  Result=dlss_segment:dirty_scan(Segment,{From,2},{To,2}),
  Total=length(Result),

  ok.

test_leveldb_delete(Config)->

  Segment=?GET(segment,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
  end,0,Count),

  ok.

test_dets_write(Config)->

  Segment=?GET(dets,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    ok=dlss_segment:dirty_write(Segment,{I,2},{value,I})
  end,0,Count),

  ok.

test_dets_read(Config)->

  Segment=?GET(dets,Config),
  Count=?GET(count,Config),

  for(fun(I)->
    {value,I}=dlss_segment:dirty_read(Segment,{I,2})
      end,0,Count),

  ok.

test_dets_delete(Config)->

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
