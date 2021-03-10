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
-module(dlss_rebalance_SUITE).

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
  disk_rebalance/1
]).

-define(TIMER(T),{(T) div 60000, (T) rem 60000}).

all()->
  [
    disk_rebalance
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  dlss_backend:init_backend(),
  Config.
end_per_suite(_Config)->
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

%%------------------------------------------------------------
%%  * add a storage
%%  * fill 1 GB
%%  * run supervisor loop -> new root should appear, former takes level 1
%%  * run supervisor loop -> former root should be queued to split
%%  * run supervisor loop -> the splitting is performed and committed, new segment is at the level 1
%%  * run supervisor loop -> no changes
%%  * + 1 GB
%%  * run supervisor loop -> new root appears, former takes level 0.9
%%  * run supervisor loop -> the former root is in the level 1 segments (it is removed)
%%  * run supervisor loop -> no changes (1 gb is between l1 segments)
%%  * + 1 GB
%%  * run supervisor loop -> new root appears, former takes level 0.9
%%  * run supervisor loop -> the former root is in the level 1 segments (it is removed)
%%  * run supervisor loop -> the first l1 segment is queued to split
%%  * run supervisor loop -> the first l1 segment is split and at the l1
%%  * run supervisor loop -> the second l1 segment is queued to split
%%  * run supervisor loop -> the second l1 segment is split and at the l1
%%  * run supervisor loop -> the new first segment from l1 goes to l2
%%  * run supervisor loop -> the new first segment is queued to merge to l2
%%  * run supervisor loop -> the merged segment is deleted
%%  * run supervisor loop -> the l2 segment is queued to split
%%  * run supervisor loop -> the l2 segment is split
%%  * run supervisor loop -> no changes
%%------------------------------------------------------------
disk_rebalance(_Config)->

  ct:timetrap(24*3600*1000),

  application:set_env([{ dlss, [
    {segment_level_limit,[
      { 0, 1024 },
      { 1, 1024 },
      { 2, 1024 }
    ]},
    {buffer_level_limit, 2 }
  ]}]),

  % new storage
  ok=dlss_storage:add(disk_rebalance,disc),

  % Check for storage Root segment
  [dlss_disk_rebalance_1]=dlss_storage:get_segments(disk_rebalance),
  {ok, #{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  % fill the storage with records (~1 GB)
  T0 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_disk_rebalance_1)/?MB]),
  Count0 = 20000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(disk_rebalance, {x, V}, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count0) ],
  Size0 = dlss_segment:get_size(dlss_disk_rebalance_1),
  T1 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB, time ~p",[Size0/?MB, ?TIMER(T1-T0)]),

  % Run supervisor
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % new root should appear, former takes level 1

  [dlss_disk_rebalance_2,dlss_disk_rebalance_1]=dlss_storage:get_segments(disk_rebalance),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  [ dlss_disk_rebalance_1 ] = dlss_storage:get_children(dlss_disk_rebalance_2),
  [] = dlss_storage:get_children(dlss_disk_rebalance_1),

  % Run supervisor loop
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % dlss_disk_rebalance_1 should be queued to split because it weights more than the limit
  [
    dlss_disk_rebalance_2,
    dlss_disk_rebalance_1,dlss_disk_rebalance_3
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  {ok, #{ level := 1.1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),

  % Run supervisor loop
  T2 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % the splitting is performed and committed, dlss_disk_rebalance_3 is at level 1
  T3 = erlang:system_time(millisecond),
  [
    dlss_disk_rebalance_2,
    dlss_disk_rebalance_3,dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  ?LOGINFO("finish splitting ~p, time ~p, split key ~p",[dlss_disk_rebalance_1, ?TIMER(T3-T2), SplitKey0]),

  % Run supervisor loop
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % The schema is balanced now, no changes
  [
    dlss_disk_rebalance_2,
    dlss_disk_rebalance_3,dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  % add 1 GB more records
  T4 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_disk_rebalance_2)/?MB]),
  Count1 = 20000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(disk_rebalance, {x, V * 2 }, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count1) ],
  Size1 = dlss_segment:get_size(dlss_disk_rebalance_2),
  T5 = erlang:system_time(millisecond),
  ?LOGDEBUG("root size ~p MB, time ~p",[Size1/?MB, ?TIMER(T5-T4)]),

  % Run supervisor
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % dlss_disk_rebalance_2 is full, a new root is created and dlss_disk_rebalance_2 is enqueued to merge with level 1 segments
  [
    dlss_disk_rebalance_4,
    dlss_disk_rebalance_3, % It is at a lower level than dlss_disk_rebalance_2 but its key is smaller, therefore it goes earlier
    dlss_disk_rebalance_2,
    dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_4),
  {ok,#{ level := 0.9, key := {x,2} }} = dlss_storage:segment_params(dlss_disk_rebalance_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  % Run supervisor
  T6 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % first dlss_disk_rebalance_3 takes its keys from dlss_disk_rebalance_2
  T7 = erlang:system_time(millisecond),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_4),
  {ok,#{ level := 0.9, key := {x,2} }} = dlss_storage:segment_params(dlss_disk_rebalance_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  ?LOGINFO("splitting ~p to ~p, time ~p, size ~p",[
    dlss_disk_rebalance_2,
    dlss_disk_rebalance_3,
    ?TIMER(T7-T6),
    dlss_segment:get_size(dlss_disk_rebalance_3) / ?MB
  ]),

  % Run supervisor
  T8 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % dlss_disk_rebalance_1 takes its keys from dlss_disk_rebalance_2
  T9 = erlang:system_time(millisecond),

  [
    dlss_disk_rebalance_4,
    % dlss_disk_rebalance_2,  this segment is merged to dlss_disk_rebalance_3 and dlss_disk_rebalance_1
    dlss_disk_rebalance_3,dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_4),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  ?LOGINFO("splitting ~p to ~p, time ~p, size ~p",[
    dlss_disk_rebalance_2,
    dlss_disk_rebalance_1,
    ?TIMER(T9-T8),
    dlss_segment:get_size(dlss_disk_rebalance_1) / ?MB
  ]),

  % add 1 GB more records
  T10 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_disk_rebalance_4)/?MB]),
  Count1 = 20000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(disk_rebalance, {x1, V }, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count1) ],
  Size2 = dlss_segment:get_size(dlss_disk_rebalance_4),
  T11 = erlang:system_time(millisecond),
  ?LOGDEBUG("root size ~p MB, time ~p",[Size2/?MB, ?TIMER(T11-T10)]),

  % Run supervisor
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % dlss_disk_rebalance_2 is full, a new root is created and dlss_disk_rebalance_2 is enqueued to merge with level 1 segments
  [
    dlss_disk_rebalance_5,
    dlss_disk_rebalance_3,   % They are at a lower level than dlss_disk_rebalance_4
    dlss_disk_rebalance_1,   % but their keys are smaller, therefore they go earlier
    dlss_disk_rebalance_4
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok,#{ level := 0.9, key := {x1,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_4),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  % Run supervisor
  T12 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % first dlss_disk_rebalance_3 takes its keys from dlss_disk_rebalance_4
  T13 = erlang:system_time(millisecond),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok,#{ level := 0.9, key := {x1,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_4),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  ?LOGINFO("merging ~p to ~p, time ~p, size ~p",[
    dlss_disk_rebalance_4,
    dlss_disk_rebalance_3,
    ?TIMER(T13-T12),
    dlss_segment:get_size(dlss_disk_rebalance_3) / ?MB
  ]),

  % Run supervisor
  T14 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % dlss_disk_rebalance_1 takes its keys from dlss_disk_rebalance_4
  T15 = erlang:system_time(millisecond),

  [
    dlss_disk_rebalance_5,
    % dlss_disk_rebalance_4,  this segment is merged to dlss_disk_rebalance_3 and dlss_disk_rebalance_1
    dlss_disk_rebalance_3,dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),

  ?LOGINFO("merging ~p to ~p, time ~p, size ~p",[
    dlss_disk_rebalance_4,
    dlss_disk_rebalance_1,
    ?TIMER(T15-T14),
    dlss_segment:get_size(dlss_disk_rebalance_1) / ?MB
  ]),

  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % dlss_disk_rebalance_1 has taken all the records from dlss_disk_rebalance_4,
  % it's now too heavy and is to be split
  [
    dlss_disk_rebalance_5,
    dlss_disk_rebalance_3,dlss_disk_rebalance_1,
    dlss_disk_rebalance_6
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  {ok, #{ level := 1.1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_6),

  % Run supervisor loop
  T16 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % the splitting is performed and committed, dlss_disk_rebalance_6 took his place at level 1
  T17 = erlang:system_time(millisecond),
  [
    dlss_disk_rebalance_5,
    dlss_disk_rebalance_3,dlss_disk_rebalance_6,dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_6),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  ?LOGINFO("finish splitting ~p, time ~p, split key ~p",[dlss_disk_rebalance_1, ?TIMER(T17-T16), SplitKey1]),

  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % The dlss_disk_rebalance_1 is still too heavy, split
  [
    dlss_disk_rebalance_5,
    dlss_disk_rebalance_3,dlss_disk_rebalance_6,dlss_disk_rebalance_1,dlss_disk_rebalance_7
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_6),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  {ok, #{ level := 1.1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_disk_rebalance_7),

  % Run supervisor loop
  T18 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % the splitting is performed and committed, dlss_disk_rebalance_6 took his place at level 1
  T19 = erlang:system_time(millisecond),
  [
    dlss_disk_rebalance_5,
    dlss_disk_rebalance_3,dlss_disk_rebalance_6,dlss_disk_rebalance_7,dlss_disk_rebalance_1
  ]=dlss_storage:get_segments(disk_rebalance),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_6),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_disk_rebalance_7),
  {ok, #{ level := 1, key := SplitKey2 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
  ?LOGINFO("finish splitting ~p, time ~p, split key ~p",[dlss_disk_rebalance_1, ?TIMER(T19-T18), SplitKey2]),

  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
  % Purge dlss_disk_rebalance_1



%%  dlss_storage_supervisor:loop( disk_rebalance, disc, node() ),
%%  % The level 1 is too heavy now, dlss_disk_rebalance_3 goes to the level 2
%%  % directly because there are no segments to merge with
%%  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_disk_rebalance_5),
%%  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_disk_rebalance_6),
%%  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_disk_rebalance_1),
%%  {ok, #{ level := 2, key := {x,1} }} = dlss_storage:segment_params(dlss_disk_rebalance_3),

  ok.


