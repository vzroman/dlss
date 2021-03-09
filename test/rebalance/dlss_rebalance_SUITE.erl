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
  schema_common/1,
  split_segment/1,
  absorb_parent/1
]).

-define(TIMER(T),{(T) div 60000, (T) rem 60000}).

all()->
  [
    schema_common
    %,split_segment
    %,absorb_parent
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
schema_common(_Config)->

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
  ok=dlss_storage:add(schema_common,disc),

  % Check for storage Root segment
  [dlss_schema_common_1]=dlss_storage:get_segments(schema_common),
  {ok, #{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_1),

  % fill the storage with records (~1 GB)
  T0 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_schema_common_1)/?MB]),
  Count0 = 20000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(schema_common, {x, V}, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count0) ],
  Size0 = dlss_segment:get_size(dlss_schema_common_1),
  T1 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB, time ~p",[Size0/?MB, ?TIMER(T1-T0)]),

  % Run supervisor
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % new root should appear, former takes level 1

  [dlss_schema_common_2,dlss_schema_common_1]=dlss_storage:get_segments(schema_common),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_1),
  [ dlss_schema_common_1 ] = dlss_storage:get_children(dlss_schema_common_2),
  [] = dlss_storage:get_children(dlss_schema_common_1),

  % Run supervisor loop
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % dlss_schema_common_1 should be queued to split because it weights more than the limit
  [
    dlss_schema_common_2,
    dlss_schema_common_1,dlss_schema_common_3
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 1.1, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),

  % Run supervisor loop
  T2 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % the splitting is performed and committed, dlss_schema_common_3 is at level 1
  T3 = erlang:system_time(millisecond),
  [
    dlss_schema_common_2,
    dlss_schema_common_3,dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_schema_common_1),
  ?LOGINFO("finish splitting ~p, time ~p, split key ~p",[dlss_schema_common_1, ?TIMER(T3-T2), SplitKey0]),

  % Run supervisor loop
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % The schema is balanced now
  [
    dlss_schema_common_2,
    dlss_schema_common_3,dlss_schema_common_1,
    dlss_schema_common_4 % it is to split dlss_common_1
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 1.1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_schema_common_4),

  ?LOGDEBUG("BEFORE: dlss_schema_common_4 first ~p, dlss_schema_common_1 first ~p",[
    dlss_segment:dirty_first(dlss_schema_common_4),
    dlss_segment:dirty_first(dlss_schema_common_1)
  ]),

  % Run supervisor loop
  T4 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % It is going to split dlss_schema_common_1 to dlss_schema_common_4
  T5 = erlang:system_time(millisecond),

  ?LOGDEBUG("AFTER: dlss_schema_common_4 first ~p",[dlss_segment:dirty_first(dlss_schema_common_4)]),
  [
    dlss_schema_common_2,
    dlss_schema_common_3,dlss_schema_common_4,dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_schema_common_1),

  ?LOGINFO("finish splitting ~p, time ~p, split key ~p",[dlss_schema_common_1, ?TIMER(T5-T4), SplitKey1]),

  % Run supervisor loop
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % It is going to purge in dlss_schema_common_1 keys that were moved to dlss_schema_common_4
  % then it is going to detect that level 1 has reached its limit and to move
  % dlss_schema_common_3 to the level 2
  [
    dlss_schema_common_2,
    dlss_schema_common_3, % level 2 but the key is smaller than in level 1 segments
    dlss_schema_common_4,dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 2, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),

  % Run supervisor loop
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % No limits are broken - no changes
  [
    dlss_schema_common_2,
    dlss_schema_common_3, % level 2 but the key is smaller than in level 1 segments
    dlss_schema_common_4,dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := SplitKey0 }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 2, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),

  % add 1 GB more records
  T6 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_schema_common_2)/?MB]),
  Count1 = 20000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(schema_common, {x, V * 2 }, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count1) ],
  Size1 = dlss_segment:get_size(dlss_schema_common_2),
  T7 = erlang:system_time(millisecond),
  ct:pal("root size ~p MB, time ~p",[Size1/?MB, ?TIMER(T7-T6)]),

  % Run supervisor
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % dlss_schema_common_2 is full, a new root is created and dlss_schema_common_2 is enqueued to merge with level 1 segments
  [
    dlss_schema_common_5,
    dlss_schema_common_3, % level 2 but the key {x,1} is the smallest
    dlss_schema_common_2, % former root
    dlss_schema_common_4,dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_5),
  {ok,#{ level := 0.9, key := {x,2} }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := SplitKey0, version:=L1Version }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := SplitKey1, version:=L1Version }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 2, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),

  % Run supervisor
  T8 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % first dlss_schema_common_4 takes its keys from dlss_schema_common_2
  T9 = erlang:system_time(millisecond),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_5),
  {ok,#{ level := 0.9, key := {x,2} }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,2} }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 2, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),

  ?LOGINFO("splitting ~p to ~p, time ~p, size ~p",[
    dlss_schema_common_2,
    dlss_schema_common_4,
    ?TIMER(T9-T8),
    dlss_segment:get_size(dlss_schema_common_4) / ?MB
  ]),

  % Run supervisor
  T10 = erlang:system_time(millisecond),
  dlss_storage_supervisor:loop( schema_common, disc, node() ),
  % first dlss_schema_common_4 takes its keys from dlss_schema_common_2
  T11 = erlang:system_time(millisecond),

  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_5),
  {ok,#{ level := 0.9, key := {x,2} }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := {x,2} }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := SplitKey1 }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok, #{ level := 2, key := {x,1} }} = dlss_storage:segment_params(dlss_schema_common_3),

  ?LOGINFO("splitting ~p to ~p, time ~p, size ~p",[
    dlss_schema_common_2,
    dlss_schema_common_1,
    ?TIMER(T11-T10),
    dlss_segment:get_size(dlss_schema_common_1) / ?MB
  ]),

%%
%%  % Spawn a new segment for storage
%%  ok = dlss_storage:spawn_segment(dlss_schema_common_1),
%%  [dlss_schema_common_2,dlss_schema_common_1,dlss_schema_common_3]=dlss_storage:get_segments(schema_common),
%%  [{_,dlss_schema_common_1},{_,dlss_schema_common_3}] = dlss_storage:get_children(dlss_schema_common_2),
%%  [{_,dlss_schema_common_3}] = dlss_storage:get_children(dlss_schema_common_1),
%%  [] = dlss_storage:get_children(dlss_schema_common_3),
%%  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
%%  {ok,#{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_1),
%%  {ok,#{ level := 2, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),
%%
%%  SizeInit = dlss_segment:get_size(dlss_schema_common_3),
%%  Half = Size0 /2,
%%  ct:pal("split dlss_schema_common_1 to dlss_schema_common_3, initial size ~p, to size ~p",[
%%    SizeInit/?MB,
%%    Half/?MB
%%  ]),
%%
%%  % Normally when the first root is going level down it is to be split, because there are
%%  % no children to absorb it yet
%%  dlss_segment:split(dlss_schema_common_1, dlss_schema_common_3, '$start_of_table', Half , 0),
%%  T2 = erlang:system_time(millisecond),
%%  SizeSplit = dlss_segment:get_size(dlss_schema_common_3),
%%  ct:pal("dlss_schema_common_3 size after split ~p, dlss_schema_common_1 size ~p",[
%%    SizeSplit/?MB,
%%    dlss_segment:get_size(dlss_schema_common_1)/?MB   % If there are not many records it is not going to decrease,
%%                                                      % because leveldb doesn't do true delete, only during rebalancing
%%                                                      % and the rebalancing may not has occurred yet
%%  ]),
%%
%%  % After a half of records are in the child segment, the child is going ta take place
%%  % next to the parent on the parent's level
%%  ?LOGINFO("finish splitting ~p, time ~p",[dlss_schema_common_1, ?TIMER(T2-T1)]),
%%  dlss_storage:level_up( dlss_schema_common_3 ),
%%
%%  % check the schema after splitting, notice that the dlss_schema_common_3 follows before dlss_schema_common_1 now
%%  [dlss_schema_common_2,dlss_schema_common_3,dlss_schema_common_1]=dlss_storage:get_segments(schema_common),
%%  [{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_2),
%%  [] = dlss_storage:get_children(dlss_schema_common_1),
%%  [] = dlss_storage:get_children(dlss_schema_common_3),
%%  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
%%  {ok,#{ level := 1, key := SplitKey }} = dlss_storage:segment_params(dlss_schema_common_1),
%%  {ok,#{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),
%%
%%  ?LOGINFO("the split key is ~p",[SplitKey]),
%%
%%  % add another portion of records to the storage
%%  % fill the storage with records (~115 MB)
%%  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_schema_common_2)/?MB]),
%%  Count1 = 20000000,
%%  [ begin
%%      if
%%        V rem 100000 =:=0 ->
%%          ct:pal("write ~p",[V]);
%%        true ->ok
%%      end,
%%      ok = dlss:dirty_write(schema_common, {x, V+Count0}, {y, <<"new",(binary:copy(integer_to_binary(V), 100))/binary>>})
%%    end || V <- lists:seq(1, Count1) ],
%%  T3 = erlang:system_time(millisecond),
%%  Size1 = dlss_segment:get_size(dlss_schema_common_2),
%%  ct:pal("root size ~p MB, time ~p",[Size1/?MB,?TIMER(T3-T2)]),
%%
%%  % The root is full, create another one
%%  ok = dlss_storage:new_root_segment(schema_common),
%%  [
%%    dlss_schema_common_4,
%%    dlss_schema_common_2,
%%    dlss_schema_common_3,
%%    dlss_schema_common_1
%%  ]=dlss_storage:get_segments(schema_common),
%%
%%  [{_,dlss_schema_common_2},{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_4),
%%  [{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_2),
%%  [] = dlss_storage:get_children(dlss_schema_common_3),
%%  [] = dlss_storage:get_children(dlss_schema_common_1),
%%  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_4),
%%  {ok, #{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
%%  {ok,#{ level := 2, key := SplitKey }} = dlss_storage:segment_params(dlss_schema_common_1),
%%  {ok,#{ level := 2, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),
%%
%%  % Level 2 segments are to absorb their parent dlss_schema_common_2.
%%  % They do it by order, first absorbs its keys the segment with the smallest key,
%%  % then next and so on
%%  ?LOGINFO("start absorbing dlss_schema_common_2, initail size ~p",[ dlss_segment:get_size(dlss_schema_common_2)/?MB ]),
%%  ?LOGINFO("absorb to dlss_schema_common_3, initail size ~p",[ dlss_segment:get_size(dlss_schema_common_3)/?MB ]),
%%  dlss_storage:absorb_parent( dlss_schema_common_3 ),
%%  T4 = erlang:system_time(millisecond),
%%  ?LOGINFO("dlss_schema_common_3 has absorbed its keys, dlss_schema_common_2 size ~p, dlss_schema_common_3 size ~p, time ~p",[
%%    dlss_segment:get_size(dlss_schema_common_2)/?MB,
%%    dlss_segment:get_size(dlss_schema_common_3)/?MB,
%%    ?TIMER(T4-T3)
%%  ]),
%%
%%  % The parent still contains key belonging to the dlss_schema_common_1
%%  false = dlss_segment:is_empty( dlss_schema_common_2 ),
%%
%%  ?LOGINFO("absorb to dlss_schema_common_1, initail size ~p",[ dlss_segment:get_size(dlss_schema_common_1)/?MB ]),
%%  dlss_storage:absorb_parent( dlss_schema_common_1 ),
%%  T5 = erlang:system_time(millisecond),
%%  ?LOGINFO("dlss_schema_common_1 has absorbed its keys, dlss_schema_common_2 size ~p, dlss_schema_common_1 size ~p, time ~p",[
%%    dlss_segment:get_size(dlss_schema_common_2)/?MB,
%%    dlss_segment:get_size(dlss_schema_common_1)/?MB,
%%    ?TIMER(T5-T4)
%%  ]),
%%
%%  % Now the parent must be empty
%%  true = dlss_segment:is_empty( dlss_schema_common_2 ),
%%
%%  ?LOGINFO("removing dlss_schema_common_2"),
%%  dlss_storage:absorb_segment( dlss_schema_common_2 ),
%%
%%  % Check the schema
%%  [
%%    dlss_schema_common_4,
%%    dlss_schema_common_3,
%%    dlss_schema_common_1
%%  ]=dlss_storage:get_segments(schema_common),
%%
%%  [{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_4),
%%  [] = dlss_storage:get_children(dlss_schema_common_3),
%%  [] = dlss_storage:get_children(dlss_schema_common_1),
%%  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_4),
%%  {ok,#{ level := 1, key := SplitKey }} = dlss_storage:segment_params(dlss_schema_common_1),
%%  {ok,#{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),

  ok.

split_segment(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  ct:timetrap(24*3600*1000),

  % Fill the root
  Count = 20000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(storage1, {x, V}, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count) ],

  %------------------------------------------------------
  % The root is full
  %------------------------------------------------------
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % The former root has no children yet, but its full.
  % It wants to split
  ok = dlss_storage:spawn_segment( dlss_storage1_1 ),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  false = dlss_storage:has_siblings( dlss_storage1_3 ),
  {ok, #{ level := 1 } } = dlss_storage:segment_params( dlss_storage1_1 ),
  {ok, #{ level := 2 } }= dlss_storage:segment_params( dlss_storage1_3 ),

  Size = round (dlss_segment:get_size( dlss_storage1_1 ) ),
  Half = Size / 2,
  ct:pal("Root size ~p, half ~p",[ Size/?MB, Half/?MB ]),
  dlss_segment:split(dlss_storage1_1, dlss_storage1_3, '$start_of_table', Half , 0),

  % for the disc based storage types the root size may not decrease or even
  % increase because leveldb does not actually delete key, just marks them with
  % 'delete'
  ct:pal("Root size ~p, child size ~p",[
    round (dlss_segment:get_size( dlss_storage1_1 ) /?MB ),
    round (dlss_segment:get_size( dlss_storage1_3 )/?MB )
  ]),

  false = dlss_segment:is_empty( dlss_storage1_3 ),



%%  { error, { total_size, Size } } = dlss_segment:get_split_key( dlss_storage1_1, 4096*1024*1024 ),
%%  ct:pal("Root size ~p",[Size]),
%%
%%  {ok,Median} = dlss_segment:get_split_key( dlss_storage1_1, Size div 2 ),
%%  ct:pal("root median ~p",[Median]),
%%  dlss_storage:spawn_segment(dlss_storage1_1,Median),
%%  dlss_storage:spawn_segment(dlss_storage1_1),
%%  [
%%    dlss_storage1_2,
%%    dlss_storage1_1,
%%    dlss_storage1_4,dlss_storage1_3
%%  ]=dlss_storage:get_segments(storage1),
%%
%%  [begin
%%     {value,N} = dlss_storage:dirty_read(storage1,{x,N})
%%   end || N <- lists:seq(1,Count)],
%%
%%  ok = dlss_storage:hog_parent(dlss_storage1_4),
%%  { error, { total_size, LeftSize } } = dlss_segment:get_split_key( dlss_storage1_4, 4096*1024*1024 ),
%%  ct:pal("left size ~p",[LeftSize]),
%%
%%  [begin
%%     {value,N} = dlss_storage:dirty_read(storage1,{x,N})
%%   end || N <- lists:seq(1,Count)],
%%
%%  ok = dlss_storage:hog_parent(dlss_storage1_3),
%%  { error, { total_size, RightSize } } = dlss_segment:get_split_key( dlss_storage1_3, 4096*1024*1024 ),
%%  ct:pal("right size ~p",[RightSize]),
%%
%%  [begin
%%     {value,N} = dlss_storage:dirty_read(storage1,{x,N})
%%   end || N <- lists:seq(1,Count)],
%%
%%  { error, { total_size, 0 } } = dlss_segment:get_split_key( dlss_storage1_1, 4096*1024*1024 ),
%%  dlss_storage:absorb_segment(dlss_storage1_1),
%%
%%  [
%%    dlss_storage1_2,
%%    dlss_storage1_4,dlss_storage1_3
%%  ]=dlss_storage:get_segments(storage1),
%%
%%  [begin
%%     {value,N} = dlss_storage:dirty_read(storage1,{x,N})
%%   end || N <- lists:seq(1,Count)],

  % Clean up
%%  dlss_storage:remove(storage1),
%%  []=dlss_storage:get_storages(),
%%  []=dlss_storage:get_segments(),

  ok.

absorb_parent(_Config)->
  ok.

