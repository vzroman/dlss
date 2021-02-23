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

-define(MB,1048576).

all()->
  [
    schema_common
    %,split_segment
    ,absorb_parent
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

schema_common(_Config)->
  % new storage
  ok=dlss_storage:add(schema_common,disc),

  % Check for storage Root segment
  [dlss_schema_common_1]=dlss_storage:get_segments(schema_common),
  {ok, #{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_1),

  % fill the storage with records (~115 MB)
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_schema_common_1)/?MB]),
  Count = 2000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(schema_common, {x, binary:copy(integer_to_binary(V), 1)}, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count) ],
  Size0 = dlss_segment:get_size(dlss_schema_common_1),

  ct:pal("root size ~p MB",[Size0/?MB]),

  % Add a new Root segment for storage
  ok = dlss_storage:new_root_segment(schema_common),
  [dlss_schema_common_2,dlss_schema_common_1]=dlss_storage:get_segments(schema_common),
  [{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_2),
  [] = dlss_storage:get_children(dlss_schema_common_1),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok, #{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_1),

  % Spawn a new segment for storage
  ok = dlss_storage:spawn_segment(dlss_schema_common_1),
  [dlss_schema_common_2,dlss_schema_common_1,dlss_schema_common_3]=dlss_storage:get_segments(schema_common),
  [{_,dlss_schema_common_1},{_,dlss_schema_common_3}] = dlss_storage:get_children(dlss_schema_common_2),
  [{_,dlss_schema_common_3}] = dlss_storage:get_children(dlss_schema_common_1),
  [] = dlss_storage:get_children(dlss_schema_common_3),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok,#{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok,#{ level := 2, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),

  SizeInit = dlss_segment:get_size(dlss_schema_common_3),
  Half = Size0 /2,
  ct:pal("split dlss_schema_common_1 to dlss_schema_common_3, initial size ~p, to size ~p",[
    SizeInit/?MB,
    Half/?MB
  ]),

  % Normally when the first root is going level down it is to be split, because there are
  % no children to absorb it yet
  dlss_segment:split(dlss_schema_common_1, dlss_schema_common_3, '$start_of_table', Half , 0),
  SizeSplit = dlss_segment:get_size(dlss_schema_common_3),
  ct:pal("dlss_schema_common_3 size after split ~p, dlss_schema_common_1 size ~p",[
    SizeSplit/?MB,
    dlss_segment:get_size(dlss_schema_common_1)/?MB   % If there are not many records it is not going to decrease,
                                                      % because leveldb doesn't do true delete, only during rebalancing
                                                      % and the rebalancing may not has occurred yet
  ]),

  % After a half of records are in the child segment, the child is going ta take place
  % next to the parent on the parent's level
  ?LOGINFO("finish splitting ~p",[dlss_schema_common_1]),
  dlss_storage:level_up( dlss_schema_common_3 ),

  % check the schema after splitting, notice that the dlss_schema_common_3 follows before dlss_schema_common_1 now
  [dlss_schema_common_2,dlss_schema_common_3,dlss_schema_common_1]=dlss_storage:get_segments(schema_common),
  [{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_2),
  [] = dlss_storage:get_children(dlss_schema_common_1),
  [] = dlss_storage:get_children(dlss_schema_common_3),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok,#{ level := 1, key := SplitKey }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok,#{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),

  ?LOGINFO("the split key is ~p",[SplitKey]),

  % add another portion of records to the storage
  % fill the storage with records (~115 MB)
  ct:pal("root size ~p MB",[dlss_segment:get_size(dlss_schema_common_2)/?MB]),
  Count = 2000000,
  [ begin
      if
        V rem 100000 =:=0 ->
          ct:pal("write ~p",[V]);
        true ->ok
      end,
      ok = dlss:dirty_write(schema_common, {x, binary:copy(integer_to_binary(V), 2)}, {y, binary:copy(integer_to_binary(V), 100)})
    end || V <- lists:seq(1, Count) ],
  Size1 = dlss_segment:get_size(dlss_schema_common_2),
  ct:pal("root size ~p MB",[Size1/?MB]),

  % The root is full, create another one
  ok = dlss_storage:new_root_segment(schema_common),
  [
    dlss_schema_common_4,
    dlss_schema_common_2,
    dlss_schema_common_3,
    dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  [{_,dlss_schema_common_2},{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_4),
  [{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_2),
  [] = dlss_storage:get_children(dlss_schema_common_3),
  [] = dlss_storage:get_children(dlss_schema_common_1),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok, #{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_2),
  {ok,#{ level := 2, key := SplitKey }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok,#{ level := 2, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),

  % Level 2 segments are to absorb their parent dlss_schema_common_2.
  % They do it by order, first absorbs its keys the segment with the smallest key,
  % then next and so on
  ?LOGINFO("start absorbing dlss_schema_common_2, initail size ~p",[ dlss_segment:get_size(dlss_schema_common_2)/?MB ]),
  ?LOGINFO("absorb to dlss_schema_common_3, initail size ~p",[ dlss_segment:get_size(dlss_schema_common_3)/?MB ]),
  dlss_storage:absorb_parent( dlss_schema_common_3 ),
  ?LOGINFO("dlss_schema_common_3 has absorbed its keys, dlss_schema_common_2 size ~p, dlss_schema_common_3 size ~p",[
    dlss_segment:get_size(dlss_schema_common_2)/?MB,
    dlss_segment:get_size(dlss_schema_common_3)/?MB
  ]),

  % The parent still contains key belonging to the dlss_schema_common_1
  false = dlss_segment:is_empty( dlss_schema_common_2 ),

  ?LOGINFO("absorb to dlss_schema_common_1, initail size ~p",[ dlss_segment:get_size(dlss_schema_common_1)/?MB ]),
  dlss_storage:absorb_parent( dlss_schema_common_1 ),
  ?LOGINFO("dlss_schema_common_1 has absorbed its keys, dlss_schema_common_2 size ~p, dlss_schema_common_1 size ~p",[
    dlss_segment:get_size(dlss_schema_common_2)/?MB,
    dlss_segment:get_size(dlss_schema_common_1)/?MB
  ]),

  % Now the parent must be empty
  true = dlss_segment:is_empty( dlss_schema_common_2 ),

  ?LOGINFO("removing dlss_schema_common_2"),
  dlss_storage:absorb_segment( dlss_schema_common_2 ),

  % Check the schema
  [
    dlss_schema_common_4,
    dlss_schema_common_3,
    dlss_schema_common_1
  ]=dlss_storage:get_segments(schema_common),

  [{_,dlss_schema_common_3},{_,dlss_schema_common_1}] = dlss_storage:get_children(dlss_schema_common_4),
  [] = dlss_storage:get_children(dlss_schema_common_3),
  [] = dlss_storage:get_children(dlss_schema_common_1),
  {ok,#{ level := 0, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_4),
  {ok,#{ level := 1, key := SplitKey }} = dlss_storage:segment_params(dlss_schema_common_1),
  {ok,#{ level := 1, key := '_' }} = dlss_storage:segment_params(dlss_schema_common_3),

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
%%  [ begin
%%      W = binary:copy(<<"1">>, V),
%%      {y, W} = dlss:dirty_read(storage_disc, {x, V})
%%    end  || V <- lists:seq(1, Count) ],


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

