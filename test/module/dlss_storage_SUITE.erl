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
-module(dlss_storage_SUITE).

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
  service_api/1,
  segment_children/1,
  get_key_segments/1,
  storage_read/1,
  storage_next/1,
  storage_prev/1,
  storage_first/1,
  storage_last/1,
  storage_scan/1,
  storage_scan_boundaries/1,
  storage_scan_infinity/1,
  storage_scan_deleted/1
]).

-define(MB,1048576).

all()->
  [
    service_api
    ,segment_children
    ,get_key_segments
    ,storage_read
    ,storage_next
    ,storage_prev
    ,storage_first
    ,storage_last
%%    ,storage_scan
%%    ,storage_scan_boundaries
%%    ,storage_scan_infinity
%%    ,storage_scan_deleted
  ].

groups()->
  [ ].

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

service_api(_Config)->

  ok=dlss_storage:add(storage1,disc),
  disc=dlss_storage:get_type(storage1),
  [storage1]=dlss_storage:get_storages(),
  [dlss_storage1_1]=dlss_storage:get_segments(),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  ?assertError(already_exists,dlss_storage:add(storage1,disc)),
  ?assertError(already_exists,dlss_storage:add(storage1,ramdisc)),


  ok=dlss_storage:add(storage2,ramdisc),
  ramdisc=dlss_storage:get_type(storage2),
  [storage1,storage2]=dlss_storage:get_storages(),
  [dlss_storage1_1,dlss_storage2_1]=dlss_storage:get_segments(),
  [dlss_storage2_1]=dlss_storage:get_segments(storage2),

  ok=dlss_storage:add(storage3,ram),
  ram=dlss_storage:get_type(storage3),
  [storage1,storage2,storage3]=dlss_storage:get_storages(),
  [dlss_storage1_1,dlss_storage2_1,dlss_storage3_1]=dlss_storage:get_segments(),
  [dlss_storage3_1]=dlss_storage:get_segments(storage3),

  dlss_storage:remove(storage2),
  [storage1,storage3]=dlss_storage:get_storages(),
  [dlss_storage1_1,dlss_storage3_1]=dlss_storage:get_segments(),

  dlss_storage:remove(storage1),
  [storage3]=dlss_storage:get_storages(),
  [dlss_storage3_1]=dlss_storage:get_segments(),

  dlss_storage:remove(storage3),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

segment_children(_Config)->

  ok=dlss_storage:add(storage1,disc),
  disc=dlss_storage:get_type(storage1),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [dlss_storage1_1,dlss_storage1_2]=dlss_storage:get_segments(storage1),
  [{_,dlss_storage1_2}] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3]=dlss_storage:get_segments(storage1),
  [{_,dlss_storage1_2},{_,dlss_storage1_3}] = dlss_storage:get_children(dlss_storage1_1),
  [{_,dlss_storage1_3}] = dlss_storage:get_children(dlss_storage1_2),

  ok = dlss_storage:spawn_segment(dlss_storage1_3),
  [{_,dlss_storage1_4}] = dlss_storage:get_children(dlss_storage1_3),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_3},
    {_,dlss_storage1_4}
  ] = dlss_storage:get_children(dlss_storage1_1),
  [] = dlss_storage:get_children(dlss_storage1_4),

  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

get_key_segments(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  % fill the storage with records
  Count = 20000,
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y, V})
    end || V <- lists:seq(1, Count) ],
  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  [
    dlss_storage1_2,
    dlss_storage1_1
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  % simulate the segments split. Move the first half of records into
  % the newly spawned segment dlss_storage1_3
  HalfCount = 10000,
  [ begin
      Key = {x, V},
      Value = {y, V},
      ok = dlss_segment:dirty_write(dlss_storage1_3, Key, Value ),
      ok = dlss_segment:dirty_delete(dlss_storage1_1, Key )
    end || V <- lists:seq(1, HalfCount) ],
  % The newborn segment is filled with its keys, move it to the level of the parent
  dlss_storage:level_up( dlss_storage1_3 ),
  % Now it stands before its previous parent
  [
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % And the parent now doesn't contain the key
  [
    dlss_storage1_2,
    dlss_storage1_3
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  % But it contains the second half of the keys
  [
    dlss_storage1_2,
    dlss_storage1_1
  ] = dlss_storage:get_key_segments(storage1, {x,10500}),

  % The edge cases
  [
    dlss_storage1_2,
    dlss_storage1_3
  ] = dlss_storage:get_key_segments(storage1, {x,10000}),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ] = dlss_storage:get_key_segments(storage1, {x,10001}),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % split the first segment of the level segments
  ok = dlss_storage:spawn_segment(dlss_storage1_3),
  [
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_4,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % Now the child of dlss_storage1_3 may also contain its keys
  [
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_4
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  % But it can't contain keys from the dlss_storage1_3
  [
    dlss_storage1_2,
    dlss_storage1_1
  ] = dlss_storage:get_key_segments(storage1, {x,10001}),

  % split the second segment of the level segments
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_4,
    dlss_storage1_1,
    dlss_storage1_5
  ]=dlss_storage:get_segments(storage1),

  % Keys from the first half a still in the same segments
  [
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_4
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  % But keys from the second half may be now in the 3 segments
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_5
  ] = dlss_storage:get_key_segments(storage1, {x,10001}),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.


storage_read(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  % fill the storage with records
  Count = 20000,
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  {y,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y,10020} = dlss_storage:dirty_read(storage1,{x,10020}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),
  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {y,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y,10020} = dlss_storage:dirty_read(storage1,{x,10020}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {y,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y,10020} = dlss_storage:dirty_read(storage1,{x,10020}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),

  % simulate the segments split. Move the first half of records into
  % the newly spawned segment dlss_storage1_3
  HalfCount = 10000,
  [ begin
      Key = {x, V},
      Value = {y, V},
      ok = dlss_segment:dirty_write(dlss_storage1_3, Key, Value ),
      ok = dlss_segment:dirty_delete(dlss_storage1_1, Key )
    end || V <- lists:seq(1, HalfCount) ],

  % check keys
  {y,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y,10020} = dlss_storage:dirty_read(storage1,{x,10020}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),

  % The newborn segment is filled with its keys, move it to the level of the parent
  dlss_storage:level_up( dlss_storage1_3 ),
  % Now it stands before its previous parent
  [
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {y,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y,10020} = dlss_storage:dirty_read(storage1,{x,10020}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),

  % Update every second key in the storage with a new value
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y1, V})
    end || V <- lists:seq(1, Count, 2) ],

  % check keys
  {y1,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y1,10021} = dlss_storage:dirty_read(storage1,{x,10021}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_4,
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % Update every fourth key in the storage with a new value
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y2, V})
    end || V <- lists:seq(1, Count, 4) ],

  % check keys
  {y2,1} = dlss_storage:dirty_read(storage1,{x,1}),
  {y,20} = dlss_storage:dirty_read(storage1,{x,20}),
  {y2,10021} = dlss_storage:dirty_read(storage1,{x,10021}),
  {y,20000} = dlss_storage:dirty_read(storage1,{x,20000}),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_next(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  % fill the storage with records
  Count = 20000,
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),
  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),

  % simulate the segments split. Move the first half of records into
  % the newly spawned segment dlss_storage1_3
  HalfCount = 10000,
  [ begin
      Key = {x, V},
      Value = {y, V},
      ok = dlss_segment:dirty_write(dlss_storage1_3, Key, Value ),
      ok = dlss_segment:dirty_delete(dlss_storage1_1, Key )
    end || V <- lists:seq(1, HalfCount) ],

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),

  % The newborn segment is filled with its keys, move it to the level of the parent
  dlss_storage:level_up( dlss_storage1_3 ),

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),

  % Delete every second key in the storage
  [ begin
      ok = dlss:dirty_delete(storage1, {x, V})
    end || V <- lists:seq(1, Count, 2) ],

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    {x,2} = dlss_storage:next(storage1,{x,0}),
    {x,2} = dlss_storage:next(storage1,{x,1}),
    {x,22} = dlss_storage:next(storage1,{x,20}),
    {x,10002} = dlss_storage:next(storage1,{x,10000}),
    {x,10022} = dlss_storage:next(storage1,{x,10020}),
    '$end_of_table' = dlss_storage:next(storage1,{x,20000}),
    ok
  end ),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_4,
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % Update every fourth key in the storage with a new value
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y2, V})
    end || V <- lists:seq(1, Count, 4) ],

  % check keys
  {x,1} = dlss_storage:dirty_next(storage1,{x,0}),
  {x,2} = dlss_storage:dirty_next(storage1,{x,1}),
  {x,21} = dlss_storage:dirty_next(storage1,{x,20}),
  {x,10001} = dlss_storage:dirty_next(storage1,{x,10000}),
  {x,10021} = dlss_storage:dirty_next(storage1,{x,10020}),
  '$end_of_table' = dlss_storage:dirty_next(storage1,{x,20000}),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    {x,1} = dlss_storage:next(storage1,{x,0}),
    {x,2} = dlss_storage:next(storage1,{x,1}),
    {x,21} = dlss_storage:next(storage1,{x,20}),
    {x,10001} = dlss_storage:next(storage1,{x,10000}),
    {x,10021} = dlss_storage:next(storage1,{x,10020}),
    '$end_of_table' = dlss_storage:next(storage1,{x,20000}),
    ok
  end ),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.


storage_prev(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  % fill the storage with records
  Count = 20000,
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,19} = dlss_storage:dirty_prev(storage1,{x,20}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10020} = dlss_storage:dirty_prev(storage1,{x,10021}),
  {x,20000} = dlss_storage:dirty_prev(storage1,{x,20001}),
  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,19} = dlss_storage:dirty_prev(storage1,{x,20}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10020} = dlss_storage:dirty_prev(storage1,{x,10021}),
  {x,20000} = dlss_storage:dirty_prev(storage1,{x,20001}),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,19} = dlss_storage:dirty_prev(storage1,{x,20}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10020} = dlss_storage:dirty_prev(storage1,{x,10021}),
  {x,20000} = dlss_storage:dirty_prev(storage1,{x,20001}),

  % simulate the segments split. Move the first half of records into
  % the newly spawned segment dlss_storage1_3
  HalfCount = 10000,
  [ begin
      Key = {x, V},
      Value = {y, V},
      ok = dlss_segment:dirty_write(dlss_storage1_3, Key, Value ),
      ok = dlss_segment:dirty_delete(dlss_storage1_1, Key )
    end || V <- lists:seq(1, HalfCount) ],

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,19} = dlss_storage:dirty_prev(storage1,{x,20}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10020} = dlss_storage:dirty_prev(storage1,{x,10021}),
  {x,20000} = dlss_storage:dirty_prev(storage1,{x,20001}),

  % The newborn segment is filled with its keys, move it to the level of the parent
  dlss_storage:level_up( dlss_storage1_3 ),

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,19} = dlss_storage:dirty_prev(storage1,{x,20}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10020} = dlss_storage:dirty_prev(storage1,{x,10021}),
  {x,20000} = dlss_storage:dirty_prev(storage1,{x,20001}),

  % Delete every second key in the storage
  [ begin
      ok = dlss:dirty_delete(storage1, {x, V})
    end || V <- lists:seq(1, Count, 2) ],

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,19} = dlss_storage:dirty_prev(storage1,{x,20}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10020} = dlss_storage:dirty_prev(storage1,{x,10021}),
  {x,20000} = dlss_storage:dirty_prev(storage1,{x,20001}),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    '$end_of_table' = dlss_storage:prev(storage1,{x,1}),
    '$end_of_table' = dlss_storage:prev(storage1,{x,2}),
    {x,2} = dlss_storage:prev(storage1,{x,4}),
    {x,20} = dlss_storage:prev(storage1,{x,22}),
    {x,10000} = dlss_storage:prev(storage1,{x,10001}),
    {x,10000} = dlss_storage:prev(storage1,{x,10002}),
    {x,10020} = dlss_storage:prev(storage1,{x,10022}),
    {x,19998} = dlss_storage:prev(storage1,{x,20000}),
    ok
  end ),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_4,
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % Update every fourth key in the storage with a new value
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y2, V})
    end || V <- lists:seq(1, Count, 4) ],

  % check keys
  '$end_of_table' = dlss_storage:dirty_prev(storage1,{x,1}),
  {x,1} = dlss_storage:dirty_prev(storage1,{x,2}),
  {x,20} = dlss_storage:dirty_prev(storage1,{x,21}),
  {x,21} = dlss_storage:dirty_prev(storage1,{x,22}),
  {x,10000} = dlss_storage:dirty_prev(storage1,{x,10001}),
  {x,10021} = dlss_storage:dirty_prev(storage1,{x,10022}),
  {x,19999} = dlss_storage:dirty_prev(storage1,{x,20000}),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    '$end_of_table' = dlss_storage:prev(storage1,{x,1}),
    {x,1} = dlss_storage:prev(storage1,{x,2}),
    {x,2} = dlss_storage:prev(storage1,{x,4}),
    {x,21} = dlss_storage:prev(storage1,{x,22}),
    {x,10000} = dlss_storage:prev(storage1,{x,10001}),
    {x,10001} = dlss_storage:prev(storage1,{x,10002}),
    {x,10021} = dlss_storage:prev(storage1,{x,10022}),
    {x,19998} = dlss_storage:prev(storage1,{x,20000}),
    ok
  end ),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_first(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  '$end_of_table' = dlss_storage:dirty_first(storage1),
  % fill the storage with records
  Count = 20000,
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  % simulate the segments split. Move the first half of records into
  % the newly spawned segment dlss_storage1_3
  HalfCount = 10000,
  [ begin
      Key = {x, V},
      Value = {y, V},
      ok = dlss_segment:dirty_write(dlss_storage1_3, Key, Value ),
      ok = dlss_segment:dirty_delete(dlss_storage1_1, Key )
    end || V <- lists:seq(1, HalfCount) ],

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  % The newborn segment is filled with its keys, move it to the level of the parent
  dlss_storage:level_up( dlss_storage1_3 ),

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  % Delete every second key in the storage
  [ begin
      ok = dlss:dirty_delete(storage1, {x, V})
    end || V <- lists:seq(1, Count, 2) ],

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    {x,2} = dlss_storage:first(storage1),
    ok
  end ),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_4,
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % Update every fourth key in the storage with a new value
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y2, V})
    end || V <- lists:seq(1, Count, 4) ],

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    {x,1} = dlss_storage:first(storage1),
    ok
  end ),

  % Remove all records from the storage
  [ begin
      ok = dlss:dirty_delete(storage1, {x, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  {x,1} = dlss_storage:dirty_first(storage1),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    '$end_of_table' = dlss_storage:first(storage1),
    ok
  end ),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_last(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  '$end_of_table' = dlss_storage:dirty_last(storage1),
  % fill the storage with records
  Count = 20000,
  [ begin
      ok = dlss:dirty_write(storage1, {x, V}, {y, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_2,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_2,
    dlss_storage1_1,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  % simulate the segments split. Move the first half of records into
  % the newly spawned segment dlss_storage1_3
  HalfCount = 10000,
  [ begin
      Key = {x, V},
      Value = {y, V},
      ok = dlss_segment:dirty_write(dlss_storage1_3, Key, Value ),
      ok = dlss_segment:dirty_delete(dlss_storage1_1, Key )
    end || V <- lists:seq(1, HalfCount) ],

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  % The newborn segment is filled with its keys, move it to the level of the parent
  dlss_storage:level_up( dlss_storage1_3 ),

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  % Delete every second key in the storage
  dlss:dirty_delete(storage1, {x, Count}),

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    {x,19999} = dlss_storage:last(storage1),
    ok
  end ),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Create a new root segment
  ok = dlss_storage:new_root_segment(storage1),
  [
    dlss_storage1_4,
    dlss_storage1_2,
    dlss_storage1_3,
    dlss_storage1_1
  ]=dlss_storage:get_segments(storage1),

  dlss:dirty_write(storage1, {x, Count},{y,Count}),

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    {x,Count} = dlss_storage:last(storage1),
    ok
  end ),

  % Remove all records from the storage
  [ begin
      ok = dlss:dirty_delete(storage1, {x, V})
    end || V <- lists:seq(1, Count) ],

  % check keys
  {x,Count} = dlss_storage:dirty_last(storage1),

  % safe check keys
  {ok,_}  =  dlss:transaction(fun()->
    '$end_of_table' = dlss_storage:last(storage1),
    ok
  end ),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.


storage_scan(_Config) ->
  ok = dlss_storage:add(storage1, ram),
  ok = dlss_storage:spawn_segment(dlss_storage1_1, 5),

  ?assertEqual(
    [dlss_storage1_1, dlss_storage1_2],
    dlss_storage:get_segments(storage1)
  ),

  [ ok = dlss_segment:dirty_write(dlss_storage1_1, X, new_value) || X <- lists:seq(1, 5) ],
  [ ok = dlss_segment:dirty_write(dlss_storage1_2, X, old_value) || X <- lists:seq(4, 7) ],

  ?assertEqual(
    [{3, new_value}, {4, new_value}, {5, new_value}, {6, old_value}],
    dlss_storage:scan_interval(storage1, 3, 7, _Limit = 4)
  ),

  ?assertEqual([{3, new_value}], dlss_storage:scan_interval(storage1, 3, 3)),

  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_scan_infinity(_Config) ->
  ok = dlss_storage:add(storage1, ram),
  ?assertEqual(
    [dlss_storage1_1],
    dlss_storage:get_segments(storage1)
  ),

  % Put value into the storage
  [ ok = dlss_segment:dirty_write(dlss_storage1_1, X, value) || X <- lists:seq(1, 3) ],

  ?assertEqual(
    [{1, value}, {2, value}, {3, value}],
    dlss_storage:scan_interval(storage1, '$start_of_table', '$end_of_table', 10)
  ),

   ?assertEqual(
    [{1, value}, {2, value}],
    dlss_storage:scan_interval(storage1, '$start_of_table', 2, 10)
  ),

  ?assertEqual(
    [{3, value}],
    dlss_storage:scan_interval(storage1, 3, '$end_of_table', 10)
  ),

  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_scan_boundaries(_Config) ->
  ok = dlss_storage:add(storage1, ram),
  ok = dlss_storage:spawn_segment(dlss_storage1_1, 5),
  ok = dlss_storage:spawn_segment(dlss_storage1_1, 15),
  ok = dlss_storage:spawn_segment(dlss_storage1_1, 25),
  ?assertEqual(
    [dlss_storage1_1, dlss_storage1_2, dlss_storage1_3, dlss_storage1_4],
    dlss_storage:get_segments(storage1)
  ),

  [ ok = dlss_segment:dirty_write(dlss_storage1_1, X, value1) || X <- lists:seq(1, 3) ],
  [ ok = dlss_segment:dirty_write(dlss_storage1_2, X, value2) || X <- lists:seq(5, 12) ],
  [ ok = dlss_segment:dirty_write(dlss_storage1_3, X, value3) || X <- lists:seq(15, 20) ],
  [ ok = dlss_segment:dirty_write(dlss_storage1_4, X, value4) || X <- lists:seq(25, 26) ],

  ?assertEqual(
    [{10, value2}, {11, value2}, {12, value2}, {15, value3}, {16, value3}, {17, value3}],
    dlss_storage:scan_interval(storage1, 10, 17, 100)
  ),

  StartOfTable =
    [{I,value1}||I<-lists:seq(1,3)] ++
    [{I,value2}||I<-lists:seq(5,12)] ++
    [{I,value3}||I<-lists:seq(15,20)],
  ?assertEqual(
    StartOfTable,
    dlss_storage:scan_interval(storage1, '$start_of_table', 20, 100)
  ),

  EndOfTable =
    [{I,value2}||I<-lists:seq(5,12)] ++
    [{I,value3}||I<-lists:seq(15,20)]++
    [{I,value4}||I<-lists:seq(25,26)],
  ?assertEqual(
    EndOfTable,
    dlss_storage:scan_interval(storage1, 5, '$end_of_table', 100)
  ),


  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_scan_deleted(_Config) ->
  ok = dlss_storage:add(storage1, ram),
  ok = dlss_storage:spawn_segment(dlss_storage1_1, 3),

  ?assertEqual(
    [dlss_storage1_1, dlss_storage1_2],
    dlss_storage:get_segments(storage1)
  ),

  [ ok = dlss_segment:dirty_write(dlss_storage1_1, X, value) || X <- lists:seq(1, 5) ],
  ok = dlss_segment:dirty_write(dlss_storage1_2, 4, old_value),

  ?assertEqual(
    [{3, value}, {4, value}, {5, value}],
    dlss_storage:scan_interval(storage1, 3, 7, _Limit = 4)
  ),

  ok = dlss_storage:dirty_delete(storage1, 4),

  ?assertEqual(
    [{3, value}, {5, value}],
    dlss_storage:scan_interval(storage1, 3, 7, _Limit = 4)
  ),

  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.