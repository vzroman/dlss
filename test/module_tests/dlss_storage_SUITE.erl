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
  segment_split/1,
  segment_children/1,
  absorb_segment/1,
  get_key_segments/1,
  storage_read/1,
  storage_next/1,
  storage_prev/1,
  storage_first/1,
  storage_last/1,
  create_root_segment/1,
  write_data_to_storage/1
]).


all()->
  [
    service_api,
    segment_split,
    segment_children,
    absorb_segment,
    get_key_segments,
    storage_read,
    storage_next,
    storage_prev,
    storage_first,
    storage_last,
    {group, add_root_segment}
  ].

groups()->
  [
    {add_root_segment,
      [parallel],  % Run strategy
      [
        create_root_segment,
        write_data_to_storage
      ]
    }
  ].

%% Init system storages
init_per_suite(Config)->
  dlss_backend:init_backend(),
  Config.
end_per_suite(_Config)->
  dlss_backend:stop(),
  ok.

init_per_group(add_root_segment,Config)->
  ok=dlss_storage:add(storage_1,disc),
  Config;
init_per_group(_,Config)->
  Config.

end_per_group(add_root_segment,_Config)->
  % Clean up
  dlss_storage:remove(storage_1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),
  ok;
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

segment_split(_Config)->

  ok=dlss_storage:add(storage1,disc),
  disc=dlss_storage:get_type(storage1),
  [storage1]=dlss_storage:get_storages(),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [dlss_storage1_1,dlss_storage1_2]=dlss_storage:get_segments(storage1),
  { ok, #{
    level := 1,
    key := '_'
  } } = dlss_storage:segment_params(dlss_storage1_2),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,100),
  [dlss_storage1_1, dlss_storage1_2, dlss_storage1_3]=dlss_storage:get_segments(storage1),
  { ok, #{
    level := 1,
    key := 100
  } } = dlss_storage:segment_params(dlss_storage1_3),

  dlss_storage:remove(storage1),
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

  ok = dlss_storage:spawn_segment(dlss_storage1_1,some_split_key),
  [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3]=dlss_storage:get_segments(storage1),
  [{_,dlss_storage1_2},{_,dlss_storage1_3}] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [{_,dlss_storage1_4}] = dlss_storage:get_children(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}] = dlss_storage:get_children(dlss_storage1_1),
  [] = dlss_storage:get_children(dlss_storage1_3),

  ok = dlss_storage:spawn_segment(dlss_storage1_2,next_split_key),
  [{_,dlss_storage1_4},{_,dlss_storage1_5}] = dlss_storage:get_children(dlss_storage1_2),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),
  [] = dlss_storage:get_children(dlss_storage1_3),

  ok = dlss_storage:spawn_segment(dlss_storage1_3),
  [{_,dlss_storage1_6}] = dlss_storage:get_children(dlss_storage1_3),
  [{_,dlss_storage1_4},{_,dlss_storage1_5}] = dlss_storage:get_children(dlss_storage1_2),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3},
    {_,dlss_storage1_6}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ?assertError( { invalid_split_key, 22 }, dlss_storage:spawn_segment(dlss_storage1_3, 22) ),

  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

absorb_segment(_Config)->

  ok=dlss_storage:add(storage1,disc),
  disc=dlss_storage:get_type(storage1),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [dlss_storage1_1,dlss_storage1_2]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,some_split_key),
  [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2,next_split_key),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_3),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3},
    {_,dlss_storage1_6}
  ] = dlss_storage:get_children(dlss_storage1_1),

  { error, root_segment } = dlss_storage:absorb_segment(dlss_storage1_1),

  %-----------------------------------------------------------------
  % Absorb the segment from the level 1
  %-----------------------------------------------------------------
  % The parent
  { ok, #{
    level := 1,
    key := '_'
  } } = dlss_storage:segment_params(dlss_storage1_2),

  % The children
  { ok, #{
    level := 2,
    key := '_'
  } } = dlss_storage:segment_params(dlss_storage1_4),

  { ok, #{
    level := 2,
    key := next_split_key
  } } = dlss_storage:segment_params(dlss_storage1_5),

  % The ABSORB
  ok = dlss_storage:absorb_segment( dlss_storage1_2 ),
  % The storage segments after the absorb
  [
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3},
    {_,dlss_storage1_6}
  ] = dlss_storage:get_children(dlss_storage1_1),

  % The children are at the level of the parent segment now
  { ok, #{
    level := 1,
    key := '_'
  } } = dlss_storage:segment_params(dlss_storage1_4),

  { ok, #{
    level := 1,
    key := next_split_key
  } } = dlss_storage:segment_params(dlss_storage1_5),

  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

get_key_segments(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_1,
    dlss_storage1_2
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,{x,50}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  [
    dlss_storage1_1,
    dlss_storage1_2
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  [
    dlss_storage1_1,
    dlss_storage1_3
  ] = dlss_storage:get_key_segments(storage1, {x,70}),

  % The edge case
  [
    dlss_storage1_1,
    dlss_storage1_3
  ] = dlss_storage:get_key_segments(storage1, {x,50}),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Keep splitting deeper
  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2, {x,25} ),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_4
  ] = dlss_storage:get_key_segments(storage1, {x,20}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_5
  ] = dlss_storage:get_key_segments(storage1, {x,40}),

  % The edge condition
  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_5
  ] = dlss_storage:get_key_segments(storage1, {x,25}),


  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.


storage_read(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_1,
    dlss_storage1_2
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,{x,50}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  % Put the value into the wrong segment
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,20},{l1,wrong_location}),
  not_found = dlss_storage:dirty_read(storage1,{x,20}),

  % Put the value to the level 1
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,20},{l1,20}),
  {l1,20} = dlss_storage:dirty_read(storage1,{x,20}),

  % The root
  ok = dlss_storage:dirty_write(storage1,{x,20},{root,20}),
  {root,20} = dlss_storage:dirty_read(storage1,{x,20}),

  % The edge condition
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,50},{l1,edge}),
  {l1,edge} = dlss_storage:dirty_read(storage1,{x,50}),

  %------------------------------------------------------
  % Three levels
  %------------------------------------------------------
  % Keep splitting deeper
  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2, {x,25} ),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  % The first subbranch
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,24},{y,24}),
  {y,24} = dlss_storage:dirty_read(storage1,{x,24}),
  % The second subbranch
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,26},{y,26}),
  {y,26} = dlss_storage:dirty_read(storage1,{x,26}),
  % The edge
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,25},{y,25}),
  {y,25} = dlss_storage:dirty_read(storage1,{x,25}),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_next(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_1,
    dlss_storage1_2
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,{x,50}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2, {x,25} ),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  % Fill in the root
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,20},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,26},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,49},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,52},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,53},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,130},l0),

  % Fill in the level 1
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,10},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,12},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,26},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,27},l1),

  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,50},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,53},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,57},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,170},l1),

  % Fill in the level 2
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,5},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,10},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,13},l2),

  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,25},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,26},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,47},l2),

  Expected = [
    {x,5},{x,10},{x,12},
    {x,13},{x,20},{x,25},
    {x,26},{x,27},{x,47},
    {x,49},{x,50},{x,52},
    {x,53},{x,57},{x,130},
    {x,170}
  ],

  % Dirty mode
  DirtyIterator=
    fun(K,Acc,I)->
      case dlss_storage:dirty_next(storage1,K) of
        '$end_of_table'->lists:reverse(Acc);
        N->
          ct:pal("dirty next key ~p",[N]),
          I(N,[N|Acc],I)
      end
    end,
  Expected  = DirtyIterator(0,[],DirtyIterator),

  % Safe mode
  SafeIterator=
    fun(K,Acc,I)->
      case dlss_storage:next(storage1,K) of
        '$end_of_table'->lists:reverse(Acc);
        N->
          ct:pal("safe next key ~p",[N]),
          I(N,[N|Acc],I)
      end
    end,

  {ok, Expected}  =  dlss:transaction(fun()->
    SafeIterator(0,[],SafeIterator)
  end ),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.


storage_prev(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_1,
    dlss_storage1_2
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,{x,50}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2, {x,25} ),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  % Fill in the root
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,20},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,26},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,49},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,52},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,53},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,130},l0),

  % Fill in the level 1
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,10},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,12},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,26},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,27},l1),

  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,50},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,53},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,57},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,170},l1),

  % Fill in the level 2
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,5},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,10},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,13},l2),

  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,25},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,26},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,47},l2),

  Expected = [
    {x,170},{x,130},{x,57},
    {x,53},{x,52},{x,50},
    {x,49},{x,47},{x,27},
    {x,26},{x,25},{x,20},
    {x,13},{x,12},{x,10},
    {x,5}
  ],

  % Dirty mode
  DirtyIterator=
    fun(K,Acc,I)->
      case dlss_storage:dirty_prev(storage1,K) of
        '$end_of_table'->lists:reverse(Acc);
        N->
          ct:pal("dirty prev key ~p",[N]),
          I(N,[N|Acc],I)
      end
    end,
  Expected  = DirtyIterator({x,x},[],DirtyIterator),

  % Safe mode
  SafeIterator=
    fun(K,Acc,I)->
      case dlss_storage:prev(storage1,K) of
        '$end_of_table'->lists:reverse(Acc);
        N->
          ct:pal("safe prev key ~p",[N]),
          I(N,[N|Acc],I)
      end
    end,

  {ok, Expected}  =  dlss:transaction(fun()->
    SafeIterator({x,x},[],SafeIterator)
  end ),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments(),

  ok.

storage_first(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_1,
    dlss_storage1_2
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,{x,50}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2, {x,25} ),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  % Fill in the root
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,20},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,26},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,49},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,52},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,53},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,130},l0),

  % Fill in the level 1
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,10},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,12},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,26},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,27},l1),

  {ok,{x,10}}  =  dlss:transaction(fun()->
    dlss_storage:first(storage1)
  end ),

  {x,10} = dlss_storage:dirty_first(storage1),

  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,50},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,53},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,57},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,170},l1),

  % Fill in the level 2
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,5},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,10},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,13},l2),

  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,25},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,26},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,47},l2),

  {ok,{x,5}}  =  dlss:transaction(fun()->
    dlss_storage:first(storage1)
  end ),

  {x,5} = dlss_storage:dirty_first(storage1),


  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments().



storage_last(_Config)->

  ok=dlss_storage:add(storage1,disc),
  [dlss_storage1_1]=dlss_storage:get_segments(storage1),

  %------------------------------------------------------
  % Two levels
  %------------------------------------------------------
  ok = dlss_storage:spawn_segment(dlss_storage1_1),
  [
    dlss_storage1_1,
    dlss_storage1_2
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_1,{x,50}),

  [
    dlss_storage1_1,
    dlss_storage1_2,
    dlss_storage1_3
  ]=dlss_storage:get_segments(storage1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2),
  [
    {_,dlss_storage1_2},{_,dlss_storage1_4},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  ok = dlss_storage:spawn_segment(dlss_storage1_2, {x,25} ),
  [
    {_,dlss_storage1_2},
    {_,dlss_storage1_4},{_,dlss_storage1_5},
    {_,dlss_storage1_3}
  ] = dlss_storage:get_children(dlss_storage1_1),

  % Fill in the root
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,20},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,26},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,49},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,52},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,53},l0),
  ok = dlss_segment:dirty_write(dlss_storage1_1,{x,130},l0),

  {ok,{x,130}}  =  dlss:transaction(fun()->
    dlss_storage:last(storage1)
  end ),

  {x,130} = dlss_storage:dirty_last(storage1),

  % Fill in the level 1
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,10},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,12},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,26},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_2,{x,27},l1),

  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,50},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,53},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,57},l1),
  ok = dlss_segment:dirty_write(dlss_storage1_3,{x,170},l1),

  % Fill in the level 2
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,5},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,10},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_4,{x,13},l2),

  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,25},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,26},l2),
  ok = dlss_segment:dirty_write(dlss_storage1_5,{x,47},l2),

  {ok,{x,170}}  =  dlss:transaction(fun()->
    dlss_storage:last(storage1)
  end ),

  {x,170} = dlss_storage:dirty_last(storage1),

  % Clean up
  dlss_storage:remove(storage1),
  []=dlss_storage:get_storages(),
  []=dlss_storage:get_segments().


create_root_segment(_Config)->
  % Check for storage type
  disc=dlss_storage:get_type(storage_1),

  % Check for storage Root segment
  [dlss_storage_1_1]=dlss_storage:get_segments(storage_1),

  % Wait for 1 second
  timer:sleep(1000),

  % Spawn a new segment for storage
  ok = dlss_storage:spawn_segment(dlss_storage_1_1),
  [dlss_storage_1_1,dlss_storage_1_2]=dlss_storage:get_segments(storage_1),
  [{_,dlss_storage_1_2}] = dlss_storage:get_children(dlss_storage_1_1),

  % Spawn new segment with splitting segment of dlss_storage_1_1
  ok = dlss_storage:spawn_segment(dlss_storage_1_1,some_split_key),
  [dlss_storage_1_1,dlss_storage_1_2,dlss_storage_1_3]=dlss_storage:get_segments(storage_1),

  % Add a new Root segment for storage
  ok = dlss_storage:new_root_segment(storage_1),

  % Check for segments
  [dlss_storage_1_4,dlss_storage_1_1,dlss_storage_1_2,dlss_storage_1_3]=dlss_storage:get_segments(storage_1),

  % Check level of new created Root segment
  { ok, #{
    storage := storage_1,
    level := 0
  } } = dlss_storage:segment_params(dlss_storage_1_4),

  % Check level of dlss_storage_1_1
  { ok, #{
    storage := storage_1,
    level := 1
  } } = dlss_storage:segment_params(dlss_storage_1_1),

  % Check level of dlss_storage_1_2
  { ok, #{
    storage := storage_1,
    level := 2
  } } = dlss_storage:segment_params(dlss_storage_1_2),

  % Check level of dlss_storage_1_3
  { ok, #{
    storage := storage_1,
    level := 2
  } } = dlss_storage:segment_params(dlss_storage_1_3),

  ok.

write_data_to_storage(_Config)->
  % Put the value to storage
  [begin
     ok = dlss_storage:dirty_write(storage_1,{x,N},{y,N})
   end || N <- lists:seq(1,200000)],

  % Get value from storage
  [begin
     {y,N} = dlss_storage:dirty_read(storage_1,{x,N})
   end || N <- lists:seq(1,200000)],
  ok.





