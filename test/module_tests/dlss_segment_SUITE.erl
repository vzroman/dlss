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

% -include("dlss_test.hrl").
-include("dlss.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(GET(Key,Config),proplists:get_value(Key,Config)).
-define(GET(Key,Config,Default),proplists:get_value(Key,Config,Default)).

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
  test_read/1,
  test_order/1,
  test_median/1
]).


all()->
  [
    test_read,
    test_order,
    test_median
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  Config.

end_per_suite(Config)->
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.


init_per_testcase(_, Config)->
  dlss_backend:init_backend(),
  dlss:add_storage(order_test, disc),
  [Segment] = dlss:get_segments(order_test),
  [
    {storage, order_test},
    {segment, Segment}
    | Config
  ].

end_per_testcase(_, Config)->
  Storage=?GET(storage, Config),
  dlss:remove_storage(Storage),
  dlss_backend:stop(),
  ok.

test_read(Config)->
  Segment=?GET(segment,Config),


  ok=dlss_segment:dirty_write(Segment,{1,2},{value,1}),
  {value,1}=dlss_segment:dirty_read(Segment,{1,2}),

  ok=dlss_segment:dirty_delete(Segment,{1,2}),
  not_found=dlss_segment:dirty_read(Segment,{1,2}),

  ok.

test_order(Config)->
  Segment=?GET(segment,Config),

  %% Simple integer
  ok=dlss_segment:dirty_write(Segment,{10,1},test_value),
  ok=dlss_segment:dirty_write(Segment,{10,2},test_value),
  {10,1}=dlss_segment:dirty_next(Segment,{2,10}),
  {10,2}=dlss_segment:dirty_next(Segment,{10,1}),
  '$end_of_table'=dlss_segment:dirty_next(Segment,{10,2}),

  % an atom is greater than an integer
  ok=dlss_segment:dirty_write(Segment,{test,5},test_value),
  ok=dlss_segment:dirty_write(Segment,{5,test},test_value),
  {5,test}=dlss_segment:dirty_first(Segment),
  {10,1}=dlss_segment:dirty_next(Segment,{5,test}),
  {test,5}=dlss_segment:dirty_next(Segment,{10,2}),

  % a tuple is greater than an atom
  ok=dlss_segment:dirty_write(Segment,{{test},5},test_value),
  ok=dlss_segment:dirty_write(Segment,{5,{test}},test_value),
  {5,{test}}=dlss_segment:dirty_next(Segment,{5,test}),
  {{test},5}=dlss_segment:dirty_next(Segment,{test,5}),

  % a longer tuple is greater than a shorter one
  ok=dlss_segment:dirty_write(Segment,{{0,test},5},test_value),
  ok=dlss_segment:dirty_write(Segment,{5,{0,test}},test_value),
  {5,{0,test}}=dlss_segment:dirty_next(Segment,{5,{test}}),
  {{0,test},5}=dlss_segment:dirty_next(Segment,{{test},5}),

  % a list is greater than a tuple
  ok=dlss_segment:dirty_write(Segment,{[test],5},test_value),
  ok=dlss_segment:dirty_write(Segment,{5,[test]},test_value),
  {5,[test]}=dlss_segment:dirty_next(Segment,{5,{0,test}}),
  {[test],5}=dlss_segment:dirty_next(Segment,{{0,test},5}),

  % lists are compared lexicographically
  ok=dlss_segment:dirty_write(Segment,{[5,test],5},test_value),
  ok=dlss_segment:dirty_write(Segment,{5,[5,test]},test_value),
  {5,[5,test]}=dlss_segment:dirty_next(Segment,{5,{0,test}}),
  {[5,test],5}=dlss_segment:dirty_next(Segment,{{0,test},5}),

  % a binary is greater than a list
  ok=dlss_segment:dirty_write(Segment,{<<0>>,5},test_value),
  ok=dlss_segment:dirty_write(Segment,{5,<<0>>},test_value),
  {5,<<0>>}=dlss_segment:dirty_next(Segment,{5,[test]}),
  {<<0>>,5}=dlss_segment:dirty_next(Segment,{[test],5}),

  ok.

test_median(Config)->
  Segment = ?GET(segment, Config),

  %% no elements 
  ?assertEqual(
    {error, null_segment},
    dlss_segment:median(Segment)
  ),

  %% 1 element
  ok = dlss_segment:dirty_write(Segment, {10, 1}, 1),
  ?assertEqual(
    {error, single_element}, 
    dlss_segment:median(Segment)
  ),

  %% a lot of elements
  [ dlss_segment:dirty_write(Segment, {10, I}, I) || I <- lists:seq(2, 10) ],
  ok = dlss_segment:dirty_write(Segment, {10, 11}, <<"some_long_name">>),
  ?assertEqual(
    {ok, {10, 7}}, 
    dlss_segment:median(Segment)
  ),

  ok.

