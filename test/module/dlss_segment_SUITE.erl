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
  test_read/1,
  test_order/1,
  test_scan/1,
  test_ram_scan/1
]).


all()->
  [
    test_read
    ,test_order
    ,test_scan
    ,test_ram_scan
  ].

groups()->
  [].

%% Init system storages
init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
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

test_scan(_Config)->

  dlss:add_storage(ram_test, ram),
  [Ram] = dlss:get_segments(ram_test),

  dlss:add_storage(ramdisc_test, ramdisc),
  [RamDisc] = dlss:get_segments(ramdisc_test),

  dlss:add_storage(disc_test, disc),
  [Disc] = dlss:get_segments(disc_test),

  % Fill in the storage
  Count = 23456,
  [ [ dlss_segment:dirty_write( S, {key,I}, {value,I} ) || S<-[Ram,RamDisc,Disc] ]||I<-lists:seq(1,Count) ],

  %--------No items---------------
  R0= [ ],
  % Including
  R0 = dlss_segment:dirty_scan( Ram, [], '$end_of_table' ),
  R0 = dlss_segment:dirty_scan( RamDisc, [], '$end_of_table' ),
  R0 = dlss_segment:dirty_scan( Disc, [], '$end_of_table' ),
  % Limit
  R0 = dlss_segment:dirty_scan( Ram, [], 1 ),
  R0 = dlss_segment:dirty_scan( RamDisc, [], 1 ),
  R0 = dlss_segment:dirty_scan( Disc, [], 1 ),

  %---------Single item---------------
  R1= [ { {key,1}, {value,1}} ],
  % Including
  R1 = dlss_segment:dirty_scan( Ram, {key,0}, {key,1} ),
  R1 = dlss_segment:dirty_scan( RamDisc, {key,0}, {key,1} ),
  R1 = dlss_segment:dirty_scan( Disc, {key,0}, {key,1} ),
  % Edge
  R1 = dlss_segment:dirty_scan( Ram, {key,1}, {key,1} ),
  R1 = dlss_segment:dirty_scan( RamDisc, {key,1}, {key,1} ),
  R1 = dlss_segment:dirty_scan( Disc, {key,1}, {key,1} ),
  % Limit
  R1 = dlss_segment:dirty_scan( Ram, '$start_of_table', '$end_of_table', 1 ),
  R1 = dlss_segment:dirty_scan( RamDisc, '$start_of_table', '$end_of_table', 1 ),
  R1 = dlss_segment:dirty_scan( Disc, '$start_of_table', '$end_of_table', 1 ),

  %--------Less than the minimal batch-----------
  R123= [ {{key,I}, {value,I}} || I<- lists:seq(1,123)],
  % Including
  R123 = dlss_segment:dirty_scan( Ram, {key,0}, {key,123} ),
  R123 = dlss_segment:dirty_scan( RamDisc, {key,0}, {key,123} ),
  R123 = dlss_segment:dirty_scan( Disc, {key,0}, {key,123} ),
  % Edge
  R123 = dlss_segment:dirty_scan( Ram, {key,1}, {key,123} ),
  R123 = dlss_segment:dirty_scan( RamDisc, {key,1}, {key,123} ),
  R123 = dlss_segment:dirty_scan( Disc, {key,1}, {key,123} ),
  % Limit
  R123 = dlss_segment:dirty_scan( Ram, '$start_of_table', '$end_of_table', 123 ),
  R123 = dlss_segment:dirty_scan( RamDisc, '$start_of_table', '$end_of_table', 123 ),
  R123 = dlss_segment:dirty_scan( Disc, '$start_of_table', '$end_of_table', 123 ),

  %--------More than the minimal batch-----------
  R2345= [ {{key,I}, {value,I}} || I<- lists:seq(123,2345)],
  % By key
  R2345 = dlss_segment:dirty_scan( Ram, {key,123}, {key,2345} ),
  R2345 = dlss_segment:dirty_scan( RamDisc, {key,123}, {key,2345} ),
  R2345 = dlss_segment:dirty_scan( Disc, {key,123}, {key,2345} ),
  % Limit
  R2345 = dlss_segment:dirty_scan( Ram, {key,123}, '$end_of_table', 2345 - 123 + 1 ),
  R2345 = dlss_segment:dirty_scan( RamDisc, {key,123}, '$end_of_table', 2345 - 123 + 1 ),
  R2345 = dlss_segment:dirty_scan( Disc, {key,123}, '$end_of_table', 2345 - 123 + 1 ),
  % Infinity
  R2345 = dlss_segment:dirty_scan( Ram, {key,123}, {key,2345}, infinity ),
  R2345 = dlss_segment:dirty_scan( RamDisc, {key,123}, {key,2345}, infinity ),
  R2345 = dlss_segment:dirty_scan( Disc, {key,123}, {key,2345}, infinity ),

  dlss:remove_storage(ram_test),
  dlss:remove_storage(ramdisc_test),
  dlss:remove_storage(disc_test),

  ok.

test_ram_scan(_Config)->
  dlss:add_storage(ram_scan_test, ram),
  [Ram] = dlss:get_segments(ram_scan_test),

  % Fill in the storage
  Count0 = 20000,
  [ dlss_segment:dirty_write( Ram, {key,I}, {value,I} ) ||I<-lists:seq(1,Count0) ],

  R2345= [ {{key,I}, {value,I}} || I<- lists:seq(123,2345)],

  Start0 = erlang:system_time(millisecond),
  R2345 = dlss_segment:dirty_scan( Ram, {key,123}, {key,2345} ),
  Time0 = erlang:system_time(millisecond) - Start0,
  ?LOGDEBUG("time0 ~p",[Time0]),

  Count1 = 20000000,
  [ dlss_segment:dirty_write( Ram, {key,I+Count0}, {value,I+Count0} ) ||I<-lists:seq(1,Count1) ],

  Start1 = erlang:system_time(millisecond),
  R2345 = dlss_segment:dirty_scan( Ram, {key,123}, {key,2345} ),
  Time1 = erlang:system_time(millisecond) - Start1,
  ?LOGDEBUG("time1 ~p",[Time1]),

  Start2 = erlang:system_time(millisecond),
  dlss_segment:dirty_scan( Ram, {key,19990123}, {key,19992345} ),
  Time2 = erlang:system_time(millisecond) - Start2,
  ?LOGDEBUG("time2 ~p",[Time2]),

  % If the time1 is significantly bigger than the time2 then the issue with ets based
  % storage types is still not resolved

  dlss:remove_storage(ram_scan_test),

  ok.

