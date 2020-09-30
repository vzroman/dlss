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

-module(dlss_segment).

-include("dlss.hrl").

-behaviour(gen_server).

%%=================================================================
%%	STORAGE SEGMENT API
%%=================================================================
-export([
  dirty_next/2,
  dirty_write/2,dirty_write/3,
  dirty_delete/2
]).
%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/1
]).
%%=================================================================
%%	OTP
%%=================================================================
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state,{segment,cycle}).

-define(DEFAULT_SCAN_CYCLE,5000).

%%=================================================================
%%	STORAGE SEGMENT API
%%=================================================================
dirty_next(Segment,Pattern)->
  mnesia:dirty_next(Segment,Pattern).

dirty_write(Segment,{Key,Value})->
  dirty_write(Segment,Key,Value).
dirty_write(Segment,Key,Value)->
  mnesia:dirty_write(Segment,#kv{key = Key,value = Value}).

dirty_delete(Segment,Key)->
  mnesia:dirty_delete(Segment,Key).

%%=================================================================
%%	API
%%=================================================================
start_link(Segment)->
  % The process is registered locally to not to conflict
  % with the processes that handle the same segment on  other nodes.
  % This gives us a more explicit way to address a segment handler
  % of the explicitly defined node and do a load balancing for dirty
  % mode operations
  gen_server:start_link({local,?MODULE}, ?MODULE, [Segment], []).



%%=================================================================
%%	OTP
%%=================================================================
init([Segment])->

  ?LOGINFO("starting segment server for ~p pid ~p",[Segment,self()]),

  Cycle=?ENV(segment_scan_cycle, ?DEFAULT_SCAN_CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{
    segment = Segment,
    cycle = Cycle
  }}.

handle_call(_Params, _From, State) ->
  {reply, {ok,undefined}, State}.


handle_cast({stop,From},State)->
  From!{stopped,self()},
  {stop, normal, State};

handle_cast(_Request,State)->
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(loop,#state{
  segment = Segment,
  cycle = Cycle
}=State)->
  {ok,_}=timer:send_after(Cycle,loop),
  {noreply,State}.

terminate(Reason,#state{segment = Segment})->
  ?LOGINFO("terminating segment server ~p, reason ~p",[Segment,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.