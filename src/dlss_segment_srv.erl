%%----------------------------------------------------------------
%% Copyright (c) 2022 Faceplate
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

-module(dlss_segment_srv).

-include("dlss.hrl").

%%=================================================================
%%	OTP
%%=================================================================
-export([
  start_link/1
]).

%%=================================================================
%%	Subscriptions API
%%=================================================================
-export([
  wait_loop/2,
  subscribe/1, do_subscribe/2,
  unsubscribe/1, do_unsubscribe/2
]).

%%=================================================================
%%	OTP
%%=================================================================
start_link( Segment )->
  case whereis( Segment ) of
    PID when is_pid( PID )->
      {error, {already_started, PID}};
    _ ->

      Worker = spawn_link(?MODULE,wait_loop,[Segment, self()]),
      true = register( Segment, Worker),
      ?LOGINFO("~p register service process ~p",[Segment, self()]),
      {ok, Worker}
  end.

%%=================================================================
%%	Subscriptions API
%%=================================================================
subscribe( Segment )->
  Node = dlss_segment:where_to_write( Segment ),
  if
    Node =:= nowhere -> {error, unavailable};
    Node =:= node()->
      do_subscribe(Segment, self());
    true->
      case rpc:call(Node, ?MODULE, do_subscribe, [ Segment, self() ]) of
        {badrpc, Error} -> {error, Error};
        Result -> Result
      end
  end.

do_subscribe(Segment, ClientPID)->
  case whereis( Segment ) of
    PID when is_pid( PID )->
      PID ! {subscribe, ClientPID},
      receive
        {ok,PID}->
          link(PID),
          ok
      after
        60000->
          PID ! {unsubscribe, ClientPID},
          {error, timeout}
      end;
    _->
      {error, not_registered}
  end.

unsubscribe( Segment )->
  Node = dlss_segment:where_to_write( Segment ),
  if
    Node =:= nowhere -> ok;
    Node =:= node()->
      do_unsubscribe(Segment, self());
    true->
      case rpc:call(Node, ?MODULE, do_unsubscribe, [ Segment, self() ]) of
        {badrpc, Error} -> {error, Error};
        Result -> Result
      end
  end.

do_unsubscribe(Segment, ClientPID)->
  case whereis( Segment ) of
    PID when is_pid( PID )->
      unlink( PID ),
      Segment ! {unsubscribe, ClientPID},
      ok;
    _->
      ok
  end.

wait_loop(Segment, Sup)->
  process_flag(trap_exit,true),
  wait_loop([], Segment, Sup).

wait_loop(Subs, Segment, Sup)->
  receive
    {write, Rec}->
      Update = {subscription, Segment, {write,Rec}},
      [ PID ! Update || PID <- Subs ],
      wait_loop( Subs, Segment, Sup );
    {delete, K}->
      Update = {subscription,Segment, {delete,K}},
      [ PID ! Update || PID <- Subs ],
      wait_loop( Subs, Segment, Sup );
    {subscribe, PID}->
      PID ! {ok, self()},
      wait_loop( [PID | Subs -- [PID]], Segment, Sup);
    {unsubscribe, PID}->
      unlink(PID),
      wait_loop( Subs -- [PID], Segment, Sup);
    {'EXIT',PID, Reason} when PID =/= Sup->
      ?LOGDEBUG("~p subcriber ~p died, reason ~p, remove subscription",[ Segment, PID, Reason ]),
      wait_loop( Subs -- [PID], Segment, Sup);
    {'EXIT',Sup, Reason} when Reason =:= normal; Reason=:=shutdown->
      ?LOGINFO("~p stop service process, reason ~p",[Segment, Reason]);
    {'EXIT',Sup, Reason}->
      ?LOGERROR("~p exit, reason ~p",[Segment, Reason ]);
    Unexpected->
      ?LOGDEBUG("~p got unexpected message ~p",[Segment, Unexpected]),
      wait_loop( Subs, Segment, Sup )
  end.