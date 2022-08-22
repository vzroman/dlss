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

-module(dlss_rpc).

-include("dlss.hrl").
-include("dlss_schema.hrl").

%%=================================================================
%% API
%%=================================================================
-export([
  call_one/4,
  call_any/4,
  call_all/4,

  cast_one/4,
  cast_any/4,
  cast_all/4
]).

%-------------CALL------------------------------------------
call_one([],_M,_F,_As) ->
  {error,none_is_available};
call_one(Ns,M,F,As) ->
  call_one(Ns,M,F,As,[]).

call_one([],_M,_F,_As,Errors)->
  {error,Errors};
call_one( Ns,M,F,As,Errors)->
  N = ?RAND( Ns ),
  case rpc:call(N, M, F, As) of
    {badrpc, Error} ->
      ?LOGINFO("DEBUG: ~p call ~p:~p(~p) error ~p",[N,M,F,As,Error]),
      call_one( Ns --[N], M, F, As, [{N,Error}|Errors]);
    Result -> Result
  end.

call_any([],_M,_F,_As)->
  {error,none_is_available};
call_any(Ns,M,F,As)->
  Results =
    [case rpc:call(N, M, F, As) of
       {badrpc, Error} ->
         ?LOGINFO("DEBUG: ~p call ~p:~p(~p) error ~p",[N,M,F,As,Error]),
         {error,Error};
       Result ->
         {ok,Result}
     end|| N <- Ns],

  case [Res || {ok,Res} <- Results] of
    [Result|_]-> Result;
    _->{error, lists:zip(Ns,[E || {error,E} <- Results ]) }
  end.

call_all([],_M,_F,_As)->
  {error,none_is_available};
call_all(Ns,M,F,As)->
  Results =
    [case rpc:call(N, M, F, As) of
       {badrpc, Error} ->
         ?LOGINFO("DEBUG: ~p call ~p:~p(~p) error ~p",[N,M,F,As,Error]),
         {error,Error};
       Result ->
         {ok,Result}
     end || N <- Ns],
  case [Res || {ok,Res} <- Results] of
    _Results when length(_Results)=:= length(Results)->
      {ok, lists:zip(Ns,_Results)};
    _->
      {error, lists:zip(Ns,[E || {error,E} <- Results ]) }
  end.

%-------------CAST------------------------------------------
cast_one([],_M,_F,_As)->
  {error,none_is_available};
cast_one(Ns,M,F,As)->
  rpc:cast(?RAND(Ns), M, F, As),
  ok.

cast_any([],_M,_F,_As)->
  {error,none_is_available};
cast_any(Ns,M,F,As)->
  [ rpc:cast(N, M, F, As) || N <- Ns ],
  ok.

cast_all(Ns,M,F,As)->
  cast_any(Ns,M,F,As).


