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

-module(dlss_subscription).

-include("dlss.hrl").

%%=================================================================
%% OTP
%%=================================================================
-export([
  start_link/0
]).
%%=================================================================
%% API
%%=================================================================
-export([
  subscribe/2,
  unsubscribe/2,
  notify/2
]).

-define(SUBSCRIPTIONS,dlss_subscriptions).
-define(SUBSCRIBE_TIMEOUT,30000).

-record(sub,{segment,clients}).

%%=================================================================
%% Service API
%%=================================================================
start_link() ->
  case whereis( ?MODULE ) of
    PID when is_pid( PID )->
      {error, {already_started, PID}};
    _ ->
      Sup = self(),
      {ok, spawn_link(fun()-> init(Sup) end)}
  end.

init(Sup)->

  register(?MODULE, self()),

  % Prepare the storage for subscriptions
  ets:new(?SUBSCRIPTIONS,[named_table,protected,set,{keypos, #sub.segment}]),

  process_flag(trap_exit,true),

  ?LOGINFO("start subscriptions server pid ~p",[ self() ]),

  wait_loop(Sup, _Subs = #{ }).


%%=================================================================
%%	Subscriptions API
%%=================================================================

subscribe(Segment, PID)->
  case whereis( ?MODULE ) of
    Server when is_pid( Server )->
      Server ! {subscribe, Segment, _ReplyTo = self() , PID},
      receive
        {ok, Server} -> ok;
        {error,Server,Error}-> {error, Error}
      after
        ?SUBSCRIBE_TIMEOUT->
          {error, timeout}
      end;
    _->
      {error, not_available}
  end.

unsubscribe( Segment, PID )->
  case whereis( ?MODULE ) of
    Server when is_pid( Server )->
      Server ! {unsubscribe, Segment, PID},
      ok;
    _->
      {error, not_available}
  end.

notify( Segment, Action )->
  case ets:lookup(?SUBSCRIPTIONS, Segment) of
    [] ->
      ignore;
    [#sub{ clients = Clients }]->
      [ C ! {subscription, Segment, Action} || C <- Clients ]
  end,
  ok.

%%---------------------------------------------------------------------
%%  Server loop
%%---------------------------------------------------------------------
wait_loop(Sup, Subs)->
  receive
    {subscribe, Segment, ReplyTo, PID}->
      case do_subscribe( Segment, PID ) of
        ok ->
          ?LOGINFO("DEBUG: ~p subscribed on ~p",[PID,Segment]),
          ReplyTo ! {ok, self()},
          case Subs of
            #{ PID := Segments }->
              wait_loop(Sup, Subs#{PID => [Segment|Segments]});
            _->
              link( PID ),
              wait_loop(Sup, Subs#{PID => [Segment]})
          end;
        {error,Error} ->
          ?LOGERROR("~p subscribe on ~p error: ~p",[PID,Segment,Error]),
          PID ! {error,self(),Error},
          wait_loop(Sup, Subs)
      end;
    {unsubscribe, Segment, PID}->
      ?LOGINFO("DEBUG: ~p unsubscribed from ~p",[PID,Segment]),
      do_unsubscribe( Segment, PID ),
      case Subs of
        #{ PID := Segments }->
          case Segments -- [Segment] of
            [] ->
              unlink( PID ),
              wait_loop(Sup, maps:remove( PID, Subs ))
          end;
        _ ->
          wait_loop(Sup, Subs)
      end;
    {'EXIT',PID, Reason} when PID =/= Sup->
      case Subs of
        #{ PID := Segments }->
          ?LOGINFO("DEBUG: ~p subcriber died, reason ~p, remove subscriptions ~p",[ PID, Reason, Segments ]),
          [ do_unsubscribe(S, PID) || S <- Segments ],
          wait_loop(Sup, maps:remove( PID, Subs ));
        _->
          % Who was it?
          wait_loop(Sup, Subs)
      end;
    {'EXIT',Sup, Reason} when Reason =:= normal; Reason=:=shutdown->
      ?LOGINFO("stop subcriptions server, reason ~p",[ Reason]);
    {'EXIT',Sup, Reason}->
      ?LOGERROR("subcriptions server exit, reason ~p",[ Reason ]);
    Unexpected->
      ?LOGDEBUG("subcriptions server got unexpected message ~p",[Unexpected]),
      wait_loop( Subs, Sup )
  end.

do_subscribe(Segment, PID)->
  try case ets:lookup(?SUBSCRIPTIONS,Segment) of
    [#sub{clients = Clients}=S] ->
      true = ets:insert(?SUBSCRIPTIONS,S#sub{ clients = (Clients -- [PID]) ++ [PID] });
    []->
      true = ets:insert(?SUBSCRIPTIONS,#sub{segment = Segment,clients = [PID]})
  end, ok
  catch
    _:Error->{error,Error}
  end.

do_unsubscribe(Segment, PID)->
  case ets:lookup(?SUBSCRIPTIONS, Segment) of
    [#sub{clients = Clients}=S] ->
      case Clients -- [PID] of
        []-> ets:delete(?SUBSCRIPTIONS, Segment );
        RestClients -> ets:insert(?SUBSCRIPTIONS, S#sub{ clients = RestClients })
      end;
    []->
      ignore
  end.
