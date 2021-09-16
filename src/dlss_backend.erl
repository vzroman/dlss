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

-module(dlss_backend).

-include("dlss.hrl").

-behaviour(gen_server).

%%=================================================================
%%	API
%%=================================================================
-export([
  init_backend/0,init_backend/1,
  add_node/1,
  remove_node/1,
  get_nodes/0,
  create_segment/2,
  delete_segment/1,
  transaction/1,sync_transaction/1,
  lock/2
]).

%%=================================================================
%%	OTP
%%=================================================================
-export([
  start_link/0,
  stop/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).


-define(DEFAULT_START_TIMEOUT, 24*3600*1000). % 1 hour.
-define(WAIT_SCHEMA_TIMEOUT,24*3600*1000). % 1 day
-define(ATTACH_TIMEOUT,600000). %10 min.
-define(DEFAULT_MASTER_CYCLE, 1000).

-record(state,{
  cycle
}).

%%=================================================================
%%	API
%%=================================================================
start_link()->
  gen_server:start_link({local,?MODULE},?MODULE, [], []).

% Add a new node to the schema
add_node(Node)->
  case mnesia:change_config(extra_db_nodes,[Node]) of
    {ok,Nodes} when is_list(Nodes)->
      lists:member(Node,Nodes);
    _->
      false
  end.

% Remove a node from the schema
remove_node(Node)->
  mnesia:del_table_copy(schema,Node).

get_nodes()->
  mnesia:system_info(db_nodes).

create_segment(Name,Params)->
  Attributes = table_attributes(Params),
  case mnesia:create_table(Name,[
    {attributes,record_info(fields,kv)},
    {record_name,kv},
    {type,ordered_set}|
    Attributes
  ]) of
    {atomic, ok } -> ok;
    {aborted, Reason } -> {error, Reason}
  end.

delete_segment(Name)->
  case dlss_segment:set_access_mode( Name, read_write ) of
    ok ->
      case mnesia:delete_table(Name) of
        {atomic,ok}->ok;
        {aborted,Reason}-> {error, Reason }
      end;
    SetModeError ->
      SetModeError
  end.

transaction(Fun)->
  % We use the mnesia engine to deliver the true distributed ACID transactions
  case mnesia:transaction(Fun) of
    {atomic,FunResult}->{ok,FunResult};
    {aborted,Reason}->{error,Reason}
  end.

% Sync transaction wait all changes are applied
sync_transaction(Fun)->
  case mnesia:sync_transaction(Fun) of
    {atomic,FunResult}->{ok,FunResult};
    {aborted,Reason}->{error,Reason}
  end.

lock( Item, Lock )->
  mnesia:lock( Item, Lock ).

%%=================================================================
%%	OTP
%%=================================================================
init([])->

  ?LOGINFO("starting backend ~p",[self()]),

  init_backend(),

  dlss_node:set_status(node(),ready),

  Cycle=?ENV(master_node_cycle,?DEFAULT_MASTER_CYCLE),

  % Subscribe to mnesia events
  mnesia:subscribe( system ),

  timer:send_after(Cycle,on_cycle),

  {ok,#state{cycle = Cycle}}.

handle_call(Request, From, State) ->
  ?LOGWARNING("backend got an unexpected call resquest ~p from ~p",[Request,From]),
  {noreply,State}.


handle_cast(Request,State)->
  ?LOGWARNING("backend got an unexpected cast resquest ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
%STOP. Fatal error
handle_info({mnesia_system_event,{mnesia_fatal,Format,Args,_BinaryCore}},_State) ->
  ?LOGERROR("FATAL ERROR: mnesia error. Format ~p, args - ~p",[Format,Args]),
  {stop,mnesia_fatal,Args};

handle_info({mnesia_system_event,Event},State) ->
  on_mnesia_event( Event ),
  {noreply,State};

handle_info(on_cycle, #state{cycle = Cycle} = State)->
  timer:send_after( Cycle, on_cycle ),

  Ready = dlss:get_ready_nodes(),
  Running = mnesia:system_info(running_db_nodes),

  [ dlss_node:set_status(N,down) ||  N <- Ready -- Running ],
  [ dlss_node:set_status(N,ready) ||  N <- Running -- Ready ],

  {noreply,State};

handle_info(Message,State)->
  ?LOGWARNING("backend got an unexpected message ~p",[Message]),
  {noreply,State}.

terminate(Reason,_State)->
  ?LOGINFO("terminating backend reason ~p",[Reason]),
  stop(),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%=================================================================
%%	Backend initialization
%%=================================================================
init_backend()->
  init_backend(#{}).
init_backend(#{
  as_node := AsNode,
  force := IsForced,
  start_timeout := StartTimeout
})->

  % Create mnesia schema
  IsFirstStart=
    case mnesia:create_schema([node()]) of
      ok->
        true;
      {error,{_,{already_exists,_}}}->
        false;
      {error,Error}->
        ?LOGERROR("FATAL! Unable to create the schema ~p",[Error]),
        ?ERROR(Error)
    end,

  % Recover restore form backups interrupted rebalance transactions
  dlss_rebalance:on_init(),

  %% Next steps need the mnesia started
  ?LOGINFO("starting mnesia"),
  % We want to see in the console what's happening
  mnesia:set_debug_level(debug),

  {ok, _} = application:ensure_all_started( mnesia_eleveldb ),

  ?LOGINFO("dlss initalization"),
  if
    IsFirstStart ->
      ?LOGINFO("schema is not defined yet"),
      if
        AsNode ->
          ?LOGINFO("node is starting as slave"),

          ?LOGINFO("restarting mnesia"),
          ok=mnesia:delete_schema([node()]),

          ok=mnesia:start(),
          % Register leveldb backend. !!! Many thanks to Google, Basho and Klarna developers
          mnesia_eleveldb:register(),

          ?LOGINFO("waiting for the schema from the master node..."),
          wait_for_schema(),

          ?LOGINFO("add local only segments"),
          add_local_only_segments(),

          ?LOGINFO("waiting for segemnts availability..."),
          wait_segments(StartTimeout);
        true ->
          ok=mnesia:start(),
          % Register leveldb backend. !!! Many thanks to Google, Basho and Klarna developers
          mnesia_eleveldb:register(),
          ?LOGINFO("node is starting as master"),
          create_schema()
      end;
    true ->
      if
        IsForced ->
          ?LOGWARNING("starting in FORCED mode"),
          set_forced_mode(),

          ?LOGWARNING("restarting mnesia"),
          mnesia:stop(),
          ok=mnesia:start(),

          ?LOGINFO("waiting for schema availability..."),
          ok = mnesia:wait_for_tables([schema,dlss_schema],?ENV(schema_start_timeout, ?WAIT_SCHEMA_TIMEOUT)),

          ?LOGINFO("waiting for segemnts availability..."),
          wait_segments(StartTimeout);
        true ->
          ?LOGINFO("node is starting in normal mode"),
          ok=mnesia:start(),

          ?LOGINFO("waiting for schema availability..."),
          ok = mnesia:wait_for_tables([schema,dlss_schema],?ENV(schema_start_timeout, ?WAIT_SCHEMA_TIMEOUT)),

          ?LOGINFO("add local only segments"),
          add_local_only_segments(),

          ?LOGINFO("waiting for segemnts availability..."),
          wait_segments(StartTimeout)
      end
  end,

  mnesia:set_debug_level(none),
  ?LOGINFO("dlss is ready");

init_backend(Params)->

  % Default backend params
  AsNode=?ENV("AS_NODE",as_node,"false"),
  IsForced=?ENV("FORCE",force,"false"),

  Params1=
    maps:merge(#{
      as_node => (AsNode=:="true") or (AsNode=:=true),
      force => (IsForced=:="true") or (IsForced=:=true),
      start_timeout => ?ENV(start_timeout,?DEFAULT_START_TIMEOUT)
    },Params),

  init_backend(Params1).



create_schema()->
  mnesia:create_table(dlss_schema,[
    {attributes, record_info(fields, kv)},
    {record_name, kv},
    {type,ordered_set},
    {disc_copies,[node()]},
    {ram_copies,[]}
  ]),
  mnesia:change_table_load_order( dlss_schema, 999 ).


stop()->
  % TODO. Why doesn't it stop in the context of the calling process?
  spawn_link(fun()->
    mnesia:stop()
  end).


wait_segments(Timeout)->
  Segments=dlss:get_segments(),
  ?LOGINFO("~p wait for segments ~p",[Timeout,Segments]),
  ok = mnesia:wait_for_tables(Segments,Timeout).

set_forced_mode()->
  case mnesia:set_master_nodes([node()]) of
    ok->ok;
    {error,Error}->
      ?LOGERROR("error set master node ~p, error ~p",[node(),Error]),
      ?ERROR(Error)
  end.

wait_for_schema()->
  % Wait master node to attach this node to the schema
  wait_for_master(),
  % Copy mnesia schema
  case mnesia:change_table_copy_type(schema,node(),disc_copies) of
    {atomic,ok}->ok;
    {aborted,Reason1}->
      ?LOGERROR("unable to copy mnesia schema ~p",[Reason1]),
      ?ERROR(Reason1)
  end,

  % copy dlss schema
  ?LOGINFO("waiting for dlss schema availability..."),
  mnesia:wait_for_tables([dlss_schema],?WAIT_SCHEMA_TIMEOUT),
  case mnesia:add_table_copy(dlss_schema,node(),disc_copies) of
    {atomic,ok}->ok;
    {aborted,Reason2}->
      ?LOGERROR("unable to copy dlss schema ~p",[Reason2]),
      ?ERROR(Reason2)
  end.

add_local_only_segments()->
  [ case dlss_segment:get_info(S) of
      #{local:=true,nodes:=Nodes}->
        case lists:member(node(),Nodes) of
          false->
            case dlss_segment:add_node(S,node()) of
              ok->ok;
              {error,Error}->
                ?LOGERROR("unable to copy local only segment ~p, error ~p",[S,Error]),
                ?ERROR(Error)
            end;
          _->ok
        end;
      _->ok
    end|| S <- dlss_storage:get_segments()],
  ok.


wait_for_master()->
  Node=node(),
  case mnesia:system_info(running_db_nodes) of
    [Node]->
      ?LOGINFO("...waiting attach"),
      timer:sleep(5000),
      wait_for_master();
    Nodes when length(Nodes)>1->
      % If there are more than one node in the schema then the schema has arrived
      ok
  end.

on_mnesia_event({inconsistent_database, Context, Node})->
  ?LOGERROR("mnesia inconsistent database: context ~p, node - ~p",[Context,Node]),

  OnPartitioning=
    case ?ENV(on_partitioning,none) of
      none->
        ?LOGINFO("use default partitioning algorithm"),
        fun default_partitioning/1;
      {Module,Method}->
        case is_exported(Module,Method) of
          {ok,ExternalHandler}->
            ?LOGINFO("use external partitioning algorithm ~p",[{Module,Method}]),
            ExternalHandler;
          {error,HandlerError}->
            ?LOGERROR("invalid on partitioning handler ~p, error ~p",[{Module,Method},HandlerError]),
            ?LOGINFO("use default partitioning algorithm"),
            fun default_partitioning/1
        end
    end,

  OnPartitioning( Node );

on_mnesia_event({mnesia_down, Node})->
  ?LOGWARNING( "~p node is down", [Node] ),
  try
    dlss_node:set_status( Node, down )
  catch
    _:_->?LOGWARNING("unable to set node status to 'down'")
  end;
on_mnesia_event({mnesia_up, Node})->
  ?LOGINFO( "~p node is up", [Node] );


on_mnesia_event({mnesia_overload, Details})->
  ?LOGWARNING("mnesia overload ~p",[Details]);

on_mnesia_event({mnesia_error,Format,Args})->
  ?LOGERROR("mnesia error: format ~p, args - ~p",[Format,Args]);

on_mnesia_event({mnesia_info,Format,Args})->
  ?LOGINFO("mnesia info: format ~p, args - ~p",[Format,Args]);

on_mnesia_event(Other)->
  ?LOGINFO("mnesia event: ~p",[Other]).



table_attributes(#{
  type:=Type,
  nodes:=Nodes,
  local:=IsLocal
})->
  TypeAttr=
    case Type of
      ram->[
        {disc_copies,[]},
        {ram_copies,Nodes}
      ];
      ramdisc->[
        {disc_copies,Nodes},
        {ram_copies,[]}
      ];
      disc->
        [{leveldb_copies,Nodes}]
    end,

  LocalContent=
    if
      IsLocal->[{local_content,true}];
      true->[]
    end,
  TypeAttr++LocalContent.


default_partitioning( Node )->

  % If this node has a smaller name than Node then it reloads all
  % the shared data from Node
  ThisNode = node(),
  NeedsReload=ThisNode > Node,

  if
    NeedsReload ->
      % We just drop local copies of all shared with Node segment
      ?LOGWARNING("The node ~p is selected as the master, drop the local copy of data and reload it...",[Node]),

      % Get local versions of the segments
      Local =
        maps:from_list([ begin
            {ok,P} = dlss_storage:segment_params( S ),
            {S, P}
          end || S <- dlss_storage:get_segments()]),

      ?LOGINFO("reload dlss schema"),
      ok = drop_segment( dlss_schema, ThisNode ),
      ok = dlss_segment:add_node( dlss_schema, ThisNode ),

      ?LOGINFO("drop local copies of shared segments"),
      [case dlss_storage:segment_params( S ) of
         {ok, #{local:=false, copies:= #{ ThisNode:=_, Node:=_ }, level := Level } = P } ->
           % This is a shared with Node segment.
           IsUpdated=
             if
               Level=:=0->
                 % The root segment is always reloaded
                 true;
               true ->
                 case maps:get( S, Local, none ) of
                   P->
                    % The segment hasn't changed
                    false;
                   _->
                     true
                 end
             end,

           % if the segment has changed we drop the local copy of it that will be reloaded
           % by the storage supervisor later
           if
             IsUpdated ->
               ?LOGWARNING("drop the local copy of ~p",[ S ]),
               drop_segment(S, ThisNode);
             true ->
               ignore
           end;
         _->
           ignore
      end || S <- dlss_storage:get_segments()],
     ok;
    true ->
      % This node is the master
      ?LOGINFO("This node is selected as master, node ~p is expected to reload the shared data"),
      ok
  end.

drop_segment(Segment, Node)->
  Active = mnesia:table_info(Segment, active_replicas),
  case Active -- [Node] of
    [_|_]->
      dlss_segment:remove_node( dlss_schema, Node );
    _->
      timer:sleep(1000),
      drop_segment( Segment, Node )
  end.

is_exported(Module,Method)->
  case module_exists(Module) of
    false->{error,invalid_module};
    true->
      case erlang:function_exported(Module,Method,1) of
        false->{error,invalid_function};
        true->{ok,fun Module:Method/1}
      end
  end.

%% Utility for checking if the module is available
module_exists(Module)->
  case code:is_loaded(Module) of
    {file,_}->true;
    _->
      case code:load_file(Module) of
        {module,_}->true;
        {error,_}->false
      end
  end.


