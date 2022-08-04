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
  env/0, env/1,
  add_node/1,
  remove_node/1,
  get_nodes/0,
  get_active_nodes/0,
  transaction/1,sync_transaction/1,
  lock/2,
  purge_stale_segments/0
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
-define(SEGMENT_REMOVE_DELAY, 5 * 60000). % 5 minutes

-record(state,{
  cycle,
  to_delete
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

get_active_nodes()->
  mnesia:system_info(running_db_nodes).

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

  Logger = spawn_link(fun log_init/0),

  init_backend(),

  Logger!finish,

  Cycle=?ENV(master_node_cycle,?DEFAULT_MASTER_CYCLE),

  % Subscribe to mnesia events
  mnesia:subscribe( system ),

  timer:send_after(Cycle,on_cycle),

  {ok,#state{cycle = Cycle, to_delete = #{}}}.

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

handle_info(on_cycle, #state{cycle = Cycle, to_delete = ToDelete} = State)->
  timer:send_after( Cycle, on_cycle ),

  % If some other node reset my status
  case dlss_node:get_status( node() ) of
    ready -> ok;
    _ -> dlss_node:set_status(node(),ready)
  end,

  % Do the operations in the safe mode
  ToDelete1 =
    try

      _ToDelete1 = purge_stale_segments( ToDelete ),

      Ready = dlss:get_ready_nodes(),
      Running = mnesia:system_info(running_db_nodes),

      [ dlss_node:set_status(N,down) ||  N <- Ready -- Running ],

      _ToDelete1
    catch
      _:Error:Stack->
        ?LOGERROR("Internal error: ~p, stack ~p",[Error, Stack])
    end,

  {noreply,State#state{ to_delete = ToDelete1 }};

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

  % Recover restore from backups interrupted rebalance transactions
  dlss_rebalance:on_init(),

  %% Next steps need the mnesia started
  ?LOGINFO("starting mnesia"),

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

      ?LOGINFO("mark read_write segments to skip network loading"),
      mark_read_write_segments(),

      if
        IsForced ->
          ?LOGWARNING("starting in FORCED mode"),
          set_forced_mode( true ),

          ?LOGWARNING("starting mnesia..."),
          ok=mnesia:start(),

          ?LOGINFO("load data..."),
          load_data(StartTimeout),

          ?LOGWARNING("trigger hash verification on other nodes"),
          [ dlss:verify_hash( N ) || N <- dlss:get_ready_nodes() -- [node()]],
          ok;
        true ->
          ?LOGINFO("node is starting in normal mode"),

          ok=mnesia:start(),

          ?LOGINFO("load data..."),
          load_data(StartTimeout)
      end
  end,

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

load_data(StartTimeout)->
  ?LOGINFO("waiting for schema availability..."),
  ok = wait_for_tables([schema,dlss_schema],StartTimeout),

  dlss_node:set_status(node(),down),

  ?LOGINFO("add local only segments"),
  add_local_only_segments(),

  ?LOGINFO("waiting for segemnts availability..."),
  wait_segments(StartTimeout),

  set_forced_mode( false ),

  ?LOGINFO("segments synchronization...."),
  synchronize_segments(),

  ?LOGINFO("purge stale segments...."),
  purge_stale_segments(),

  dlss_node:set_status(node(),ready),

  ok.

env()->
  maps:from_list([{E,mnesia_monitor:get_env(E)} || E <- mnesia_monitor:env() ]).

env( Settings ) when is_map( Settings )->
  env( maps:to_list(Settings) );
env( Settings ) when is_list( Settings )->
  ?LOGINFO( "set dlss backend env ~p",[ Settings ]),
  [mnesia_monitor:set_env(E,V) || {E,V} <- Settings],
  ok.


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

mark_read_write_segments()->
  % We need to mark local read_write segment to load from disc
  % as they are going to be reloaded during hash verification

  [ case atom_to_binary(T,utf8) of
      <<"dlss_schema">> ->
        ignore;
      <<"dlss_",_/binary>> when Access =/= read_only ->
        ?LOGWARNING("~p is marked to skip network loading",[T]),
        set_master_nodes(T,[node()]);
      _->
        ignore
    end ||  {T,Access,_} <- get_regesterd_tables()],

  ok.

synchronize_segments()->

  ?LOGINFO("verify hash values for hosted storages"),
  dlss_storage_supervisor:verify_hash(_Force = true),

  ?LOGINFO("run data synchronization"),
  dlss_storage_supervisor:sync_copies(),

  ok.

wait_segments(Timeout)->
  Segments=dlss:get_local_segments(),
  ?LOGINFO("~p wait for segments ~p",[Timeout,Segments]),
  ok = wait_for_tables(Segments,Timeout).

set_forced_mode( true )->
  case get_regesterd_tables() of
    {error,Error} ->
      ?LOGERROR("unable to parse schema ~p",[Error]),
      ?ERROR(Error);
    Tables->
      Nodes = get_registered_nodes( Tables ),
      ?LOGINFO("registered nodes: ~p",[Nodes]),

      ActiveNodes = confirm_active_nodes( Nodes --[node()], [] ),
      ?LOGINFO("active nodes: ~p, inactive: ~p",[ActiveNodes, (Nodes--[node()]) -- ActiveNodes]),

      if
        length(ActiveNodes) =:= 0 ->
          ?LOGINFO("all tables are going to be loaded from disc"),
          set_master_nodes();
        true ->
          [ case ordsets:intersection(ActiveNodes,Copies) of
              []->
                ?LOGWARNING("~p doesn't have active nodes and will be loaded from disc",[T]),
                set_master_nodes(T,[node()]);
              Masters->
                ?LOGINFO("~p is going to be loaded from ~p",[T,Masters]),
                set_master_nodes(T,Masters)
          end|| {T,_AccessMode,Copies} <- Tables]
      end
  end,

  ok;

set_forced_mode( false )->
  case mnesia:set_master_nodes([]) of
    ok->ok;
    {error,Error}->
      ?LOGERROR("error reset master nodes, error ~p",[Error]),
      ?ERROR(Error)
  end.

wait_for_tables(Tables, Timeout)->
  case mnesia:wait_for_tables(Tables,Timeout) of
    ok -> ok;
    {timeout,Tables1}->
      ?LOGWARNING("timeout on waiting for tables ~p",[Tables1]),
      Text = "if some nodes that have copies of tables were alive when the node stopped"
        ++"\r\n they might have more actual data. If they are not available now you can"
        ++"\r\n try to load in FORCED then that data will be lost."
        ++"\r\n to start in FORED mode set the environment variable:"
        ++"\r\n\t env FORCE=true <start command>",
      ?LOGINFO(Text),
      ?LOGINFO("retry..."),
      wait_for_tables(Tables1, Timeout);
    {error, Error}->
      ?LOGERROR("error on waiting for tables ~p",[Error]),
      ?ERROR(Error)
  end.

get_regesterd_tables()->
  mnesia_lib:lock_table(schema),

  Result =
    case mnesia_schema:read_cstructs_from_disc() of
      {ok, Cstructs} ->
        lists:foldl(fun(Cs,Acc)->
          Copies = mnesia_lib:copy_holders(Cs),
          case lists:member(node(),Copies) of
            true -> [{
              _Name = element(2,Cs),
              _AcessMode = element(8,Cs),
              ordsets:from_list(Copies)
            } | Acc];
            _ -> Acc
          end
        end,[], Cstructs );
      Error->
        Error
    end,

  mnesia_lib:unlock_table(schema),

  Result.

get_registered_nodes(Tables)->
  lists:foldl(fun({Table,_AccessMode,Copies},Acc)->
    ?LOGINFO("~p has copies ~p",[Table, Copies]),
    ordsets:union(Acc,Copies)
  end,ordsets:new(),Tables).

confirm_active_nodes([Node|Rest], Acc)->
  case net_adm:ping(Node) of
    pong-> confirm_active_nodes(Rest,[Node|Acc]);
    _-> confirm_active_nodes(Rest,Acc)
  end;
confirm_active_nodes([], Acc)->
  lists:reverse(Acc).

set_master_nodes()->
  case mnesia:set_master_nodes([node()]) of
    ok->ok;
    {error, Error}->
      ?LOGERROR("error set master nodes, error ~p",[Error]),
      ?ERROR(Error)
  end.

set_master_nodes(Table, Nodes)->
  case mnesia:set_master_nodes(Table,Nodes) of
    ok->ok;
    {error, Error}->
      ?LOGERROR("~p unable to set master nodes, error ~p",[Table,Error]),
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

default_partitioning( Node )->

  % If this node has a smaller name than Node then it reloads all
  % the shared data from Node
  ThisNode = node(),

  %TODO. Evaluate which node is cheaper to reboot:
  % 1. Which node has less segments that has only 1 active copy
  % 2. Which node has less segments
  % 3. Which node has a greater name
  % Only 3 is implemented at the moment
  NeedsReload=ThisNode > Node,

  if
    NeedsReload ->
      % TODO.
      % 1. Halt all mnesia operations be mecking all methods in mnesia module with
      %     method(...Args)-> timer:sleep(1000), method(...Args) end.
      % 2. restart mnesia with mnesia:stop(), mnesia:start()
      % 3. unmeck mnesia
      % 4. verify hash for segments
      % 5. sync copies of segments

      ?LOGERROR("-------------THE NODE GOES INTO REBOOT BECAUSE OF NETWORK PARTITIONING WITH ~p-----------",[Node]),
      init:reboot();
    true ->
      % This node is the master
      ?LOGINFO("This node is selected as master, node ~p is expected to reload the shared data",[Node]),
      ok
  end.

purge_stale_segments()->
  TimeOut = erlang:system_time(millisecond) - 1000,

  ToDelete =
    maps:from_list([{S,TimeOut} || S <- dlss_segment:get_local_segments()] ),

  purge_stale_segments(ToDelete).


purge_stale_segments( ToDelete ) ->

  Node = node(),
  Delay = ?ENV(segment_delay_timeout,?SEGMENT_REMOVE_DELAY),
  erlang:system_time(millisecond),
  TS = erlang:system_time(millisecond),
  ReadyNodes = ordsets:from_list( dlss:get_ready_nodes() ),

  MnesiaTables = dlss_segment:get_local_segments(),
  case transaction(fun()->
    % Get schema info in the locked mode
    lock({table,dlss_schema},read),

    [{T,dlss_storage:segment_params( T )} || T <- MnesiaTables]
  end) of
    {ok, Segments} ->
      lists:foldl(fun({T, Params}, Acc)->
        case Params of
          { error, not_found } ->
            % The segment does not belong to the schema
            case get_segment_nodes( T ) of
              Nodes when is_list( Nodes )->
                case ordsets:from_list(Nodes) -- ReadyNodes of
                  [] ->
                    % All segment nodes are ready
                    case ordsets:intersection( ordsets:from_list(Nodes), ReadyNodes ) of
                      [Node|_] ->
                        % This is the master node for the segment
                        TimeOut = maps:get(T, ToDelete, TS + Delay ),
                        if
                          TS > TimeOut ->
                            ?LOGINFO("removing stale segment ~p",[T]),
                            case dlss_segment:remove( T ) of
                              ok ->
                                ?LOGINFO("segment ~p was removed succesfully",[T]),
                                Acc;
                              {error, Error}->
                                ?LOGDEBUG("unable to remove stale segment ~p, error ~p",[ T, Error ]),
                                Acc#{ T => TimeOut }
                            end;
                          true ->
                            Acc#{ T => TimeOut }
                        end;
                      _ ->
                        % This node is not the master for the segment, the master will delete it
                        Acc
                    end;
                  WaitForNodes->
                    ?LOGDEBUG("unable to remove stale segment ~p, ~p nodes are not ready",[ T, WaitForNodes ]),
                    Acc

                end;
              _ ->
                % The segment info is not available, it might be already removed by the master
                Acc
            end;
          _ ->
            % The segment does not belong to the storage
            Acc
        end
      end, #{}, Segments );
    {error, Error}->
      ?LOGERROR("unable to get segments from schema ~p",[Error]),
      ToDelete
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


log_init()->

  % We want to see in the console what's happening
  mnesia:set_debug_level(debug),

  log_init_subscribe(),

  log_init_loop().

log_init_subscribe()->
  case mnesia:subscribe( system ) of
    {ok,_}->
      ok;
    _->
      timer:sleep(1),
      log_init_subscribe()
  end.

log_init_loop()->
  receive
    {mnesia_system_event,Event} ->
      case Event of
        {Type,Text,Args}->
          ?LOGINFO("~p:"++Text,[Type|Args]);
        _->
          ?LOGINFO("init: report system event ~p",[Event])
      end,
      log_init_loop();
    finish->
      mnesia:set_debug_level(none),
      ok;
    _->
      log_init_loop()
  end.

get_segment_nodes( Segment )->
  try
    #{ nodes := Nodes } = dlss_segment:get_info( Segment ),
    Nodes
  catch
    _:Error ->
      % The segment must be already removed
      ?LOGWARNING("unable to get info on the ~p segment, error ~p",[Segment, Error]),
      error
  end.
