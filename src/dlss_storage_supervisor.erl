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

-module(dlss_storage_supervisor).

-include("dlss.hrl").

-define(PROCESS(Storage), list_to_atom( atom_to_list(Storage) ++ "_sup" ) ).
-define(DEFAULT_SCAN_CYCLE,5000).
-define(SPLIT_SYNC_DELAY, 3000).
-define(WAIT_LIMIT, 5).

% The encoded @deleted@ value. Actually this is {[],[],'@deleted@'}
% because mnesia_eleveldb uses this format
-define(DELETED, <<131,104,3,106,106,100,0,9,64,100,101,108,101,116,101,100,64>>).

-record(state,{ storage, type, cycle }).
-record(dump,{ version, hash }).


%%=================================================================
%%	The rebalancing algorithm follows the rules:
%%   * There is a supervisor process for each storage on each node
%%   * The transformation procedure is started only by the segment's
%%     master node. The master is the active node with the smallest
%%     name in the list of the nodes that have copies of the segment.
%%   * The transformation procedure is some state of the schema when
%%     there is at least one segment at the floating level.
%%   * To start a transformation the supervisor needs to move the due
%%     segment to the floating level and to increment its version.
%%     ( Split - L.1, Merge - L.9 )
%%   * If there is a segment at the floating level all the supervisors
%%     try to do their part of work to move it to a sustainable level.
%%     After a supervisor has accomplished its operations on a floating
%%     segment it updates its hash in the dlss_schema.
%%   * The transformation procedure can be committed if all the segments
%%     have confirmed their hash.
%%   * If some of nodes has a different form the master node hash it drops
%%     the local copy what leads to complete reloading the segment
%%   * If a segment reaches the limit it splits. A new segment is created
%%     at the same nodes with the level L.1 and the first key same as the
%%     the full (parent) segment
%%   * If a level reaches max number of the segments the first segment in
%%     in the level goes to L.9 level if there are segments in the lower level
%%     they all get the increment of the version. If there are no segments
%%     on the L+1 level yet the merged segment directly goes to the L+1 level
%%
%%=================================================================


%%=================================================================
%%	API
%%=================================================================
-export([
  start/1,
  start_link/1,
  stop/1,
  verify_hash/0, verify_hash/1,
  sync_copies/0
]).

-export([
  rebalance/1,
  get_efficiency/1
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

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  loop/3
]).

-endif.

%%=================================================================
%%	API
%%=================================================================
start( Storage )->
  dlss_schema_sup:start_storage( Storage ).

start_link( Storage )->
  gen_server:start_link({local, ?PROCESS(Storage) }, ?MODULE, [Storage], []).

stop(Storage)->
  case whereis(?PROCESS(Storage)) of
    PID when is_pid(PID)->
      dlss_schema_sup:stop_storage(PID);
    _ ->
      { error, not_started }
  end.

%%============================================================================
%% Verify hash of the segments for the Node
%%============================================================================
verify_hash()->
  do_verify_hash(dlss:get_storages()).

do_verify_hash([])->
  ok;
do_verify_hash( Storages )->
  Results =
    [ case whereis(?PROCESS(S)) of
        undefined -> S;
        _-> gen_server:cast(?PROCESS(S), verify_hash)
      end || S <- Storages],
  do_verify_hash([R || R <- Results, R=/=ok]).

verify_hash(Force)->
  Node = node(),
  Trace = fun(Text,Args)->?LOGINFO(Text,Args) end,
  [ verify_storage_hash(S, Node, Force, Trace) || S <- dlss:get_storages()].

verify_storage_hash(Storage, Node, Force, Trace )->
  Trace("~p verify storage hash: force ~p",[Storage, Force]),
  [ verify_segment_hash(S, Node, Force, Trace) || S <- dlss_storage:get_node_segments(Node, Storage) ],
  ok.

verify_segment_hash(Segment, Node, Force, Trace )->
  Trace("~p verify segment hash: force ~p",[Segment, Force]),
  case dlss_segment:get_info(Segment) of
    #{ local := true } ->
      Trace("~p is local only segment, skip hash verification",[ Segment ]),
      ok;
    #{ nodes := Nodes }->
      case lists:member( Node, Nodes ) of
        false -> Trace("~p has no local copy, skip hash verification",[ Segment ]);
        _->
          case master_node( Segment ) of
            Node -> Trace("~p is the master node for ~p, skip hash verification",[Node,Segment]);
            undefined -> ?LOGWARNING("master node for ~p is not ready, skip hash verification",[Segment]);
            Master ->
              AccessMode = dlss_segment:get_access_mode( Segment ),
              if
                Force =:= true , AccessMode =/= read_only ->
                  ?LOGWARNING("~p is not read only yet, it might have more actual data on other nodes",[Segment]),
                  drop_segment_copy(Segment, Node);
                true ->
                  do_verify_segment_hash(Segment, Master, Node, Force, Trace)
              end
          end
      end
  end.

do_verify_segment_hash(Segment, Master, Node, Force, Trace)->
  Trace("~p verify hash: master ~p, force ~p",[ Segment, Master, Force ]),

  {ok, #{copies:= Copies} } = dlss_storage:segment_params( Segment ),

  case Copies of
    #{Master:=Dump, Node:= Dump}->
      Trace("~p has the same verison with ~p: version ~p",[Segment, Master, dump_version(Dump)]);

    #{Master:=MasterDump,Node:=NodeDump}->
      MasterVersion = dump_version( MasterDump ),
      NodeVersion = dump_version( NodeDump ),
      if
        NodeVersion =:= MasterVersion->
          ?LOGWARNING("~p has different hash of ~p",[Master, Segment]);
        true->
          ?LOGWARNING("~p has different version of ~p: local version ~p, master version ~p",[
            Master, Segment, NodeVersion, MasterVersion
          ])
      end,

      if
        MasterVersion < NodeVersion, Force =/= true->
          ?LOGWARNING("~p has not updated it's version of ~p yet, skip hash verification",[Master, Segment]);
        true->
          % We just drop the local copy as it will copied again during the synchronization
          drop_segment_copy(Segment, Node)
      end;
    #{ Master:=_ }->
      ?LOGWARNING("~p doesn't have a copy of ~p, skip hash verification",[Node, Segment]);
    #{Node:=_}->
      ?LOGWARNING("master ~p doesn't have a copy of ~p, skip hash verification",[Master, Segment]);
    _->
      ?LOGWARNING("neigther ~p nor ~p has a copy of ~p, skip hash verification",[Master, Node, Segment])
  end.

%%============================================================================
%% Obtain/Remove copies of segments of a Storage to the Node
%%============================================================================
sync_copies()->
  Node = node(),
  Trace = fun(Text,Args)->?LOGINFO(Text,Args) end,
  [ sync_copies(S, Node, Trace) || S <- dlss:get_storages()].

sync_copies( Storage, Node, Trace )->
  Trace("~p synchronize storage",[Storage]),
  [ sync_segment_copies(S, Node, Trace) || S <- dlss_storage:get_segments(Storage) ],
  ok.

sync_segment_copies( Segment, Node, Trace)->
  case dlss_segment:get_info( Segment ) of
    #{ local := true }->
      Trace("~p is local only segment, skip synchronization",[ Segment ]);
    #{ nodes := Nodes }->
      {ok, #{copies := Copies} } = dlss_storage:segment_params(Segment),

      case { maps:is_key( Node, Copies ), lists:member(Node,Nodes) } of
        { true, false }->
          % The segment is in the schema but is not on the node yet
          case Nodes -- (Nodes -- dlss:get_ready_nodes()) of
            []-> ?LOGWARNING("~p does not have active copies on other nodes",[Segment]);
            _-> add_segment_copy(Segment, Node)
          end;
        { false, true }->
          % The segment is to be removed from the node
          drop_segment_copy(Segment, Node);
        _->
          Trace("~p is already synchronized with the schema",[Segment])
      end
  end.

add_segment_copy(Segment, Node) ->
  ?LOGINFO("~p add local copy",[ Segment ]),

  % We do add the copy in the locked mode to be sure that the segment is not transformed
  % during the copying
  case dlss_storage:segment_transaction(Segment, write, fun()->
    Master = master_node( Segment ),
    case dlss_segment:add_node( Segment, Node ) of
      ok->
        {ok, #{copies := Copies} } = dlss_storage:segment_params(Segment),
        #{Master := Dump} = Copies,
        {ok,Dump};
      Error -> Error
    end
  end) of
    {ok,Dump} ->

      % The segment is copied successfully, set the version the same as the master has
      ?LOGINFO("~p successfully copied, version ~p",[ Segment, dump_version(Dump) ]),
      ok = dlss_storage:set_segment_version( Segment, Node, Dump );
    {error, Error}->
      ?LOGERROR("~p unable to add local copy, error ~p",[Segment,Error])
  end.

drop_segment_copy(Segment, Node)->
  ?LOGWARNING("~p drop local copy",[ Segment ]),

  % We do remove the copy in the locked mode to be sure that the segment is not transformed
  % during the removing
  case dlss_storage:segment_transaction(Segment, write, fun()->
    dlss_segment:remove_node(Segment, Node)
  end) of
    ok->
      ?LOGINFO("~p successfully removed",[ Segment ]),
      ok;
    {error,Error}->
      ?LOGERROR("~p unable to remove local copy, error ~p",[ Segment, Error ])
  end.


get_efficiency( Storage )->
  eval_segment_efficiency( dlss_storage:root_segment( Storage ) ).

rebalance( Storage )->
  Master = master_node( dlss_storage:root_segment(Storage) ),
  gen_server:cast({?PROCESS(Storage), Master}, rebalance).


%%=================================================================
%%	OTP
%%=================================================================
init([ Storage ])->

  ?LOGINFO("starting storage server for ~p pid ~p",[ Storage, self() ]),

  Type = dlss_storage:get_type( Storage ),
  Cycle=?ENV(storage_supervisor_cycle, ?DEFAULT_SCAN_CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{
    storage = Storage,
    type = Type,
    cycle = Cycle
  }}.

handle_call(Request, From, State) ->
  ?LOGWARNING("unexpected request from ~p, request ~p",[From,Request]),
  {noreply, State}.

handle_cast({stop,From},State)->
  From!{stopped,self()},
  {stop, normal, State};

handle_cast(rebalance, #state{storage = Storage} = State)->
  ?LOGINFO("~p storage trigger rebalance procedure",[Storage]),
  dlss_storage:split_segment( dlss_storage:root_segment( Storage ) ),
  ?LOGINFO("~p storage trigger2 rebalance procedure",[Storage]),
  {noreply, State};

handle_cast(verify_hash, #state{storage = Storage} = State)->
  Trace = fun(Text,Args)->?LOGINFO(Text, Args) end,
  verify_storage_hash(Storage, node(), _Force = true, Trace ),
  {noreply, State};

handle_cast(Request,State)->
  ?LOGWARNING("unexpected request ~p",[Request]),
  {noreply,State}.

%%============================================================================
%%	The loop
%%============================================================================
handle_info(loop,#state{
  storage = Storage,
  cycle = Cycle
}=State)->

  % The loop
  {ok,_}=timer:send_after( Cycle, loop ),

  State1 =
    try loop( State )
    catch
      _:Error:Stack->
        ?LOGERROR("~p storage supervisor error ~p, stack ~p",[ Storage, Error, Stack ]),
        State
    end,

  {noreply, State1}.


terminate(Reason,#state{storage = Storage})->
  ?LOGINFO("terminating storage supervisor ~p, reason ~p",[Storage,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

loop( #state{ storage =  Storage} = State )->

  Node = node(),

  Trace = fun(Text,Args)->?LOGDEBUG(Text, Args) end,

  % Check hash values of storage segments
  verify_storage_hash(Storage, Node, _Force = false, Trace ),

  % Synchronize actual copies configuration to the schema settings
  sync_copies( Storage, Node, Trace),

  % Set read_only mode for low-level segments
  set_read_only_mode(Storage, Node),

  % Check for pending transformation
  case pending_transformation(Storage, Node) of
    {Operation, Segment} ->
      do_transformation(Operation, Segment);
    _ ->
      % Check if a transformation required
      check_limits( Storage, Node )

  end,

  State.


set_read_only_mode(Storage, Node)->
  Root = dlss_storage:root_segment(Storage),

  [ case dlss_segment:get_info( S ) of
      #{ local := true }->
        % Local only storage types are not synchronized between nodes
        ok;
      _->
        case master_node( S ) of
          Node->
            % The node is the master for the segment
            case dlss_segment:get_access_mode(S) of
              read_write ->
                set_segment_read_only( S );
              _ ->
                ignore
            end;
          _->
            % The node is not the master of the segment
            ignore
        end
    end || S <- dlss_storage:get_segments(Storage), S =/= Root ],
  ok.

set_segment_read_only(Segment)->

  % If we set read_only while other nodes copy it
  % mnesia will crash down. So we do it in locked mode
  case dlss_storage:segment_transaction(Segment, write, fun()->
    % WARNING! We need the pause to allow mnesia to settle down it's schema
    timer:sleep(5000),
    dlss_segment:set_access_mode(Segment, read_only)
  end, 1000) of
    ok->
      ?LOGINFO("~p set read_only mode",[ Segment ]);
    {error, lock_timeout}->
      ?LOGINFO("~p lock timeout, skip set read_only mode");
    {error,Error}->
      ?LOGERROR("~p unable to set read_only mode, error ~p",[ Segment, Error ])
  end.

%%============================================================================
%% The transformations
%%============================================================================
pending_transformation( Storage, Node )->
  Segments=
    [ begin
        {ok, P } = dlss_storage:segment_params(S),
        { S, P }
      end || S <- dlss_storage:get_segments(Storage) ],
  case [ {S, P} || {S, #{level:=L} = P}<-Segments, is_float(L) ] of
    [{ Segment, #{level := Level} = Params } | _ ]->
      % The segment is under transformation
      case which_operation( Level ) of
        split ->
          case Params of
            #{ version:=Version, copies := #{ Node := #dump{version = Version}} }->
              % The version for the node is already updated
              {wait, Segment, split};
            #{ copies := #{ Node := _ } }->
              {split, Segment};
            _ ->
              % I'm not engaged. I can do my own work
              undefined
          end;
        merge ->
          {merge, Segment}
      end;
    []->
      % There is no active schema transformations
      undefined
  end.


do_transformation(split, Segment )->

  Node = node(),

  {ok, #{ version:=ChildVersion }} = dlss_storage:segment_params( Segment ),

  Parent = dlss_storage:parent_segment( Segment ),
  {ok, #{
    version:=ParentVersion,
    copies := #{ Node := ParentDump }}
  } = dlss_storage:segment_params( Parent ),


  InitHash = <<>>,
  ?LOGINFO("~p splitting: init size ~s, child ~p init size ~s",[
    Parent, ?PRETTY_SIZE(dlss_segment:get_size(Parent)), Segment, ?PRETTY_SIZE(dlss_segment:get_size(Segment))
  ]),

  case dlss_storage:segment_transaction(Segment, read, fun()->
    {SplitKey,FinalHash} = dlss_copy:split(Parent, Segment,#{ hash => InitHash }),
    ?LOGINFO("~p split finish: split key ~p, final size ~s, child ~p final size ~s, hash ~s, commit...",[
      Parent,SplitKey,?PRETTY_SIZE(dlss_segment:get_size(Parent)),Segment,?PRETTY_SIZE(dlss_segment:get_size(Segment)), ?PRETTY_HASH(FinalHash)
    ]),
    % Update the version of the child segment in the schema
    dlss_storage:set_segment_version( Segment, Node, #dump{hash = FinalHash, version = ChildVersion }),

    % Update the version of the parent segment in the schema
    ParentInitHash =
      case ParentDump of #dump{ hash = Hash0 } -> Hash0;_-> <<>> end,

    ParentHash0 = crypto:hash_update(crypto:hash_init(sha256),ParentInitHash),
    NewParentHash = crypto:hash_update(ParentHash0, FinalHash),
    FinalParentHash = crypto:hash_final( NewParentHash ),

    dlss_storage:set_segment_version( Parent, Node, #dump{hash = FinalParentHash, version = ParentVersion }),

    % Commit the transformation
    split_commit(Segment, SplitKey, master_node( Segment ))
  end) of
    ok -> ok;
    {error,Error}->
      ?LOGERROR("~p split to ~p error ~p",[ Parent, Segment, Error ])
  end;

do_transformation(merge, Segment )->

  {ok, Params} = dlss_storage:segment_params( Segment ),

  #{ key := FromKey} = Params,

  Children =
    [ begin
        {ok,CParams} = dlss_storage:segment_params(C),
        {C,CParams}
      end || C <- dlss_storage:get_children( Segment )],

  [{C0,P0}|Tail] = Children,

  % The first segment takes keys from the parent's head even if they are
  % smaller than its first key
  merge_level( [{C0, P0#{key => FromKey }} | Tail], Segment, Params, node() ).

%---------------------MERGE---------------------------------------------------
merge_level([ {S, #{key:=FromKey, version:=Version, copies:=Copies}}| Tail ], Source, Params, Node )->
  % This is the segment that is currently copying the keys from the source
  case Copies of
    #{ Node := #dump{version = Version} }->
      % The segment is already updated
      merge_level( Tail, Source, Params, Node );
    #{ Node := Dump } when Dump=:=undefined; Dump#dump.version < Version->
      % This node hosts the target segment, it must to play its role
      StartKey =
        if
          FromKey =:= '$start_of_table'->undefined;
          true -> FromKey
        end,
      EndKey =
        case Tail of
          [{_, #{ key := NextKey }}| _]->
            % Copying is not inclusive to end
            NextKey;
          _->
            undefined
        end,
      InitHash=
        case Dump of
          #dump{hash = _H}->_H;
          _-><<>>
        end,
      ?LOGINFO("~p merge to ~p, range ~p : ~p, init size ~s, init hash ~s",[
        Source, S, FromKey, EndKey, ?PRETTY_SIZE(dlss_segment:get_size(S)), ?PRETTY_HASH(InitHash)
      ]),

      % Merge 1 segment at a time
      case dlss_storage:segment_transaction(S,read,fun()->
        FinalHash = dlss_copy:copy(Source,S,#{ hash => InitHash, start_key =>StartKey, end_key => EndKey}),
        ?LOGINFO("~p merged to ~p, range ~p, to ~p, final size ~s, new version ~p, new hash ~s, commit...",[
          Source, S, FromKey, EndKey, ?PRETTY_SIZE(dlss_segment:get_size(S)), Version, ?PRETTY_HASH(FinalHash)
        ]),
        % Update the version of the segment in the schema
        dlss_storage:set_segment_version( S, Node, #dump{hash = FinalHash, version = Version }),

        % Commit the transformation
        merge_commit_child(S, master_node( S ))

      end) of
        ok -> merge_level(Tail, Source, Params, Node);
        {error,{abort,_}}->
          % Segment commit aborted, do not proceed. Let's start the next cycle,
          % do hash verification and copies synchronization and only then proceed.
          % Already committed segments are going to be skipped during the version check
          ok;
        Error -> Error
      end;
    _->
      % The Node doesn't hosts the target segment, check the rest of the level
      merge_level( Tail, Source, Params, Node )
  end;
merge_level([], Source, _Params, _Node )->
  % There is no more locally hosted segments to merge to, final commit.
  merge_commit_final(Source).

%%------------------------------------------------------------
%%  TRANSFORMATION COMMIT
%%------------------------------------------------------------
%%------------------------------------------------------------
%%  Split
%%------------------------------------------------------------
%-----------------Master commit--------------------------------
split_commit(Segment, SplitKey, Master) when Master=:=node()->

  {ok, #{copies:=Copies}} = dlss_storage:segment_params( Segment ),
  Dump =
    #dump{version = Version, hash = Hash} = maps:get( Master, Copies ),
  case not_confirmed( Dump, Copies ) of
    [] ->
      % All are ready
      ?LOGINFO("~p split commit, version ~p, hash ~s",[ Segment, Version, ?PRETTY_HASH(Hash) ]),
      dlss_storage:split_commit( Segment, SplitKey );
    Nodes->
      % There are still nodes that are not confirmed the hash yet
      ?LOGDEBUG("~p splitting is not finished yet, waiting for ~p",[Segment, Nodes]),
      timer:sleep( ?ENV(storage_supervisor_cycle, ?DEFAULT_SCAN_CYCLE) ),
      % Update the master
      split_commit(Segment, SplitKey, master_node( Segment ))
  end;

%--------------------Slave commit-----------------------------------
split_commit(Segment, SplitKey, Master)->

  {ok, #{copies:=Copies}} = dlss_storage:segment_params( Segment ),
  MasterDump =
    #dump{ version = MasterVersion, hash = MasterHash } = maps:get( Master, Copies ),
  MyDump =
    #dump{ version = MyVersion, hash = MyHash } = maps:get( node(), Copies ),

  if
    MyDump =:= MasterDump->
      ?LOGINFO("~p split commit, version ~p, hash ~s",[ Segment, MyVersion, ?PRETTY_HASH(MyHash) ]);
    not is_number(MasterVersion); MyVersion > MasterVersion ->

      % There are still nodes that are not confirmed the hash yet
      ?LOGINFO("~p splitting is not finished yet, waiting for master ~p",[Segment, Master]),
      timer:sleep( ?ENV(storage_supervisor_cycle, ?DEFAULT_SCAN_CYCLE) ),
      % Update the master
      split_commit(Segment, SplitKey, master_node( Segment ));
    true->
      % Something went wrong.
      % The local copy is going to be dropped during hash verification and reloaded during copies synchronization on the next cycle
      ?LOGERROR("~p split abort: has different dump to master ~p, master version ~p, master hash ~s, local version ~p, local hash ~s",[
        Segment, Master, MasterVersion, ?PRETTY_HASH(MasterHash), MyVersion, ?PRETTY_HASH(MyHash)
      ])
  end.

%%------------------------------------------------------------
%%  Merge child commit
%%------------------------------------------------------------
%-----------------Master commit--------------------------------
merge_commit_child(_Segment, Master) when Master =:= node()->
  % I'm the master I do only the final commit
  ok;

%--------------------Slave commit-----------------------------------
merge_commit_child(Segment, Master)->

  {ok, #{copies:=Copies}} = dlss_storage:segment_params( Segment ),
  MasterDump =
    #dump{ version = MasterVersion, hash = MasterHash } = maps:get( Master, Copies ),
  MyDump =
    #dump{ version = MyVersion, hash = MyHash } = maps:get( node(), Copies ),

  if
    MyDump =:= MasterDump->
      ?LOGINFO("~p merge commit, version ~p, hash ~s",[ Segment, MyVersion, ?PRETTY_HASH(MyHash) ]);
    not is_number(MasterVersion); MyVersion > MasterVersion->

      % There are still nodes that are not confirmed the hash yet
      ?LOGDEBUG("~p merging is not finished yet, waiting for master ~p",[Segment, Master]),
      timer:sleep( ?ENV(storage_supervisor_cycle, ?DEFAULT_SCAN_CYCLE) ),
      % Update the master
      merge_commit_child(Segment, master_node( Segment ));
    true->
      % Something went wrong.
      % The local copy is going to be dropped during hash verification and reloaded during copies synchronization on the next cycle
      ?LOGERROR("~p merge abort: has different dump to master ~p, master version ~p, master hash ~s, local version ~p, local hash ~s",[
        Segment, Master, MasterVersion, ?PRETTY_HASH(MasterHash), MyVersion, ?PRETTY_HASH(MyHash)
      ]),

      throw(abort)
  end.

%%------------------------------------------------------------
%%  Merge final commit
%%------------------------------------------------------------
merge_commit_final( Source )->
  case dlss_storage:get_children(Source) of
    {error, not_found} ->
      % If the segment does not exists the master has committed the merge already
      ok;
    Children ->
      % Final commit must do to the master for the last segment
      Last = lists:last( Children),
      merge_commit_final(Source, master_node(Last))
  end.
merge_commit_final(Source, undefined)->
  ?LOGWARNING("~p merge can not not be committed the master is unavailable",[Source]);

%-----------------Master commit--------------------------------
merge_commit_final(Source, Master) when Master =:= node()->
  case confirm_children( dlss_storage:get_children(Source), Source ) of
    [] ->
      % All children are ready
      ?LOGINFO("~p merge final commit",[ Source ]),
      dlss_storage:merge_commit( Source );
    NotConfirmed->
      % There are still nodes that are not confirmed the hash yet
      ?LOGINFO("~p merging is not finished yet, waiting for ~p",[Source, NotConfirmed]),
      timer:sleep( ?ENV(storage_supervisor_cycle, ?DEFAULT_SCAN_CYCLE) ),
      % Update the master
      merge_commit_final( Source )
  end;
%-----------------Slave commit--------------------------------
merge_commit_final(Source, Master)->
  % It's a job for the master. May be I'm even not engaged
  ?LOGDEBUG("~p merge commit wait for master ~p",[Source,Master]).

confirm_children([Segment|Rest], Source)->
  {ok, #{copies:=Copies, version:=SegmentVersion}} = dlss_storage:segment_params( Segment ),
  case master_node( Segment ) of
    undefined ->
      ?LOGWARNING("~p merge to ~p can not be finished, ~p is not available",[Source, Segment, Segment ]),
      % The segment is not available, wait for all holding nodes
      [Segment | confirm_children( Rest, Source ) ];
    Master ->
      case maps:get( Master, Copies ) of
        MasterDump = #dump{ version = SegmentVersion }->
          % Master has updated his version, check for slaves
          case not_confirmed( MasterDump, Copies ) of
            [] ->
              % All copies are confirmed
              confirm_children( Rest, Source );
            Nodes->
              ?LOGINFO("~p merge to ~p is not finished, wait for ~p",[Source,Segment,Nodes]),
              [Segment| confirm_children( Rest, Source )]
          end;
        _->
          % Master has not updated his version yet
          ?LOGINFO("~p merge is not finished, wait for master ~p",[Segment,Master]),
          [Segment| confirm_children( Rest, Source )]
      end
  end;
confirm_children([],_Source)->
  [].

not_confirmed(Dump, Copies0)->

  Copies = maps:to_list(Copies0),

  All = [N|| {N,_} <- Copies],

  Ready = All -- (All -- dlss:get_ready_nodes()),

  Confirmed =
    [N|| {N,D} <- Copies, D=:=Dump],

  Ready -- Confirmed.

%%============================================================================
%% This is the entry point for all rebalancing transformations
%%============================================================================
check_limits( Storage, Node )->
  Segments =
    [ begin
        {ok, P} = dlss_storage:segment_params(S),
        {S, P}
      end || S <- dlss_storage:get_segments(Storage) ],

  % Operations with lower level segments have the priority,
  % therefore reverse the results to start from the lowest level
  Levels = lists:reverse( group_by_levels(Segments) ),

  %-------------Step 1. Check per level limits---------------------------------
  case check_level_limits( Levels, Node ) of
    {Level, Segment} ->
      ?LOGINFO("level ~p has reached the limit, queue merge ~p",[Level, Segment]),
      dlss_storage:merge_segment( Segment ),
      merge;
    undefined ->
      %--------Step 2. Check segments size limits------------------------------
      case check_size_limit( Levels, Node ) of
        {Level, Segment}->
          Size = dlss_segment:get_size( Segment ),
          Limit = segment_level_limit(Storage, Level ),
          ?LOGINFO("~p size ~s level ~p has reached the limit ~s, queue a split",[
            Segment,
            ?PRETTY_SIZE(Size),
            Level,
            ?PRETTY_SIZE(Limit)
          ]),
          dlss_storage:split_segment( Segment ),
          split;
        undefined->
          none
      end
  end.

group_by_levels( Segments )->
  Levels =
    lists:foldl(fun({_,#{level:=L}} = S,Acc)->
      LS = maps:get(L,Acc,[]),
      Acc#{L=>[S|LS]}
    end,#{},Segments),
  lists:usort([{L,lists:reverse(S)}||{L,S} <- maps:to_list(Levels)]).

check_level_limits([{Level, Segments } | Rest], Node )->
  [{ Segment, _Params } | _ ] = Segments,
  case master_node( Segment ) of
    Node->
      % The node is the master for the first segment, it controls the number
      % segments in the level
      Limit = level_count_limit( Level ),
      % Check the limit
      if
        is_integer(Limit), length(Segments) > Limit ->
          % The level has reached the limit on number of segments
          {Level, Segment};
        true ->
          % The level is ok, check the next
          check_level_limits( Rest, Node )
      end;
    _->
      check_level_limits( Rest, Node )
  end;
check_level_limits([], _Node)->
  undefined.

check_size_limit([{Level, Segments} | Rest], Node)->
  case check_segment_size( Segments, Node ) of
    undefined->
      check_size_limit( Rest, Node);
    Segment->
      % The level has an oversized segment
      {Level, Segment}
  end;
check_size_limit([], _Node)->
  undefined.

check_segment_size([{ Segment, #{storage:=Storage, level:=Level }}| Rest], Node)->
  case master_node( Segment ) of
    Node->
      Size = dlss_segment:get_size( Segment ),
      Limit = segment_level_limit(Storage, Level ),
      if
        is_number(Limit), Size > Limit-> Segment;
        true ->
          check_segment_size( Rest, Node )
      end;
    _->
        check_segment_size( Rest, Node )
  end;
check_segment_size([], _Node)->
  undefined.


eval_segment_efficiency( Segment )->

  #{ type:= Type }=dlss_segment:get_info( Segment ),


  % Prepare the deleted flag
  DeletedValue =
    if
      Type=:=disc -> ?DELETED;
      true -> '@deleted@'
    end,

  #{ deleted := Deleted, total := Total, gaps := Gaps }=
    dlss_rebalance:fold(fun({_K, V}, #{
      deleted := D, total := T, gaps := G, prev := P
    } = Acc)->
      X = if V =:= DeletedValue-> 0; true -> 1 end,
      Acc#{
        deleted => if X =:= 0 -> D + 1; true -> D end,
        total => T + 1,
        gaps => if P =:= 1, X =:= 0 ->  G + 1; true -> G end,
        prev => X
      };
      (_Other, Acc) -> Acc
    end, #{
      deleted => 0, total => 0, gaps => 0, prev => 1
    }, Segment),

  if
    Total =:= 0 ->
      ?LOGINFO("~p is empty",[ Segment ]),
      1;
    true ->
      Size = dlss_segment:get_size( Segment ),
      {ok, #{ storage := Storage}} = dlss_storage:segment_params( Segment ),
      Limit = segment_level_limit(Storage, 0),

      if
        Total =:= Deleted ->
          ?LOGINFO("~p has only deleted records",[ Segment ]),
          1 - ( Size / Limit );
        true ->
          AvgRecord = Size / ( Total - Deleted ),
          Capacity = Limit / AvgRecord,
          AvgGap =
            if
              Gaps > 0 -> Deleted / Gaps;
              true -> 0
            end,

          ?LOGINFO("~p statistics: ~p",[ Segment, #{
            size => ?PRETTY_SIZE(Size),
            limit => ?PRETTY_SIZE(Limit),
            total => ?PRETTY_COUNT(Total),
            deleted => ?PRETTY_COUNT(Deleted),
            gaps => ?PRETTY_COUNT(Gaps),
            avg_record => ?PRETTY_SIZE(AvgRecord),
            avg_gap_length => ?PRETTY_COUNT(AvgGap),
            capacity => ?PRETTY_COUNT(Capacity)
          }]),

          % Total efficiency
          (Total - Deleted) / Total
      end
  end.


%%============================================================================
%%	Internal helpers
%%============================================================================
master_node( Segment )->

  {ok, #{copies:= Copies} } = dlss_storage:segment_params( Segment ),

  SegmentNodes = lists:usort([ N || { N, _Dump } <- maps:to_list( Copies ) ]),
  LiveNodes = lists:usort( dlss_segment:get_active_nodes( Segment ) ),

  SegmentLiveNodes = ordsets:intersection( SegmentNodes, LiveNodes ),

  ReadyNodes = dlss:get_ready_nodes(),

  case SegmentLiveNodes -- (SegmentLiveNodes -- ReadyNodes) of
    [ Master | _ ]->
      % The master is the node with the smallest name
      Master;
    _-> undefined
  end.

which_operation(Level)->
  Floor = round(math:floor( Level )),
  if
    round(Level) =:= Floor ->
      % If the level is closer to the upper level then it is the split
      split;
    true ->
      % If the level is closer to lower level, then it is a merge
      merge
  end.

segment_level_limit(Storage, Level )->
  StorageLimits = dlss_storage:storage_limits( Storage ),
  case StorageLimits of
    #{Level:=Limit}->Limit *?MB;
    _-> undefined
  end.

level_count_limit( 0 )->
  % There can be only one root {"B", 0}segment
  1;
level_count_limit( _Level )->
  % A storage always has only 2 levels.
  % We reject using more levels to minimize @deleted@ records
  % which in case of 2-level storage exist only in the root segment
  unlimited.

dump_version(#dump{version = Version}) ->
  Version;
dump_version(_)->
  % The dump is not defined
  0.

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

loop( Storage, Type, _Node)->
  loop( #state{storage = Storage, type = Type } ).

-endif.


