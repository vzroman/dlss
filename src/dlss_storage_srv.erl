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

-module(dlss_storage_srv).

-behaviour(gen_server).

-include("dlss.hrl").

-define(CYCLE,5000).
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
  start_link/1,
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
start_link( Storage )->
  gen_server:start_link({local, Storage }, ?MODULE, [Storage], []).

%%============================================================================
%% Verify hash of the segments for the Node
%%============================================================================
verify_hash()->
  do_verify_hash(dlss:get_storages()).

do_verify_hash([])->
  ok;
do_verify_hash( Storages )->
  Results =
    [ case whereis(S) of
        undefined -> S;
        _-> gen_server:cast(S, verify_hash)
      end || S <- Storages],
  do_verify_hash([R || R <- Results, R=/=ok]).

verify_hash(Force)->
  Trace = fun(Text,Args)->?LOGINFO(Text,Args) end,
  [ verify_storage_hash(S, Force, Trace) || S <- dlss:get_storages()].

verify_storage_hash(Storage, Force, Trace )->
  Trace("~p verify storage hash: force ~p",[Storage, Force]),
  [ verify_segment_hash(S, Force, Trace) || S <- dlss_storage:get_node_segments(node(), Storage) ],
  ok.

verify_segment_hash(Segment, Force, Trace )->
  Trace("~p verify segment hash: force ~p",[Segment, Force]),
  case dlss_segment:get_info(Segment) of
    #{ local := true } ->
      Trace("~p is local only segment, skip hash verification",[ Segment ]),
      ok;
    #{ nodes := Nodes }->
      case lists:member( node(), Nodes ) of
        false -> Trace("~p has no local copy, skip hash verification",[ Segment ]);
        _->
          case master_node( Segment ) of
            Node when Node =:= node()->
              Trace("~p is the master node for ~p, skip hash verification",[Node,Segment]);
            undefined ->
              ?LOGWARNING("master node for ~p is not ready, skip hash verification",[Segment]);
            Master ->
              AccessMode = dlss_segment:access_mode( Segment ),
              if
                Force =:= true , AccessMode =/= read_only ->
                  ?LOGWARNING("~p is not read only yet, it might have more actual data on other nodes",[Segment]),
                  drop_segment_copy( Segment );
                true ->
                  do_verify_segment_hash(Segment, Master, Force, Trace)
              end
          end
      end
  end.

do_verify_segment_hash(Segment, Master, Force, Trace)->
  Trace("~p verify hash: master ~p, force ~p",[ Segment, Master, Force ]),

  #{copies:= Copies} = dlss_storage:segment_params( Segment ),
  Node = node(),
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
          drop_segment_copy(Segment)
      end;
    #{ Master:=_ }->
      ?LOGWARNING("~p no local copy, skip hash verification",[ Segment ]);
    #{Node:=_}->
      ?LOGWARNING("master ~p doesn't have a copy of ~p, skip hash verification",[Master, Segment]);
    _->
      throw(internal_error)
  end.

%%============================================================================
%% Obtain/Remove copies of segments of a Storage to the Node
%%============================================================================
sync_copies()->
  Trace = fun(Text,Args)->?LOGINFO(Text,Args) end,
  [ sync_copies(S, Trace) || S <- dlss:get_storages()].

sync_copies( Storage, Trace )->
  Trace("~p synchronize storage",[Storage]),
  [ sync_segment_copies(S, Trace) || S <- dlss_storage:get_segments(Storage) ],
  ok.

sync_segment_copies( Segment, Trace)->
  case dlss_segment:get_info( Segment ) of
    #{ local := true }->
      Trace("~p is local only segment, skip synchronization",[ Segment ]);
    #{ nodes := Nodes }->
      #{copies := Copies} = dlss_storage:segment_params(Segment),

      case { maps:is_key( node(), Copies ), lists:member(node(),Nodes) } of
        { true, false }->
          % The segment is in the schema but is not on the node yet
          case Nodes -- (Nodes -- dlss:get_ready_nodes()) of
            []-> ?LOGWARNING("~p does not have active copies on other nodes",[Segment]);
            _-> add_segment_copy(Segment)
          end;
        { false, true }->
          % The segment is to be removed from the node
          drop_segment_copy(Segment);
        _->
          Trace("~p is already synchronized with the schema",[Segment])
      end
  end.

add_segment_copy(Segment) ->
  ?LOGINFO("~p add local copy, lock...",[ Segment ]),

  Master = master_node( Segment ),
  case dlss_segment:add_copy( Segment ) of
    ok->
      #{copies := Copies} = dlss_storage:segment_params(Segment),
      #{Master := Dump} = Copies,
      % The segment is copied successfully, set the version the same as the master has
      ?LOGINFO("~p successfully copied, version ~p",[ Segment, dump_version(Dump) ]),
      ok = dlss_storage:set_segment_version( Segment, node(), Dump );
    {error, Error}->
      ?LOGERROR("~p add local copy error ~p",[Segment,Error])
  end.

drop_segment_copy(Segment)->
  ?LOGWARNING("~p drop local copy",[ Segment ]),

  case dlss_segment:remove_copy(Segment) of
    ok->
      ?LOGINFO("~p successfully removed",[ Segment ]);
    {error,Error}->
      ?LOGERROR("~p unable to remove local copy, error ~p",[ Segment, Error ])
  end.


get_efficiency( Storage )->
  eval_segment_efficiency( dlss_storage:root_segment( Storage ) ).

rebalance( Storage )->
  Master = master_node( dlss_storage:root_segment(Storage) ),
  gen_server:cast({Storage, Master}, rebalance).


%%=================================================================
%%	OTP
%%=================================================================
init([ Storage ])->

  ?LOGINFO("~p: starting storage server pid ~p",[ Storage, self() ]),

  Type = dlss_storage:get_type( Storage ),
  Cycle=?ENV(storage_supervisor_cycle, ?CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{
    storage = Storage,
    type = Type,
    cycle = Cycle
  }}.

handle_call(Request, From, State) ->
  ?LOGWARNING("storage serbvr got unexpected call request from ~p, request ~p",[From,Request]),
  {noreply, State}.

handle_cast(rebalance, #state{storage = Storage} = State)->
  ?LOGINFO("~p storage trigger rebalance procedure",[Storage]),
  dlss_storage:split_segment( dlss_storage:root_segment( Storage ) ),
  ?LOGINFO("~p storage trigger2 rebalance procedure",[Storage]),
  {noreply, State};

handle_cast(verify_hash, #state{storage = Storage} = State)->
  Trace = fun(Text,Args)->?LOGINFO(Text, Args) end,
  verify_storage_hash(Storage, _Force = true, Trace ),
  {noreply, State};

handle_cast(Request,State)->
  ?LOGWARNING("storage server got unexpected  cast request ~p",[Request]),
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
        ?LOGERROR("~p storage server error ~p, stack ~p",[ Storage, Error, Stack ]),
        State
    end,

  {noreply, State1};

handle_info({subscription,_,_},State)->
  % Late tail subscription updates during active copying
  {noreply, State};
handle_info(Unexpected,State)->
  ?LOGWARNING("storage server got unexpected  info message ~p",[Unexpected]),
  {noreply, State}.



terminate(Reason,#state{storage = Storage})->
  ?LOGINFO("terminating storage server ~p, reason ~p",[Storage,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

loop( #state{ storage =  Storage} = State )->

  Trace = fun(Text,Args)->?LOGDEBUG(Text, Args) end,

  % Check hash values of storage segments
  verify_storage_hash(Storage, _Force = false, Trace ),

  % Synchronize actual copies configuration to the schema settings
  sync_copies( Storage, Trace),

  % Set read_only mode for low-level segments
  set_read_only_mode(Storage),

  % Check for pending transformation
  case pending_transformation(Storage) of
    {Operation, Segment} ->
      do_transformation(Operation, Segment);
    _ ->
      % Check if a transformation required
      check_limits( Storage )

  end,

  State.


set_read_only_mode(Storage)->
  Root = dlss_storage:root_segment(Storage),

  [begin
     #{nodes := Nodes} = dlss_segment:get_info( S ),
     case master_node( S ) of
       Master when Master =:= node()->
         % The node is the master for the segment
         case dlss_segment:access_mode(S) of
           read_write ->
             case Nodes -- dlss:get_ready_nodes() of
               []->
                 set_segment_read_only( S );
               NotReadyNodes->
                 ?LOGINFO("~p copies at ~p are not available, skip set read only mode",[S,NotReadyNodes])
             end;
           _ ->
             ignore
         end;
       _->
         % The node is not the master of the segment
         ignore
     end
   end || S <- dlss_storage:get_segments(Storage), S =/= Root],
  ok.

set_segment_read_only(Segment)->
  case dlss_segment:access_mode(Segment, read_only) of
    ok ->
      ?LOGINFO("~p set read_only mode",[ Segment ]);
    {error,Error}->
      ?LOGERROR("~p unable to set read_only mode, error ~p",[ Segment, Error ])
  end.

%%============================================================================
%% The transformations
%%============================================================================
pending_transformation( Storage )->

  Segments=
    [ { S, dlss_storage:segment_params(S) } || S <- dlss_storage:get_segments(Storage) ],

  case [ {S, P} || {S, #{level:=L} = P}<-Segments,
    is_float(L), % Segment is queued for transformation
    read_only =:= dlss_segment:access_mode(S)
  ] of
    [{ Segment, #{level := Level} = Params } | _ ]->
      % The segment is under transformation
      case which_operation( Level ) of
        split ->
          Node = node(),
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

  #{ version:=ChildVersion } = dlss_storage:segment_params( Segment ),

  Parent = dlss_storage:parent_segment( Segment ),
  #{
    version:=ParentVersion,
    copies := #{ Node := ParentDump }
  } = dlss_storage:segment_params( Parent ),


  InitHash = <<>>,

  ?LOGINFO("~p splitting: init size ~s, child ~p init size ~s",[
    Parent, ?PRETTY_SIZE(dlss_segment:get_size(Parent)), Segment, ?PRETTY_SIZE(dlss_segment:get_size(Segment))
  ]),

  {SplitKey,FinalHash} = dlss_segment:split(Parent, Segment, InitHash),
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
  split_commit(Segment, SplitKey, master_node( Segment ));

do_transformation(merge, Segment )->

  Params = dlss_storage:segment_params( Segment ),

  #{ key := FromKey} = Params,

  Children =
    [ {C,dlss_storage:segment_params(C)} || C <- dlss_storage:get_children( Segment )],

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
      EndKey =
        case Tail of
          [{_, #{ key := NextKey }}| _]->
            % Copying is not inclusive to end
            NextKey;
          _->
            '$end_of_table'
        end,
      InitHash=
        case Dump of
          #dump{hash = _H}->_H;
          _-><<>>
        end,

      ?LOGINFO("~p merge to ~p, range ~p : ~p, init size ~s, init hash ~s",[
        Source, S, FromKey, EndKey, ?PRETTY_SIZE(dlss_segment:get_size(S)), ?PRETTY_HASH(InitHash)
      ]),

      FinalHash = dlss_copy:merge(Source,S, FromKey, EndKey, InitHash),

      ?LOGINFO("~p merged to ~p, range ~p, to ~p, final size ~s, new version ~p, new hash ~s, commit...",[
        Source, S, FromKey, EndKey, ?PRETTY_SIZE(dlss_segment:get_size(S)), Version, ?PRETTY_HASH(FinalHash)
      ]),
      % Update the version of the segment in the schema
      dlss_storage:set_segment_version( S, Node, #dump{hash = FinalHash, version = Version }),

      % Commit the transformation
      merge_commit_child(S, master_node( S )),

      % Commit the transformation
      case merge_commit_child(S, master_node( S )) of
        ok->
          merge_level(Tail, Source, Params, Node);
        abort->
          % Segment commit aborted, do not proceed. Let's start the next cycle,
          % do hash verification and copies synchronization and only then proceed.
          % Already committed segments are going to be skipped during the version check
          ?LOGERROR("~p has different hash with the master, stop merge",[S]),
          ok
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

  #{copies:=Copies} = dlss_storage:segment_params( Segment ),
  Dump =
    #dump{version = Version, hash = Hash} = maps:get( Master, Copies ),
  case not_confirmed( Dump, Copies ) of
    [] ->
      % All are ready
      ?LOGINFO("~p split commit, version ~p, hash ~s",[ Segment, Version, ?PRETTY_HASH(Hash) ]),
      dlss_storage:split_commit( Segment, SplitKey );
    Nodes->
      % There are still nodes that are not confirmed the hash yet
      ?LOGINFO("~p splitting is not finished yet, waiting for ~p",[Segment, Nodes]),
      timer:sleep( ?CYCLE ),
      % Update the master
      split_commit(Segment, SplitKey, master_node( Segment ))
  end;

%--------------------Slave commit-----------------------------------
split_commit(Segment, SplitKey, Master)->

  #{copies:=Copies} = dlss_storage:segment_params( Segment ),
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
      timer:sleep( ?CYCLE ),
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

  #{copies:=Copies} = dlss_storage:segment_params( Segment ),
  MasterDump =
    #dump{ version = MasterVersion, hash = MasterHash } = maps:get( Master, Copies ),
  MyDump =
    #dump{ version = MyVersion, hash = MyHash } = maps:get( node(), Copies ),

  if
    MyDump =:= MasterDump->
      ?LOGINFO("~p merge commit, version ~p, hash ~s",[ Segment, MyVersion, ?PRETTY_HASH(MyHash) ]),
      ok;
    not is_number(MasterVersion); MyVersion > MasterVersion->

      % There are still nodes that are not confirmed the hash yet
      ?LOGINFO("~p merging is not finished yet, waiting for master ~p",[Segment, Master]),
      timer:sleep( ?CYCLE ),
      % Update the master
      merge_commit_child(Segment, master_node( Segment ));
    true->
      % Something went wrong.
      % The local copy is going to be dropped during hash verification and reloaded during copies synchronization on the next cycle
      ?LOGERROR("~p merge abort: has different dump to master ~p, master version ~p, master hash ~s, local version ~p, local hash ~s",[
        Segment, Master, MasterVersion, ?PRETTY_HASH(MasterHash), MyVersion, ?PRETTY_HASH(MyHash)
      ]),

      abort
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
      timer:sleep( ?CYCLE ),
      % Update the master
      merge_commit_final( Source )
  end;
%-----------------Slave commit--------------------------------
merge_commit_final(Source, Master)->
  % It's a job for the master. May be I'm even not engaged
  ?LOGDEBUG("~p merge commit wait for master ~p",[Source,Master]).

confirm_children([Segment|Rest], Source)->
  #{copies:=Copies, version:=SegmentVersion} = dlss_storage:segment_params( Segment ),
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
check_limits( Storage )->
  Segments =
    [ {S, dlss_storage:segment_params(S)} || S <- dlss_storage:get_segments(Storage) ],

  % Operations with lower level segments have the priority,
  % therefore reverse the results to start from the lowest level
  Levels = lists:reverse( group_by_levels(Segments) ),

  %-------------Step 1. Check per level limits---------------------------------
  case check_level_limits( Levels ) of
    {Level, Segment} ->
      ?LOGINFO("level ~p has reached the limit, queue merge ~p",[Level, Segment]),
      dlss_storage:merge_segment( Segment ),
      merge;
    undefined ->
      %--------Step 2. Check segments size limits------------------------------
      case check_size_limit( Levels ) of
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

check_level_limits([{Level, Segments } | Rest] )->
  [{ Segment, _Params } | _ ] = Segments,
  case master_node( Segment ) of
    Master when Master =:= node()->
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
          check_level_limits( Rest )
      end;
    _->
      check_level_limits( Rest )
  end;
check_level_limits([])->
  undefined.

check_size_limit([{Level, Segments} | Rest])->
  case check_segment_size( Segments ) of
    undefined->
      check_size_limit( Rest );
    Segment->
      % The level has an oversized segment
      {Level, Segment}
  end;
check_size_limit([])->
  undefined.

check_segment_size([{ Segment, #{storage:=Storage, level:=Level }}| Rest])->
  case master_node( Segment ) of
    Master when Master=:=node() ->
      Size = dlss_segment:get_size( Segment ),
      Limit = segment_level_limit(Storage, Level ),
      if
        is_number(Limit), Size > Limit-> Segment;
        true ->
          check_segment_size( Rest )
      end;
    _->
        check_segment_size( Rest )
  end;
check_segment_size([])->
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
      #{ storage := Storage} = dlss_storage:segment_params( Segment ),
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

  #{copies:= Copies} = dlss_storage:segment_params( Segment ),

  SchemaNodes = [ N || { N, _Dump } <- maps:to_list( Copies ) ],
  ReadyNodes = dlss_segment:ready_nodes( Segment ),

  case lists:usort(SchemaNodes -- ( SchemaNodes -- ReadyNodes )) of
    [ Master | _ ]->
      % The master is a ready node with the smallest name
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


