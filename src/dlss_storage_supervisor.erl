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

-record(state,{ storage, type, cycle }).
-record(dump,{ version, hash }).

%%=================================================================
%%	API
%%=================================================================
-export([
  start/1,
  start_link/1,
  stop/1
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
      dlss_schema_sup:stop_segment(PID);
    _ ->
      { error, not_started }
  end.

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
  storage = Storage,
  cycle = Cycle,
  type = Type
}=State)->

  % The loop
  {ok,_}=timer:send_after( Cycle, loop ),

  try

    % Synchronize actual copies configuration to the schema settings
    sync_copies( Storage, node() ),

    % Perform waiting transformations
    case pending_transformation( Storage, Type, node()) of
      { Operation, Segment } ->

        % Master commits the schema transformation if it is finished
        hash_confirm( Operation, Segment, node() );
      _->

        % Remove stale head
        purge_stale( Storage ),

        % No active transformations, check limits
        check_limits( Storage )
    end

  catch
    _:Error:Stack->
      ?LOGINFO("~p storage supervisor error ~p, stack ~p",[ Storage, Error, Stack ])
  end,

  {noreply,State}.


terminate(Reason,#state{storage = Storage})->
  ?LOGINFO("terminating storage supervisor ~p, reason ~p",[Storage,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%============================================================================
%% Obtain/Remove copies of segments of a Storage to the Node
%%============================================================================
sync_copies( Storage, Node )->
  [ case dlss_segment:get_info( S ) of
      #{ local => true }->
        % Local only storage types are not synchronized between nodes
        ok;
      #{ nodes => Nodes }->
        {ok, #{copies := Copies} } = dlss_storage:segment_params(S),

        case { maps:is_key( Node, Copies ), lists:member(Node,Nodes) } of
          { true, false }->
            % The segment is to be added to the Node
            Master = master_node( Copies ),
            Version = maps:get( Master, Copies ),

            % The segment must have a copy on the Node
            case dlss_segment:add_node( S, Node ) of
              ok ->
                % The segment has just copied, take the version from the parent
                ok = dlss_storage:set_segment_version( S, Node, Version );
              { error, Error }->
                ?LOGERROR("~p unable to add a copy to ~p, error ~p", [ S, Node, Error ] )
            end;
          { false, true }->
            % The segment is to be removed from the node
            case dlss_segment:remove_node(S,Node) of
              ok->ok;
              {error,Error}->
                ?LOGERROR("~p unable to remove a copy from ~p, error ~p",[ S, Node, Error ])
            end;
          _->
            % The segment is already synchronized with the schema
            ok
        end
    end || S <- dlss_storage:get_segments(Storage) ],
  ok.

%%============================================================================
%% The transformations
%%============================================================================
pending_transformation( Storage, Type, Node )->
  Segments=
    [ begin
        {ok, P } = dlss_storage:segment_params(S),
        { S, P }
      end || S <- dlss_storage:get_segments(Storage) ],
  case [ {S, P} || {S, #{level:=L} = P}<-Segments, is_float(L) ] of
    []->
      % There is no active schema transformations
      undefined;
    [{ Segment, #{level := Level} = Params } | _ ]->
      % The segment is under transformation
      Operation = which_operation( Level ),
      if
        Operation =:= split->
          %----------split---------------------------------------
          case Params of
            #{ version:=Version, copies := #{ Node := #dump{ hash = Hash }=Dump } }->
              % The segment has local copy
              Parent = dlss_storage:parent_segment( Segment ),
              case split_segment( Parent, Segment, Type, Hash ) of
                {ok, NewHash }->
                  % Update the version of the segment in the schema
                  ok = dlss_storage:set_segment_version( Segment, Node, Dump#dump{hash = NewHash, version = Version }  );
                {error, SplitError}->
                  ?LOGERROR("~p error on splitting, error ~p",[ Segment, SplitError ])
              end;
            _->
              % The segment does not have local copies
              ok
          end;
        Operation =:= merge->
          %----------merge---------------------------------------
          MergeLevel = round(Level),
          MergeTo =
            [ {S, P} || { S, #{level := MergeLevel} = P } <- Segments ],
          merge_level( MergeTo, Segment, Params, Node, Type )
      end,
      % The segment is under transformation
      {Operation, Segment}
  end.

%---------------------SPLIT---------------------------------------------------
split_segment( Parent, Segment, Type, Hash )->
  % Prepare the deleted flag
  Deleted =
    if
      Type=:=disc -> mnesia_eleveldb:encode_val('@deleted@');
      true ->'@deleted@'
    end,

  OnDelete =
    case dlss_storage:get_children(Segment) of
      []->
        % The lower segment has no children, it is safe to delete keys
        fun(K)->{delete, K} end;
      _->
        % There are lower level segments that may contain the Key, we have to mark
        % the key as deleted to override values from the lower-level segments
        fun(K)->{put, K, Deleted} end
    end,

  Copy=
    fun({K,V})->
      if
        V=:=Deleted -> OnDelete(K);
        true->{ put, K, V }
      end
    end,

  {ok, #{ level:= Level}} = dlss_storage:segment_params( Segment ),
  ConfigVar = list_to_atom("level_"++integer_to_list(Level)++"_segment_limit"),

  % Copy a half of the size of the segment
  ToSize = ?ENV( ConfigVar, maps:get(Level,?DEFAULT_SEGMENT_LIMITS) ) / 2 * ?MB,

  OnBatch=
    fun(K,Count,Size)->
      ?LOGINFO("~p splitting from ~p: key ~p, count ~p, size ~p",[
        Segment,
        Parent,
        if Type =:=disc-> mnesia_eleveldb:decode_key(K); true ->K end,
        Count,
        Size / ?MB
      ]),
      % We stop splitting when the total size of copied records reaches the half of the limit for the segment.
      % TODO. ATTENTION!!! If nodes have different settings for segments size limits it will lead to different
      % final hash for the segment for participating nodes. And the segment is going to be reloaded from the master
      % each time after splitting
      if
        Size >= ToSize-> stop ;
        true -> next
      end
    end,

  dlss_rebalance:copy( Parent, Segment, Copy, '$start_of_table', OnBatch,  Hash ).

%---------------------MERGE---------------------------------------------------
merge_level([_Prev, Next = {_S, #{key:=K2}}| Tail ], Source, #{key:=Key} = Params, Node, Type )
  when Key>=K2->
  % The first key in the source segment is bigger is bigger than the first key
  % of the next segment, so there are no keys for the S1 segment in the source
  merge_level([Next|Tail], Source,Params, Node, Type );
merge_level([ {S, #{key:=FromKey, version:=Version, copies:=Copies}}| Tail ], Source, _Params, Node, Type )->
  % This is the segment that is currently copies the keys from the source
  case Copies of
    #{ Node := #dump{hash = Hash } = Dump}->
      % This node hosts the target segment, it must to play its role
      ToKey =
        case Tail of
          [{_, #{ key := _NextKey }}| _]->
            dlss_segment:dirty_prev( Source, _NextKey );
          _->
            '$end_of_table'
        end,
      case merge_segment( S, Source, FromKey, ToKey, Type, Hash ) of
        {ok, NewHash }->
          % Update the version of the segment in the schema
          ok = dlss_storage:set_segment_version( S, Node, Dump#dump{hash = NewHash, version = Version }  );
        {error, MergeError}->
          ?LOGERROR("~p error on merging to ~p, error ~p",[ Source, S, MergeError ])
      end;
    _->
      % The Node doesn't hosts the target segment, just wait for other nodes to do their part of work
      ok
  end;
merge_level( [], _Source, _Params, _Node, _Type )->
  % There is no locally hosted segments to merge to
  ok.

merge_segment( Target, Source, FromKey0, ToKey0, Type, Hash )->

  % Prepare the deleted flag
  { Deleted, FromKey, ToKey } =
    if
      Type=:=disc ->
        {
          mnesia_eleveldb:encode_val('@deleted@'),
          if
            FromKey0 =:= '$start_of_table'->
              '$start_of_table';
            true ->
              mnesia_eleveldb:encode_key(FromKey0)
          end,
          if
            ToKey0 =:= '$end_of_table'->
              '$end_of_table';
            true ->
              mnesia_eleveldb:encode_key(ToKey0)
          end
        };
      true ->{
        '@deleted@',
        FromKey0,
        ToKey0
      }
    end,

  OnDelete =
    case dlss_storage:get_children( Target ) of
      []->
        % The lower segment has no children, it is safe to delete keys
        fun(K)->{delete, K} end;
      _->
        % There are lower level segments that may contain the Key, we have to mark
        % the key as deleted to override values from the lower-level segments
        fun(K)->{put, K, Deleted} end
    end,

  Copy=
    fun({K,V})->
      if
        ToKey =/= '$end_of_table', K > ToKey -> stop;
        V =:= Deleted -> OnDelete(K);
        true->{ put, K, V }
      end
    end,

  OnBatch=
    fun(K,Count,Size)->
      ?LOGINFO("~p merging from ~p: key ~p, count ~p, size ~p",[
        Target,
        Source,
        if Type =:=disc-> mnesia_eleveldb:decode_key(K); true ->K end,
        Count,
        Size / ?MB
      ]),

      % Stop only when
      next
    end,

  dlss_rebalance:copy( Source, Target, Copy, FromKey, OnBatch, Hash ).

reload_segment( Segment, SourceNode )->

  {ok, #{copies:= Copies} } = dlss_storage:segment_params( Segment ),
  #dump{ version = Ver, hash = Hash } = maps:get( SourceNode, Copies ),

  case dlss_rebalance:reload_from( Segment, SourceNode ) of
    ok->
      ok = dlss_storage:set_segment_version( Segment, node(), #dump{hash = Hash, version = Ver }  );
    { error, Error }->
      ?LOGERROR("~p unable to reload from ~p, error ~p",[
        Segment,
        SourceNode,
        Error
      ])
  end.


%%============================================================================
%% Confirm schema transformation
%%============================================================================
hash_confirm( Operation, Segment, Node )->
  {ok, #{copies:= Copies} = Params } = dlss_storage:segment_params( Segment ),
  case master_node( Copies ) of
    Node ->
      master_commit( Operation, Segment,Node, Params );
    Master->
      check_hash( Operation, Segment, Node, Master, Params )
  end.

master_commit( split, Segment, Node, #{version := Version,copies:=Copies})->
  #dump{ hash = Hash } = maps:get( Node, Copies ),
  case not_confirmed( Version, Hash, Copies ) of
    [] ->
      dlss_storage:split_commit( Segment );
    Nodes->
      % There are still nodes that are not confirmed the hash yet
      ?LOGINFO("~p splitting is not finished yet, waiting for ~p",[ Nodes ]),
      ok
  end;
master_commit( merge, Segment, _Node, _Params )->
  Children = dlss_storage:get_children( Segment ),
  Children1=
    [ begin
        {ok, #{version:=V, copies:=C} } = dlss_storage:segment_params(S),
        M = master_node(C),
        #{ M:= #dump{ hash = H } } = C,
        {S, not_confirmed( V, H, C ) }
      end || S <- Children ],
  case [ {S,W} || {S, W } <- Children1, length(W)>0 ] of
    []->
      dlss_storage:merge_commit( Segment );
    Wait->
      ?LOGINFO("~p merging is not finished yet, waiting for ~p",[ Wait ]),
      ok
  end.

check_hash( split, Segment, Node, Master, #{copies:=Copies} )->
  #dump{ version = Ver, hash = Hash } = maps:get( Master, Copies ),
  case maps:get( Node, Copies ) of
    #dump{ version = Ver, hash = Hash }->
      % The hash is confirmed
      ok;
    #dump{ version = Ver}->
      ?LOGWARNING("~p hash for ~p differs from hash for master ~p",[ Segment, Node, Master ]),
      % We purge the local copy of the segment to let the sync mechanism to reload it
      % next cycle
      dlss_segment:remove_node( Segment, Node );
    _->
      % The node has not updated its version yet
      ok
  end;

check_hash( merge, Segment, Node, _Master, _Params )->
  Children = dlss_storage:get_children( Segment ),


not_confirmed( Version, Hash, Copies )->
  [ N || { N, #dump{version = V,hash = H}} <- Copies, V =/=Version or H=/=Hash  ].

%%============================================================================
%%	Internal helpers
%%============================================================================
master_node( Copies )->
  WithHash = lists:usort([ N || { N, #dump{ hash = Hash } } <- maps:to_list( Copies ), is_binary(Hash) ]),
  Ready = dlss:get_ready_nodes( ),
  case WithHash -- Ready of
    [ Master | _ ]-> Master;
    _-> undefined
  end.

which_operation(Level)->
  Floor = math:floor( Level ),
  if
    round(Level) =:= Floor ->
      % If the level is closer to the upper level then it is the split
      split;
    true ->
      % If the level is closer to lower level, then it is a merge
      merge
  end.


