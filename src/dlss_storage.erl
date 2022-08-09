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

-module(dlss_storage).

-include("dlss.hrl").

-record(sgm,{str,key,lvl,ver,copies}).

-define(BATCH_SIZE,100000).

%%=================================================================
%%	STORAGE SERVICE API
%%=================================================================
-export([
  %-----Service API-------
  is_storage/1,
  get_storages/0,
  get_segments/0,get_segments/1,
  get_node_segments/1,get_node_segments/2,

  root_segment/1,
  segment_params/1,
  get_type/1,
  is_local/1,

  get_children/1,
  has_siblings/1,
  parent_segment/1
]).

%%=================================================================
%%	SCHEMA TRANSFORMATION API
%%=================================================================
-export([
  % create/delete a storage
  add/2,add/3,
  remove/1,

  % get/set storage limits
  default_limits/0, default_limits/1,
  storage_limits/1, storage_limits/2,

  % add/remove segment copies
  add_segment_copy/2,
  remove_segment_copy/2,
  remove_all_segments_from/1,

  % Perform a transaction over segment in locked mode
  segment_transaction/3,

  % rebalance the storage schema
  split_segment/1,
  set_segment_version/3,
  split_commit/1,
  merge_segment/1,
  merge_commit/1,

  % MasterKey API
  get_master_key/1,
  set_master_key/2,
  remove_master_key/1
]).

%%=================================================================
%%	STORAGE READ/WRITE API
%%=================================================================
-export([
  read/2,read/3,dirty_read/2,
  write/3,write/4,dirty_write/3,
  delete/2,delete/3,dirty_delete/2,
  dirty_increment/3,
  drop_increment/2
]).

%%=================================================================
%%	STORAGE ITERATOR API
%%=================================================================
-export([
  first/1,dirty_first/1,
  last/1,dirty_last/1,
  next/2,dirty_next/2,
  prev/2,dirty_prev/2,

  dirty_range_select/3, dirty_range_select/4
]).
%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  get_key_segments/2
]).

-endif.

%%-----------------------------------------------------------------
%%  Service API
%%-----------------------------------------------------------------
is_storage(Storage)->
  case dlss_segment:dirty_read( dlss_schema, { id, Storage } ) of
    not_found -> false;
    _-> true
  end.
get_storages()->
  MS=[{
    #kv{key = #sgm{str = '$1',key = '_',lvl = 0,ver = '_',copies = '_'},value = '_'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

get_segments()->
  MS=[{
    #kv{key = #sgm{str = '_',key = '_',lvl = '_',ver = '_',copies = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).


get_segments(Storage)->
  MS=[{
    #kv{key = #sgm{str = Storage,key = '_',lvl = '_',ver = '_',copies = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

get_node_segments(Node)->
  MS=[{
    #kv{key = #sgm{str = '_',key = '_',lvl = '_',ver = '_',copies = '$2'}, value = '$1'},
    [],
    [['$1','$2']]
  }],
  AllSegments = dlss_segment:dirty_select(dlss_schema,MS),
  [ S || [S, #{Node := _}] <- AllSegments].

get_node_segments(Node, Storage)->
  MS=[{
    #kv{key = #sgm{str = Storage,key = '_',lvl = '_',ver = '_',copies = '$2'}, value = '$1'},
    [],
    [['$1','$2']]
  }],
  StorageSegments = dlss_segment:dirty_select(dlss_schema,MS),
  [ S || [S, #{Node := _}] <- StorageSegments].

root_segment(Storage)->
  case dlss_segment:dirty_next(dlss_schema,#sgm{str=Storage,key = '_',lvl = -1 }) of
    #sgm{ str = Storage } = Sgm->
      dlss_segment:dirty_read(dlss_schema,Sgm);
    _->?ERROR(invalid_storage)
  end.

get_type(Storage)->
  Root=root_segment(Storage),
  #{ type:= T }=dlss_segment:get_info(Root),
  T.

is_local(Storage)->
  Root=root_segment(Storage),
  #{ local:= IsLocal }=dlss_segment:get_info(Root),
  IsLocal.

segment_params(Name)->
  case segment_by_name(Name) of
    { ok, #sgm{ str = Str, lvl = Lvl, key = Key, ver = Version, copies = Copies } }->
      % The start key except for '_' is wrapped into a tuple
      % to make the schema properly ordered by start keys
      StartKey =
        case Key of
          { K } -> K;
          _-> Key
        end,
      { ok, #{ storage => Str, level => Lvl, key => StartKey, version=>Version, copies => Copies } };
    Error -> Error
  end.

%---------Create/remove a storage----------------------------------------
add(Name,Type)->
  add(Name,Type,#{}).
add(Name,Type,Options)->

  % Check if the occupied
  case is_storage(Name) of
    true->?ERROR(already_exists);
    _->ok
  end,

  % Default options
  Params =maps:merge(#{
    type=>Type,
    nodes=>[node()],
    local=>false
  },Options),

   % Generate an unique name within the storage
  Root=new_segment_name(Name),

  ?LOGINFO("create a new storage ~p of type ~p with root segment ~p with params ~p",[
    Name,
    Type,
    Root,
    Params
  ]),

  {Limits, SegmentParams} =
    case maps:take(limits, Params) of
      error -> { undefined ,Params};
      {_Limits,_Params} -> {_Limits,_Params}
    end,

  case dlss_segment:create(Root,SegmentParams) of
    ok -> ok;
    { error , Error }->
      ?LOGERROR("unable to create a root segment ~p of type ~p with params ~p for storage ~p, error ~p",[
        Root,
        Type,
        Params,
        Name,
        Error
      ]),
      ?ERROR(Error)
  end,

  % Set storage limits
  if
    is_map(Limits) -> storage_limits( Name, Limits );
    true -> ignore
  end,

  Copies = maps:from_list([ {N,undefined} ||N<-maps:get(nodes,Params)]),
  % Add the storage to the schema
  ok=dlss_segment:dirty_write(dlss_schema,#sgm{str=Name,key='_',lvl=0,ver = 0,copies = Copies},Root).

remove(Name)->
  ?LOGWARNING("removing storage ~p",[Name]),
  case dlss:transaction(fun()->
    % Set a lock on the schema
    dlss_backend:lock({table,dlss_schema},write),

    Start=#sgm{str=Name,key='_',lvl = -1},
    remove(Name,dlss_segment:next(dlss_schema,Start)),
    reset_id(Name)
  end) of
    {ok,_} ->
      ?LOGINFO("storage ~p removed",[Name]),
      ok;
    {error,Error} ->
      ?LOGERROR("unable to remove storage ~p, error ~p",[
        Name,
        Error
      ]),
      ?ERROR(Error)
  end.

remove(Storage,#sgm{str=Storage}=Sgm)->
  Table=dlss_segment:read(dlss_schema,Sgm),
  ?LOGWARNING("removing segment ~p storage ~p",[Table,Storage]),
  dlss_segment:delete(dlss_schema,Sgm),
  remove(Storage,dlss_segment:next(dlss_schema,Sgm));
remove(_Storage,_Sgm)->
  % '$end_of_table'
  ok.

% Get default limits
default_limits()->
  Limits =
    case ?ENV(segment_level_limit,#{}) of
      _C when is_map(_C)->_C;
      _C when is_list(_C)->maps:from_list(_C);
      _->#{}
    end,
  maps:merge( ?DEFAULT_SEGMENT_LIMIT, Limits ).

% set default limits
default_limits( Limits ) when is_list(Limits)->
  default_limits( maps:from_list(Limits) );
default_limits( Limits ) when is_map(Limits)->
  ?LOGINFO( "set dlss default limits ~p",[ Limits ]),
  application:set_env(dlss,segment_level_limit, Limits);
default_limits( _Limits )->
  ?ERROR(invalid_arguments).

% get storage limits
storage_limits( Name )->
  StorageLimits =
    case ?ENV({Name,limits},#{}) of
      _S when is_map(_S)->_S;
      _S when is_list(_S)->maps:from_list(_S);
      _ -> #{}
    end,
  DefaultLimits = default_limits(),
  maps:merge( DefaultLimits, StorageLimits ).

% set storage limits
storage_limits( Name, Limits ) when is_list(Limits)->
  storage_limits( Name, maps:from_list(Limits) );
storage_limits( Name, Limits ) when is_map(Limits)->
  ?LOGINFO( "set storage ~p default limits ~p",[Name, Limits]),

  application:set_env(dlss,{Name,limits}, Limits);
storage_limits( _Name, _Limits )->
  ?ERROR(invalid_arguments).

%%--------------------------------------------------------------------------------
%%  Split procedure
%%--------------------------------------------------------------------------------
split_segment( Segment )->
  {ok, #sgm{ str = Storage, lvl = Level } } = segment_by_name( Segment ),
  if
    Level =:= 0 ->
      new_root_segment( Storage );
    true ->
      split_segment( Storage, Segment )
  end.

split_segment( Storage, Segment )->
  % Inherit params
  Params = dlss_segment:get_info(Segment),

  %% Generate an unique name within the storage for the new segment
  NewSegment = new_segment_name(Storage),
  ?LOGINFO("~p split to ~p with params ~p",[
    Segment,
    NewSegment,
    Params
  ]),

  %% Creating a new table for the new segment
  case dlss_segment:create(NewSegment, Params) of
    ok -> ok;
    { error, CreateError }->
      ?LOGERROR("unable to create a new split segment ~p with params ~p for storage ~p, error ~p",[
        NewSegment,
        Params,
        Storage,
        CreateError
      ]),
      ?ERROR(CreateError)
  end,

  %% Level down all segments to +1
  case dlss:transaction(fun()->

    %% Locking an old Root table
    dlss_backend:lock({table,dlss_schema},write),

    {ok, Prn = #sgm{ copies = Copies0, lvl = Level }} = segment_by_name( Segment ),
    Copies = maps:map(fun(_K,_V)->undefined end, Copies0 ),

    % Put the new segment on the floating level
    ok = dlss_segment:write(dlss_schema, Prn#sgm{lvl= Level+0.1 ,ver = 0,copies = Copies}, NewSegment , write),

    ok
  end) of
    {ok,ok}->
      ok;
    SchemaError->
      ?ERROR( SchemaError )
  end.

new_root_segment( Storage ) ->
  %% Get Root segment
  Root = root_segment(Storage),

  %% Get Root table info
  Params = dlss_segment:get_info(Root),

  %% Generate an unique name within the storage for the new Root segment
  NewRoot=new_segment_name(Storage),
  ?LOGINFO("add a new root segment ~p with params ~p",[
    NewRoot,
    Params
  ]),

  %% Creating a new table for New Root
  case dlss_segment:create(NewRoot,Params) of
    ok -> ok;
    { error, CreateError }->
      ?LOGERROR("unable to create a new root segment ~p with params ~p for storage ~p, error ~p",[
        NewRoot,
        Params,
        Storage,
        CreateError
      ]),
      ?ERROR( CreateError )
  end,

  %% Level down all segments to +1
  case dlss:transaction(fun()->

    %% Locking the schema
    dlss_backend:lock({table,dlss_schema},write),

    %% Locking an old Root table
    dlss_backend:lock({table,Root},read),

    {ok, #sgm{ copies = Copies0} } = segment_by_name( Root ),
    Copies = maps:map(fun(_K,_V)->undefined end, Copies0 ),

    merge_segment( Root ),

    % Put the new root segment on the level 0
    ok = dlss_segment:write(dlss_schema, #sgm{str=Storage,key='_',lvl=0,ver = 0,copies = Copies}, NewRoot , write),

    ok

  end) of
    {ok, ok} ->
      % All the segments lower level 0 are read only
      % dlss_segment:set_access_mode( Root, read_only );
      ok;
    SchemaError ->
      ?ERROR( SchemaError )
  end.

split_commit( Segment )->
  Parent = parent_segment( Segment ),
  {ok, Sgm } = segment_by_name( Segment ),
  {ok, Prn } = segment_by_name( Parent ),
  ?LOGINFO("commit split from ~p to ~p",[
    Parent,Segment
  ]),
  split_commit( Sgm, Prn ).

split_commit( Sgm, #sgm{lvl = Level }=Prn )->

  % Set a version for a segment in the schema
  case dlss:transaction(fun()->

    % Set a lock on the segment
    Segment = dlss_segment:read( dlss_schema, Sgm, write ),
    Parent = dlss_segment:read( dlss_schema, Prn, write ),

    case dlss_segment:dirty_last( Segment ) of
      '$end_of_table'->

        % If the child is empty then the parent has only '@deleted@' records
        % Neither parent nor child are actually needed

        ?LOGWARNING("split commit: empty child segment, remove both parent ~p and child ~p",[
          Parent, Segment
        ]),
        ok = dlss_segment:delete(dlss_schema, Sgm , write ),
        ok = dlss_segment:delete(dlss_schema, Prn , write );
      Last ->

        case dlss_segment:dirty_next( Parent, Last ) of
          '$end_of_table' ->

            % If the parent has no more keys then all not '@deleted@' are in the child now
            % the parent is no longer needed

            ?LOGWARNING("split commit: empty parent segment, remove parent ~p ",[
              Parent
            ]),

            ok = dlss_segment:delete(dlss_schema, Prn , write ),

            % Remove old version of the child
            ok = dlss_segment:delete(dlss_schema, Sgm , write ),
            % Add new version of the child
            ok = dlss_segment:write( dlss_schema, Sgm#sgm{ lvl = Level }, Segment, write );
          Next ->

            % The Next is the key on which the parent is split
            ?LOGINFO("split commit: parent ~p, key ~p, child ~p, key ~p",[
              Parent, Next,
              Segment, Sgm#sgm.key
            ]),

            % Remove old versions
            ok = dlss_segment:delete(dlss_schema, Sgm , write ),
            ok = dlss_segment:delete(dlss_schema, Prn , write ),


            % Add the new versions
            ok = dlss_segment:write( dlss_schema, Sgm#sgm{ lvl = Level }, Segment, write ),
            ok = dlss_segment:write( dlss_schema, Prn#sgm{ key = { Next } }, Parent, write )
        end
    end,

    % Transaction end
    ok
  end) of
    {ok, ok} ->
      ok;
    Error -> ?ERROR( Error )
  end.

%%--------------------------------------------------------------------------------
%%  Merge procedure
%%--------------------------------------------------------------------------------
merge_segment( Segment )->
  MergeTo=
    case get_children(Segment) of
      []->[];
      Children->
        FirstKey =
          case segment_by_name( Segment ) of
            {ok, #sgm{ key = '_' } }->
              dlss_segment:dirty_first(Segment);
            {ok, #sgm{ key = { _FirstKey } } }->
              _FirstKey
          end,
        Last = lists:last(Children),
        LastKey = dlss_segment:dirty_last( Last ),
        if
          FirstKey > LastKey->
            % The segment doesn't have common keys with children, it just
            % takes its place in the end of the level
            ?LOGINFO("~p doesn't have coomon keys with ~p, it goes to the end of the level",[
              Segment,Children
            ]),
            [] ;
          true ->
            Children
        end
    end,
  merge_segment(Segment, MergeTo ).

merge_segment( Segment, [] )->
  % The segment has no children to merge with, move it directly level down
  { ok, Sgm = #sgm{ lvl = Level } } = segment_by_name( Segment ),

  % The next segment has to take '_' as the first key
  NextSgm = next_sibling( Sgm ),

  case dlss:transaction(fun()->
    % Set a lock on the segment
    Segment = dlss_segment:read( dlss_schema, Sgm, write ),

    % Update the segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ key = '_', lvl = Level + 1 }, Segment, write ),

    % Update the next segment
    if
      NextSgm =/= undefined->
        NextSegment = dlss_segment:read( dlss_schema, NextSgm, write ),
        ok = dlss_segment:delete(dlss_schema, NextSgm , write ),
        ok = dlss_segment:write( dlss_schema, NextSgm#sgm{ key = '_' }, NextSegment, write );
      true ->
        ok
    end,

    ok
  end) of
    {ok,ok} ->
      ?LOGINFO("~p successfully moved to level ~p",[ Segment, Level+1 ]),
      ok;
    Error -> ?ERROR( Error )
  end;
merge_segment( Segment, Children )->
  % The segment has children to merge with, move it to the floating level
  { ok, Sgm = #sgm{ lvl = Level } } = segment_by_name( Segment ),

  case dlss:transaction(fun()->

    % Set a lock on the segment
    Segment = dlss_segment:read( dlss_schema, Sgm, write ),

    % Increment the version of the children to merge with
    [ begin
        {ok,S1 = #sgm{ver = V}} = segment_by_name( S ),

        ok = dlss_segment:delete(dlss_schema, S1 , write ),
        ok = dlss_segment:write(dlss_schema, S1#sgm{ ver = V+1 }, S , write )

      end || S <- Children ],

    % Update the segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ lvl = Level + 0.9 }, Segment, write ),

    ok
  end) of
    {ok,ok} ->
      ?LOGINFO("~p is queued to merge to level ~p segments ~p",[ Segment, Level+1, Children ]),
      ok;
    Error -> ?ERROR( Error )
  end.

merge_commit( Segment )->

  { ok, Sgm } = segment_by_name( Segment ),

  % The next segment has to take '_' as the first key
  NextSgm = next_sibling( Sgm ),

  % Set a version for a segment in the schema
  case dlss:transaction(fun()->

    % Remove the merged segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),

    % Update the next segment
    if
      NextSgm =/= undefined->
        NextSegment = dlss_segment:read( dlss_schema, NextSgm, write ),
        ok = dlss_segment:delete(dlss_schema, NextSgm , write ),
        ok = dlss_segment:write( dlss_schema, NextSgm#sgm{ key = '_' }, NextSegment, write );
      true ->
        ok
    end,

    ok
  end) of
    {ok, ok } ->
      ok;
    {error, Error} ->
      ?LOGERROR("error on merge commit ~p, error ~p",[
        Segment,
        Error
      ])
  end.

set_segment_version( Segment, Node, Version )->

  % Set a version for a segment in the schema
  case dlss:transaction(fun()->
    % Set a lock on the segment
    Sgm = #sgm{copies = Copies} = lock_segment(Segment, write),

    % Update the copies
    Copies1 = Copies#{ Node=>Version },

    % Update the segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ copies = Copies1 }, Segment, write ),

    Segment
  end) of
    {ok,Segment} ->
      ?LOGINFO("update ~p verson for node ~p, new version ~p",[
        Segment,
        Node,
        Version
      ]),
      ok;
    Error -> ?ERROR( Error )
  end.

%%--------------------------------------------------------------------------------
%%  Add/Remove segment copies
%%--------------------------------------------------------------------------------
add_segment_copy( Segment , Node )->

  case dlss:transaction(fun()->
    % Set a lock on the segment
    Sgm = #sgm{copies = Copies} = lock_segment(Segment, write),

    case Copies of
      #{Node:=_}->
        % The copy is already added
        ok;
      _->
        % Just add a copy to the schema, the actual copying will do the storage supervisor
        ok = dlss_segment:delete(dlss_schema, Sgm , write ),
        ok = dlss_segment:write( dlss_schema, Sgm#sgm{ copies = Copies#{Node => undefined } }, Segment, write )
    end,

    ok
  end) of
    {ok,ok} -> ok;
    Error -> ?ERROR( Error )
  end.

remove_segment_copy( Segment , Node )->

  case dlss:transaction(fun()->
    % Set a lock on the segment
    Sgm = #sgm{copies = Copies} = lock_segment(Segment, write),

    case Copies of
      #{Node:=_}->
        % Just add a copy to the schema, the actual copying will do the storage supervisor
        ok = dlss_segment:delete(dlss_schema, Sgm , write ),
        ok = dlss_segment:write( dlss_schema, Sgm#sgm{ copies = maps:without([Node],Copies) }, Segment, write );
      _->
        % The copy is already removed
        ok
    end,

    ok
  end) of
    {ok,ok} -> ok;
    Error -> ?ERROR( Error )
  end.

remove_all_segments_from( Node )->
  case dlss:transaction(fun()->

    % Set lock on schema
    dlss_backend:lock({table,dlss_schema},write),

    % Remove segments' copies from the node
    [ remove_segment_copy(S, Node) || S <- get_node_segments( Node ) ],

    ok
  end) of
    {ok,ok} -> ok;
    Error -> ?ERROR( Error )
  end.

%------------Get children segments-----------------------------------------
get_children(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> get_children(Segment);
    Error -> Error
  end;
get_children(#sgm{str = Storage,lvl = Level})->
  LevelDown = round(math:floor( Level )) + 1,
  MS=[{
    #kv{key = #sgm{str = Storage,key = '_',lvl = LevelDown,ver = '_',copies = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

has_siblings(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> has_siblings(Segment);
    Error -> Error
  end;
has_siblings(Segment)->
  case { next_sibling(Segment), prev_sibling(Segment) } of
    { undefined, undefined }-> false;
    _-> true
  end.

%------------Get parent segment-----------------------------------------
parent_segment(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }->
      Parent = parent_segment(Segment),
      dlss_segment:dirty_read(dlss_schema,Parent);
    Error -> Error
  end;
parent_segment(#sgm{str = Str, lvl = Lvl} = Sgm)->
  case is_storage(Str) of
    true-> parent_segment( dlss_segment:dirty_prev(dlss_schema, Sgm ), Str, Lvl );
    _-> ?ERROR( invalid_storage )
  end.
parent_segment( #sgm{ str = Str, lvl = 0 } = Sgm, Str, _Lvl )->
  % The root segment
  Sgm;
parent_segment( #sgm{ str = Str, lvl = LvlUp } = Sgm, Str, Lvl ) when LvlUp < Lvl->
  % The level has changed. It means we have stepped level up
  % and this is the closest to the Key segment at this level
  Sgm;
parent_segment( #sgm{str = Str } = Sgm, Str, Lvl )->
  % if the level is the same it means that we are running through the level
  % towards the common Key. Skip
  parent_segment( dlss_segment:dirty_prev(dlss_schema, Sgm ),Str ,Lvl );
parent_segment( Other, Str, Lvl )->
  % '$end_of_table' or different storage.
  % Do we really can get here? Only in case of absent root segment.
  % Theoretically it is possible in dirty mode when a new root segment
  % is being created.
  % Wait a bit until a new root is in the schema
  timer:sleep(10),
  parent_segment( dlss_segment:dirty_next(dlss_schema, Other ),Str ,Lvl ).

%%=================================================================
%%	Read/Write
%%=================================================================
%---------------------Read-----------------------------------------
read(Storage, Key )->
  read( Storage, Key, _Lock = none ).
read(Storage, Key, Lock)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % Get potential segments ordered by priority (level)
  [ Root | Segments ]= get_key_segments(Storage,Key),
  case dlss_segment:read(Root,Key,Lock) of
    not_found->
      % The lock is already on the root segment, further search can be done
      % in dirty mode
      segments_dirty_read(Segments, Key );
    '@deleted@'->
      % The key is deleted
      not_found;
    Value->
      % The value is found in the root
      Value
  end.
dirty_read( Storage, Key )->
  % Get potential segments ordered by priority (level)
  Segments= get_key_segments(Storage,Key),
  % Search through segments
  segments_dirty_read( Segments, Key).

segments_dirty_read([ Segment | Rest ], Key)->
  case dlss_segment:dirty_read(Segment, Key) of
    '@deleted@'->not_found;
    not_found->segments_dirty_read(Rest, Key);
    Value -> Value
  end;
segments_dirty_read([], _Key)->
  not_found.

get_key_segments(Storage, Key)->
  case is_storage(Storage) of
    true->
      % The scanning starts at the lowest level
      Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
      key_segments( parent_segment(Lowest),[]);
    _->?ERROR(invalid_storage)
  end.
key_segments( #sgm{ lvl = 0 } = Sgm, Acc )->
  % The level 0 is the final
  [ dlss_segment:dirty_read(dlss_schema, Sgm)| Acc ];
key_segments( Sgm, Acc )->
  % The level has changed. It means we have stepped level up
  % and this is the closest to the Key segment at this level
  Acc1 = [ dlss_segment:dirty_read(dlss_schema, Sgm) | Acc ],
  key_segments( parent_segment(Sgm) ,Acc1 ).

%---------------------Write-----------------------------------------
write(Storage, Key, Value)->
  write( Storage, Key, Value, _Lock = none).
write(Storage, Key, Value, Lock)->
  % Set a lock on the schema while performing the operation
  dlss_backend:lock({table,dlss_schema},read),
  % All write operations are performed to the Root segment only
  Root = root_segment(Storage),
  dlss_segment:write( Root, Key, Value, Lock ).
dirty_write(Storage, Key, Value)->
  % All write operations are performed to the Root segment only
  Root = root_segment(Storage),
  dlss_segment:dirty_write( Root, Key, Value ).

%---------------------Delete-----------------------------------------
delete(Storage, Key)->
  delete( Storage, Key, _Lock = none).
delete(Storage, Key, Lock)->
  case get_key_segments(Storage,Key) of
    [Root]->
      % If there is only root segment in the storage we perform
      % true delete to keep it light (actual for ram-based storage types)
      dlss_segment:delete(Root,Key,Lock);
    [Root|_]->
      % The key may be present in the low-level segments,
      % the value is replaced with a service marker '@deleted@'
      % Actual delete is performed during rebalancing
      dlss_segment:write( Root, Key, '@deleted@', Lock )
  end.
dirty_delete(Storage, Key)->
  % Take a look at comments in the 'delete' method
  case get_key_segments(Storage,Key) of
    [Root]->
      dlss_segment:dirty_delete(Root,Key);
    [Root|_]->
      dlss_segment:dirty_write( Root, Key, '@deleted@' )
  end.


dirty_increment(Storage, Key, Incr)->
  dlss_segment:dirty_increment(dlss_schema,{counter,Storage,Key},Incr).
drop_increment(Storage, Key)->
  dlss_segment:dirty_delete(dlss_schema, {counter,Storage,Key} ).
%%=================================================================
%%	Iterate
%%=================================================================
safe_iterator( Iterator, Segment, Storage, Key )->
  case dlss_segment:Iterator(Segment,Key) of
    '$end_of_table' -> '$end_of_table';
    Next ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dirty_read(Storage,Next) of
        not_found ->
          safe_iterator( Iterator, Segment, Storage, Next);
        _->
          Next
      end
  end.
safe_iterator( Iterator, Segment, Storage )->
  case dlss_segment:Iterator(Segment) of
    '$end_of_table' -> '$end_of_table';
    Next ->
      case dirty_read(Storage,Next) of
        not_found ->
          Iterator1 =
            if
              Iterator =:= first -> next ;
              Iterator =:= dirty_first -> dirty_next;
              Iterator =:= last -> prev;
              Iterator =:= dirty_last -> dirty_prev
            end,
          safe_iterator( Iterator1, Segment, Storage, Next);
        _->
          Next
      end
  end.

%---------FIRST-----------------------
first(Storage)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_iterator(first, Segment, Storage) end,
  % The scanning starts at the lowest level
  Lowest = #sgm{ str = Storage, key = '~', lvl = '_' },
  Segments = key_segments( parent_segment(Lowest),[]),
  first(Segments,Iter,'$end_of_table').

dirty_first(Storage)->
  % Dirty iterator
  Iter = fun(Segment)-> safe_iterator(dirty_first, Segment, Storage) end,
  % The scanning starts at the lowest level
  Lowest = #sgm{ str = Storage, key = '~', lvl = '_' },
  Segments = key_segments( parent_segment(Lowest),[]),
  first(Segments,Iter,'$end_of_table').

first([S1|Rest],Iter,Acc)->
  F = Iter( S1 ),
  Acc1= next_acc( F, Acc ),
  first(Rest,Iter,Acc1);
first([],_Iter,F)-> F.

%---------LAST------------------------
last(Storage)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_iterator(last, Segment,Storage) end,
  % The scanning starts at the lowest level
  Highest = #sgm{ str = Storage, key = [], lvl = '_' },
  Segments = key_segments( parent_segment(Highest),[]),
  last(Segments,Iter,'$end_of_table').

dirty_last(Storage)->
  % Dirty iterator
  Iter = fun(Segment)->safe_iterator(dirty_last, Segment,Storage) end,
  % The scanning starts at the lowest level
  Highest = #sgm{ str = Storage, key = [], lvl = '_' },
  Segments = key_segments( parent_segment(Highest),[]),
  last(Segments,Iter,'$end_of_table').

last([S1|Rest],Iter, Acc)->
  L = Iter( S1 ),
  Acc1 = prev_acc( L, Acc ),
  last(Rest,Iter,Acc1);
last([],_Iter,L)-> L.

%---------NEXT------------------------
next( Storage, Key )->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_iterator(next,Segment,Storage,Key) end,
  % Schema starting point
  Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
  next( parent_segment(Lowest), Iter, '$end_of_table' ).

dirty_next(Storage,Key)->
  % The iterator
  Iter = fun(Segment)->safe_iterator(dirty_next,Segment,Storage,Key) end,
  % Starting point
  Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
  next( parent_segment(Lowest), Iter, '$end_of_table' ).

next( #sgm{ lvl = 0 } = Sgm, Iter, Acc )->
  % The level 0 is final
  Segment = dlss_segment:dirty_read(dlss_schema, Sgm),
  Next = Iter( Segment ),
  next_acc( Next, Acc );
next( Sgm, Iter, Acc )->
  Segment = dlss_segment:dirty_read(dlss_schema, Sgm),
  Next=
    case Iter( Segment ) of
      '$end_of_table'->
        % If the segment does not contain the Next key then try to lookup
        % in the next segment at the same level
        case next_sibling(Sgm) of
          undefined ->
            '$end_of_table';
          NextSgm->
            NextSegment = dlss_segment:dirty_read(dlss_schema, NextSgm),
            Iter( NextSegment )
        end;
      NextKey->NextKey
    end,
  Acc1= next_acc( Next, Acc ),
  next( parent_segment(Sgm), Iter, Acc1 ).

next_acc(Key,Acc)->
  if
    Key =:= '$end_of_table'-> Acc;
    Acc =:= '$end_of_table' -> Key;
    Key < Acc -> Key;
    true -> Acc
  end.

next_sibling(#sgm{ str = Str, lvl = Lvl } = Sgm)->
  next_sibling( dlss_segment:dirty_next(dlss_schema, Sgm), Str, Lvl ).
next_sibling(#sgm{ str = Str,  lvl = LvlDown } = Sgm, Str, Lvl) when LvlDown > Lvl->
  % Running through sub-levels
  next_sibling( dlss_segment:dirty_next(dlss_schema, Sgm), Str, Lvl );
next_sibling(#sgm{ str = Str, lvl = Lvl } = Sgm, Str, Lvl)->
  % The segment is at the same level. This is the sibling
  Sgm;
next_sibling(_Other, _Str, _Lvl)->
  % Lower level or different storage
  undefined.

%---------PREVIOUS------------------------
prev( Storage, Key )->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_iterator(prev, Segment,Storage,Key) end,
  % Schema starting point
  Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
  prev( parent_segment(Lowest), Iter, '$end_of_table' ).

dirty_prev(Storage,Key)->
  % The iterator
  Iter = fun(Segment)->safe_iterator(dirty_prev, Segment,Storage,Key) end,
  % Starting point
  Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
  prev( parent_segment(Lowest), Iter, '$end_of_table' ).

prev( #sgm{ lvl = 0 } = Sgm, Iter, Acc )->
  % The level 0 is final
  Segment = dlss_segment:dirty_read(dlss_schema, Sgm),
  Prev = Iter( Segment ),
  prev_acc( Prev, Acc );
prev( Sgm, Iter, Acc )->
  Segment = dlss_segment:dirty_read(dlss_schema, Sgm),
  Prev=
    case Iter( Segment ) of
      '$end_of_table'->
        % If the segment does not contain the Next key then try to lookup
        % in the previous segment at the same level
        case prev_sibling(Sgm) of
          undefined ->
            '$end_of_table';
          PrevSgm->
            PrevSegment = dlss_segment:dirty_read(dlss_schema, PrevSgm),
            Iter( PrevSegment )
        end;
      PrevKey->PrevKey
    end,
  Acc1= prev_acc( Prev, Acc ),
  prev( parent_segment(Sgm), Iter, Acc1 ).

prev_acc(Key,Acc)->
  if
    Key =:= '$end_of_table'-> Acc;
    Acc =:= '$end_of_table' -> Key;
    Key > Acc -> Key;
    true -> Acc
  end.


prev_sibling(#sgm{ str = Str, lvl = Lvl } = Sgm)->
  prev_sibling( dlss_segment:dirty_prev(dlss_schema, Sgm), Str, Lvl ).
prev_sibling(#sgm{ str = Str, lvl = LvlDown } = Sgm, Str, Lvl) when LvlDown > Lvl->
  % Running through sub-levels
  prev_sibling( dlss_segment:dirty_prev(dlss_schema, Sgm), Str, Lvl );
prev_sibling(#sgm{ str = Str, lvl = Lvl } = Sgm, Str, Lvl)->
  % The segment is at the same level. This is the sibling
  Sgm;
prev_sibling(_Other, _Str, _Lvl)->
  % Higher level or different storage
  undefined.

%-------------Range of keys----------------------------------------------
dirty_range_select(Storage, StartKey, EndKey ) ->
  dirty_range_select(Storage, StartKey, EndKey, infinity).
dirty_range_select(Storage, StartKey, EndKey, Limit) ->

  % Filter the segment that contain keys bigger
  % than the EndKey
  HeadSegments = find_head_segments( Storage, EndKey ),

  % Remove the head segments from levels that cannot contain keys from the range
  Segments=
    if
      StartKey =/= '$start_of_table'->
        drop_head( HeadSegments, { StartKey } );
      true ->
        HeadSegments
    end,

  % Order the segments by level and then by start key
  OrderedSegments = lists:usort( Segments ),

  % The segments can contain a lot of @deleted@ records, therefore we try
  % to embrace a longer range for each segment scanning iteration to cut down
  % the number of iterations
  ScanLimit =
    if
      is_number(Limit), Limit < 1024 -> 1024;
      true -> Limit
    end,
  % scan each segment
  Results =
    [ { L, scan_segment( S, StartKey, EndKey, ScanLimit) } || [L,_Key, S] <- OrderedSegments ],

  % Merge by segments results
  Merged = merge_results( Results ),

  % Remove deleted records from the result
  Filtered =
    [ {K, V} || {K,V} <- Merged, V=/='@deleted@' ],

  % Limit the result
  if
    Limit=:=infinity ->
      Filtered;
    length( Filtered ) =:= Limit->
      % Exactly the limit
      Filtered;
    length( Filtered ) > Limit->
      % We satisfy the limit, cut off the excessive records
      lists:sublist( Filtered, Limit );
    length( Merged )<Limit->
      % The total length of he result including deleted records is shorter than the limit,
      % there is no sense to keep searching
      Filtered;
    length( Merged ) =:= length( Filtered )->
      % There are no deleted records in the result, therefore it is the maximum that we can find
      Filtered;
    length( Filtered ) =:= 0->
      % There are only deleted entries found, keep searching from the next valid key
      % and also increase the range
      {NextKey, _} = lists:last( Merged ),
      lists:sublist( dirty_range_select( Storage, NextKey, EndKey, Limit * 2 ), Limit );
    true ->
      % If we are here then there were deleted records in the result that prevented us
      % from getting the full result, we need to keep searching.
      % Continue from the last key in the full result

      { Head, [ { LastKey,_} ]} = lists:split( length( Filtered )-1, Filtered ),
      Head ++ lists:sublist( dirty_range_select( Storage, LastKey, EndKey, Limit ), Limit - length(Head) )

  end.

find_head_segments( Storage, Key )->
  ToGuard=
    if
      Key =:= '$end_of_table' -> [];
      true -> [{'=<','$1',{const, { Key }}}]
    end,
  MS=[{
    #kv{key = #sgm{str=Storage, key='$1', lvl='$2',ver = '_',copies = '_'}, value='$3'},
    ToGuard,
    [['$2','$1','$3']]  % The level goes first to be able to order by it later
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

drop_head( [ [Level,_Key,_Name1 ], [Level, Key, _Name2 ]=Next | Rest ], FromKey ) when FromKey >= Key->
  % If the following segment of the same level contain keys bigger than
  % the FromKey then the current segment cannot contain key from the range
  drop_head( [Next | Rest], FromKey );
drop_head( [S | Rest], FromKey )->
  % FromKey is smaller than first key of the next segment in the level
  % or it is the last segment in the level - include
  [ S | drop_head( Rest, FromKey ) ];
drop_head( [], _FromKey )->
  [].

scan_segment( Segment, StartKey, EndKey, Limit )->
  % As scanning is performed in dirty mode
  % there are might be schema transformation after selecting
  % the segments so the segment may not exist any more
  try
    dlss_segment:dirty_scan(Segment, StartKey, EndKey, Limit)
  catch
      _:{no_exists, _}->[];
    _:Error->?ERROR({Segment,Limit,Error})
  end.

merge_results( [ { Level, Records1 }, { Level, Records2 } | Rest ] )->
  % The results are from the same level, union them
  merge_results([ { Level, merge_levels( Records2, Records1 ) }|Rest]);
merge_results( [ {_Level ,LevelResult} | Rest ] )->
  % The next result is from the lower level
  merge_levels(  merge_results( Rest ), LevelResult );
merge_results([])->
  [].

merge_levels([{K1,_}=E1|D1], [{K2,_}=E2|D2]) when K1 < K2 ->
  [E1|merge_levels(D1, [E2|D2])];
merge_levels([{K1,_}=E1|D1], [{K2,_}=E2|D2]) when K1 > K2 ->
  [E2|merge_levels([E1|D1], D2)];
merge_levels([_E1|D1], [E2|D2])-> % K1 = K2, take the value from the second set
  [E2|merge_levels(D1, D2)];
merge_levels([], D2) -> D2;
merge_levels(D1, []) -> D1.


%%=================================================================
%%	Internal stuff
%%=================================================================
new_segment_name(Storage)->
  Id=get_unique_id(Storage),
  Name="dlss_"++atom_to_list(Storage)++"_"++integer_to_list(Id),
  list_to_atom(Name).

get_unique_id(Storage)->
  case dlss:transaction(fun()->
    I =
      case dlss_segment:read( dlss_schema, { id, Storage }, write ) of
        ID when is_integer(ID) -> ID + 1;
        _ -> 1
      end,
    ok = dlss_segment:write( dlss_schema, { id, Storage }, I ),
    I
  end) of
    {ok, Value } -> Value;
    { error, Error } -> ?ERROR(Error)
  end.
reset_id(Storage)->
  dlss_segment:delete( dlss_schema, {id,Storage}, write).

segment_by_name(Name)->
  MS=[{
    #kv{key = '$1', value = Name},
    [],
    ['$1']
  }],
  case dlss_segment:dirty_select(dlss_schema,MS) of
    [Key]->{ ok, Key };
    _-> { error, not_found }
  end.

lock_segment( Segment, Lock )->
  case segment_by_name( Segment ) of
    { ok, Sgm }->
      case dlss_segment:read( dlss_schema, Sgm, Lock ) of
        Segment -> Sgm;
        _->
          % We are here probably because master is changing the segment's config
          % Waiting for master to finish
          timer:sleep(10),
          lock_segment( Segment, Lock )
      end;
    Error ->
      ?ERROR(Error)
  end.

segment_transaction(Segment, Lock, Fun)->
  Owner = self(),
  Holder = spawn_link(fun()->
    case dlss:sync_transaction(fun()->lock_segment(Segment, Lock) end) of
      {ok,_}->
        Owner ! {locked, self()},
        receive
          {unlock, Owner}->
            unlink(Owner),
            ok
        end;
      Error -> Owner ! {error,self(),Error}
    end
  end),

  receive
    {locked, Holder}->
      Result =
        try Fun() catch _:Error -> {error,Error} end,
      Holder ! {unlock,self()},
      Result;
    {error,Holder,Error} -> {error,Error}
  end.


%----------------------MasterKey API---------------------------
set_master_key(Segment, Key) ->
  dlss_segment:dirty_write(dlss_schema, {rebalance,Segment}, Key),
  ok.

get_master_key(Segment) ->
  dlss_segment:dirty_read(dlss_schema, {rebalance, Segment}).

remove_master_key(Segment) ->
  dlss_segment:dirty_delete(dlss_schema, {rebalance, Segment}),
  ok.

