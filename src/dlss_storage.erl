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
  new_root_segment/1,
  set_segment_version/3,
  split_commit/1,
  merge_commit/1,
  root_segment/1,
  segment_params/1,
  add/2,add/3,
  remove/1,
  get_type/1,
  is_local/1,
  spawn_segment/1,
  absorb_parent/1,
  absorb_segment/1,
  level_up/1,
  get_children/1,
  has_siblings/1,
  parent_segment/1
]).

%%=================================================================
%%	STORAGE READ/WRITE API
%%=================================================================
-export([
  read/2,read/3,dirty_read/2,
  write/3,write/4,dirty_write/3,
  delete/2,delete/3,dirty_delete/2
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
    #kv{key = #sgm{str = '$1',key = '_',lvl = 0},value = '_'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

get_segments()->
  MS=[{
    #kv{key = #sgm{str = '_',key = '_',lvl = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).


get_segments(Storage)->
  MS=[{
    #kv{key = #sgm{str = Storage,key = '_',lvl = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  dlss_segment:dirty_select(dlss_schema,MS).

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
  Params=maps:merge(#{
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
  case dlss_backend:create_segment(Root,Params) of
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

  % Add the storage to the schema
  ok=dlss_segment:dirty_write(dlss_schema,#sgm{str=Name,key='_',lvl=0},Root).

remove(Name)->
  ?LOGWARNING("removing storage ~p",[Name]),
  Segments = get_segments( Name ),
  case dlss:transaction(fun()->
    % Set a lock on the schema
    dlss_backend:lock({table,dlss_schema},write),

    Start=#sgm{str=Name,key='_',lvl = -1},
    remove(Name,dlss_segment:dirty_next(dlss_schema,Start)),
    reset_id(Name)
  end) of
    {ok,_} ->
      [case dlss_backend:delete_segment(S) of
         ok -> ok;
         { error, Error } ->
           ?LOGERROR("backend error on removing segment ~p storage ~p, error ~p",[
             S,
             Name,
             Error
           ])
       end|| S<-Segments],
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
  Table=dlss_segment:dirty_read(dlss_schema,Sgm),
  ?LOGWARNING("removing segment ~p storage ~p",[Table,Storage]),
  ok=dlss_segment:delete(dlss_schema,Sgm,write),
  remove(Storage,dlss_segment:dirty_next(dlss_schema,Sgm));
remove(_Storage,_Sgm)->
  % '$end_of_table'
  ok.

%---------Add a new Root segment to the storage----------------------------------------

new_root_segment(Storage) ->
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
  case dlss_backend:create_segment(NewRoot,Params) of
    ok -> ok;
    { error, Error }->
      ?LOGERROR("unable to create a new root segment ~p with params ~p for storage ~p, error ~p",[
        NewRoot,
        Params,
        Storage,
        Error
      ]),
      ?ERROR(Error)
  end,

  %% Level down all segments to +1
  dlss:transaction(fun()->

    %% Locking an old Root table
    dlss_backend:lock({table,dlss_schema},write),

    %% Locking an old Root table
    dlss_backend:lock({table,Root},read),

    % Find all segments of the Storage
    Segments = get_children(#sgm{str=Storage,key = '_',lvl = -1 }),

    % Put all segments level down
    [ begin
        ok = dlss_segment:write(dlss_schema, S#sgm{ lvl = S#sgm.lvl + 1 }, T , write ),
        ok = dlss_segment:delete(dlss_schema, S , write )
      end || {S, T} <- lists:reverse(Segments)],
    % Add the new Root segment to the schema
    ok=dlss_segment:write(dlss_schema, #sgm{str=Storage,key='_',lvl=0}, NewRoot , write)
  end),
  ok.


set_segment_version( Segment, Node, Version )->
  case segment_by_name( Segment ) of
    { ok, Sgm }-> set_segment_version( Sgm, Node, Version );
    Error -> Error
  end;
set_segment_version( #sgm{ copies = Copies } = Sgm, Node, Version )->

  % Set a version for a segment in the schema
  case dlss:transaction(fun()->
    % Set a lock on the segment
    Segment = dlss_segment:read( dlss_schema, Sgm, write ),

    % Update the copies
    Copies1 = Copies#{ Node=>Version },

    % Update the segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ copies = Copies1 }, Segment, write )

  end) of
    {ok,ok} -> ok;
    Error -> Error
  end.

split_commit( Segment )->
  Parent = parent_segment( Segment ),
  First = dlss_segment:dirty_first( Segment ),
  Last = dlss_segment:dirty_last( Segment ),
  Next = dlss_segment:dirty_next( Parent, Last ),

  {ok, Sgm } = segment_by_name( Segment ),
  {ok, Prn } = segment_by_name( Parent ),

  split_commit( Sgm, First, Prn, Next ).

split_commit( Sgm, K1, #sgm{lvl = Level }=Prn, K2 )->

  % Set a version for a segment in the schema
  case dlss:transaction(fun()->
    % Set a lock on the segment
    Segment = dlss_segment:read( dlss_schema, Sgm, write ),
    Parent = dlss_segment:read( dlss_schema, Prn, write ),

    % Update the segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ lvl = Level, key = K1 }, Segment, write ),

    % Update the parent
    ok = dlss_segment:delete(dlss_schema, Prn , write ),
    ok = dlss_segment:write( dlss_schema, Prn#sgm{ key = K2 }, Parent, write ),

    ok
  end) of
    {ok,ok} -> ok;
    Error -> Error
  end.

merge_commit( Segment )->

  { ok, Sgm } = segment_by_name( Segment ),

  Children =
    [ segment_by_name(S) || S <- get_children( Segment ) ],

  merge_commit( Sgm, Children ).

merge_commit( Sgm, Children )->
  % Set a version for a segment in the schema
  case dlss:transaction(fun()->
    % Set a lock on the segment
    Merged = dlss_segment:read( dlss_schema, Sgm, write ),
    [ begin

        Segment = dlss_segment:read( dlss_schema, S, write ),
        First = dlss_segment:first( Segment ),

        % Update the segment
        ok = dlss_segment:delete(dlss_schema, S , write ),
        ok = dlss_segment:write( dlss_schema, S#sgm{ key = First }, Segment, write )

      end || S <- Children ],

    % Remove the merged segment
    ok = dlss_segment:delete(dlss_schema, Sgm , write ),

    Merged
  end) of
    {ok, Merged } ->
      ?LOGINFO("removing merged segment ~p",[Merged]),
      case dlss_backend:delete_segment( Merged ) of
        ok->ok;
        {error,Error}->
          ?LOGERROR("unable to remove segment ~p, reason ~p",[
            Merged,
            Error
          ])
      end;
    {error, Error} ->
      ?LOGERROR("error on merge commit ~p, error ~p",[
        dlss_segment:dirty_read( dlss_schema, Sgm ),
        Error
      ])
  end.


%---------Spawn a segment----------------------------------------
spawn_segment(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> spawn_segment( Segment );
    Error -> Error
  end;
spawn_segment(#sgm{str = Str, lvl = Lvl, key = Key} = Sgm)->

  % Obtain the segment name
  Segment=dlss_segment:dirty_read(dlss_schema,Sgm),

  % Get segment params
  Params = dlss_segment:get_info(Segment),

  % Generate an unique name within the storage
  ChildName=new_segment_name(Str),

  ?LOGINFO("create a new child segment ~p from ~p first ~p with params ~p",[
    ChildName,
    Segment,
    Key,
    Params
  ]),
  case dlss_backend:create_segment(ChildName,Params) of
    ok -> ok;
    { error, BackendError }->
      ?LOGERROR("unable to create a new child segment ~p from ~p with params ~p for storage ~p, error ~p",[
        ChildName,
        Segment,
        Params,
        Str,
        BackendError
      ]),
      ?ERROR(BackendError)
  end,

  % Add the segment to the schema
  case dlss:transaction(fun()->
    % Set a lock on the schema
    dlss_backend:lock({table,dlss_schema},read),
    % Add the segment
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ key=Key, lvl= Lvl + 1 }, ChildName, write )
  end) of
    {ok,ok} -> ok;
    Error -> Error
  end.

%---------Absorb parent segment----------------------------------------
absorb_parent(Segment)->
  case segment_by_name(Segment) of
    { ok, #sgm{ lvl = Lvl } } when Lvl =< 1->
      ?ERROR( not_low_level_segment );
    { ok, #sgm{ key = Key } = Sgm }->
      Parent = parent_segment( Segment ),

      % To avoid high peaks we queue processes that are absorbing their parent
      % the process actually do absorbing only if the parent starts with its range
      % of keys. As the previous segment finishes absorbing the parent does not have keys
      % from its range any more.

      case dlss_segment:dirty_first(Parent) of
        '$end_of_table'->
          ok;
        First when Key=:='_'; First>=Key->
          case next_sibling( Sgm ) of
            #sgm{ key = { Next } } when First<Next->
              % This is our queue
              Stop = dlss_segment:dirty_prev( Parent, Next ),
              ?LOGINFO("~p absorb parent ~p from ~p to ~p",[ Segment, Parent, First, Stop ]),
              absorb_parent( First, Stop, Parent, Segment, 0 ),
              ?LOGINFO("~p has finished absorbing parent ~p from ~p to ~p",[ Segment, Parent, First, Stop ]);
            undefined->
              % This is the last segment
              Stop = dlss_segment:dirty_last(Parent),
              ?LOGINFO("~p absorb parent ~p from ~p to ~p",[ Segment, Parent, First, Stop ]),
              absorb_parent( First, Stop, Parent, Segment, 0 ),
              ?LOGINFO("~p has finished absorbing the parent ~p from ~p to ~p",[ Segment, Parent, First, Stop ]);
            _->
              % It's not our queue yet
              ok
          end;
        _->
          % It's not our queue yet
          ok
      end
  end.

absorb_parent( '$end_of_table', _Stop, _Parent, _Segment, _Count )->
  ok;
absorb_parent( Key, Stop, Parent, Segment, Count )
  when Key =/='$end_of_table',Key =< Stop->

  ?LOGINFO("segment ~p parent ~p, count ~p",[Segment,Parent,Count]),
  Rows = dlss_segment:dirty_scan(Parent,Key,Stop,?BATCH_SIZE),

  % Copy to child
  [if
     V=:='@deleted@' ->
       ok = dlss_segment:dirty_delete( Segment, K );
     true ->
       ok = dlss_segment:dirty_write( Segment, K, V )
   end || {K,V} <-Rows],

  % Remove from parent
  [ ok = dlss_segment:dirty_delete( Parent, K ) || {K,_V} <-Rows],

  if
    length(Rows)>=?BATCH_SIZE ->
      {LastKey,_}=lists:last(Rows),
      absorb_parent(LastKey,Stop,Parent,Segment, Count + length(Rows) );
    true -> ok
  end;
absorb_parent( _Key, _Stop, _Parent, _Segment, _Count )->
  ok.

%---------Absorb a segment----------------------------------------
absorb_segment(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> absorb_segment( Segment );
    Error -> ?ERROR(Error)
  end;
absorb_segment(#sgm{lvl = 0})->
  % The root segment cannot be absorbed
  ?ERROR(root_segment);
absorb_segment(#sgm{str = Str} = Sgm)->

  % Obtain the segment name
  Name=dlss_segment:dirty_read(dlss_schema,Sgm),

  case dlss:transaction(fun()->

    % Set a lock on the schema while traversing segments
    dlss_backend:lock({table,dlss_schema},write),

    % Find all the children of the segment
    Children = get_children(Sgm),

    % Remove the absorbed segment from the dlss schema
    ok = dlss_segment:delete(dlss_schema, Sgm, write ),

    % Put all children segments level up
    [ begin
        ok = dlss_segment:write(dlss_schema, S#sgm{ lvl = S#sgm.lvl - 1 }, T , write ),
        ok = dlss_segment:delete(dlss_schema, S , write )
      end || { S, T } <- Children]
  end) of
    { ok, _} ->
      % Remove the segment from backend
      ?LOGINFO("removing segment ~p",[Name]),
      case dlss_backend:delete_segment(Name) of
        ok->ok;
        {error,Error}->
          ?LOGERROR("unable to remove segment ~p storage ~p, reason ~p",[
            Name,
            Str,
            Error
          ])
      end;
    { error, Error}->
      ?LOGERROR("error absorbing segment ~p storage ~p, error ~p",[
        Name,
        Str,
        Error
      ]),
      ?ERROR(Error)
  end.

%---------level up segment----------------------------------------
level_up(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> level_up( Segment );
    Error -> ?ERROR(Error)
  end;
level_up(#sgm{lvl = 0})->
  % The root segment cannot be absorbed
  ?ERROR(root_segment);
level_up(#sgm{} = Sgm)->

  % Get the parent
  #sgm{lvl = Level }=ParentSgm = parent_segment(Sgm),
  ParentName = dlss_segment:dirty_read(dlss_schema,ParentSgm),
  First = dlss_segment:dirty_first(ParentName),

  % Current segment
  Name=dlss_segment:dirty_read(dlss_schema,Sgm),

  ?LOGINFO("moving segment ~p from level ~p to level ~p, the split key ~p",[Name,Sgm#sgm.lvl,Level,First]),
  case dlss:transaction(fun()->

    dlss_backend:lock({table,dlss_schema},write),

    ok = dlss_segment:write(dlss_schema, ParentSgm#sgm{ key = { First } }, ParentName , write ),
    ok = dlss_segment:delete(dlss_schema, ParentSgm , write ),

    ok = dlss_segment:write(dlss_schema, Sgm#sgm{ lvl = Level }, Name , write ),
    ok = dlss_segment:delete(dlss_schema, Sgm , write )

  end) of
    { ok, _} ->
      ok;
    { error, Error}->
      ?LOGERROR("error to level up segment ~p, error ~p",[
        Name,
        Error
      ]),
      ?ERROR(Error)
  end.

%------------Get children segments-----------------------------------------
get_children(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> get_children(Segment);
    Error -> Error
  end;
get_children(#sgm{str = Storage,lvl = Level})->
  LevelDown = math:floor( Level ) + 1,
  MS=[{
    #kv{key = #sgm{str = Storage,key = '_',lvl = LevelDown}, value = '$1'},
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

%%=================================================================
%%	Iterate
%%=================================================================

%---------FIRST-----------------------
first(Storage)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_first(Segment,Storage) end,
  % The scanning starts at the lowest level
  Lowest = #sgm{ str = Storage, key = '~', lvl = '_' },
  Segments = key_segments( parent_segment(Lowest),[]),
  first(Segments,Iter,'$end_of_table').

dirty_first(Storage)->
  % Dirty iterator
  Iter = fun(Segment)->dlss_segment:dirty_first(Segment) end,
  % The scanning starts at the lowest level
  Lowest = #sgm{ str = Storage, key = '~', lvl = '_' },
  Segments = key_segments( parent_segment(Lowest),[]),
  first(Segments,Iter,'$end_of_table').

first([S1|Rest],Iter,Acc)->
  F = Iter( S1 ),
  Acc1= next_acc( F, Acc ),
  first(Rest,Iter,Acc1);
first([],_Iter,F)-> F.

safe_first(Segment,Storage)->
  case dlss_segment:first(Segment) of
    '$end_of_table' -> '$end_of_table';
    First ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dirty_read(Storage,First) of
        not_found -> safe_next(Segment,Storage,First);
        _->First
      end
  end.
%---------LAST------------------------
last(Storage)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_last(Segment,Storage) end,
  % The scanning starts at the lowest level
  Highest = #sgm{ str = Storage, key = [], lvl = '_' },
  Segments = key_segments( parent_segment(Highest),[]),
  last(Segments,Iter,'$end_of_table').

dirty_last(Storage)->
  % Dirty iterator
  Iter = fun(Segment)->dlss_segment:dirty_last(Segment) end,
  % The scanning starts at the lowest level
  Highest = #sgm{ str = Storage, key = [], lvl = '_' },
  Segments = key_segments( parent_segment(Highest),[]),
  last(Segments,Iter,'$end_of_table').

last([S1|Rest],Iter, Acc)->
  L = Iter( S1 ),
  Acc1 = prev_acc( L, Acc ),
  last(Rest,Iter,Acc1);
last([],_Iter,L)-> L.

safe_last(Segment,Storage)->
  case dlss_segment:last(Segment) of
    '$end_of_table' -> '$end_of_table';
    Last ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dirty_read(Storage,Last) of
        not_found -> safe_prev(Segment,Storage,Last);
        _->Last
      end
  end.

%---------NEXT------------------------
next( Storage, Key )->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_next(Segment,Storage,Key) end,
  % Schema starting point
  Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
  next( parent_segment(Lowest), Iter, '$end_of_table' ).

dirty_next(Storage,Key)->
  % The iterator
  Iter = fun(Segment)->dlss_segment:dirty_next(Segment,Key) end,
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

safe_next(Segment,Storage,Key)->
  case dlss_segment:next(Segment,Key) of
    '$end_of_table' -> '$end_of_table';
    Next ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dirty_read(Storage,Next) of
        not_found ->
          safe_next(Segment,Storage,Next);
        _->
          Next
      end
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
  Iter = fun(Segment)-> safe_prev(Segment,Storage,Key) end,
  % Schema starting point
  Lowest = #sgm{ str = Storage, key = { Key }, lvl = '_' },
  prev( parent_segment(Lowest), Iter, '$end_of_table' ).

dirty_prev(Storage,Key)->
  % The iterator
  Iter = fun(Segment)->dlss_segment:dirty_prev(Segment,Key) end,
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

safe_prev(Segment,Storage,Key)->
  case dlss_segment:prev(Segment,Key) of
    '$end_of_table' -> '$end_of_table';
    Prev ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dirty_read(Storage,Prev) of
        not_found ->
          safe_prev(Segment,Storage,Prev);
        _->
          Prev
      end
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

  % scan each segment
  Results =
    [ { L, scan_segment( S, StartKey, EndKey, Limit) } || [L,_Key, S] <- OrderedSegments ],

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
    true ->
      % If we are here then there were deleted records in the result that prevented us
      % from getting the full result, we need to keep searching.
      % Continue from the last key in the full result
      { Head, [ { LastKey,_} ] } = lists:split( length( Filtered )-1, Filtered ),
      Head ++ lists:sublist( dirty_range_select( Storage, LastKey, EndKey, Limit ), Limit - length(Head) )
  end.

find_head_segments( Storage, Key )->
  ToGuard=
    if
      Key =:= '$end_of_table' -> [];
      true -> [{'=<','$1',{const, { Key }}}]
    end,
  MS=[{
    #kv{key = #sgm{str=Storage, key='$1', lvl='$2'}, value='$3'},
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
    _:Error->?ERROR(Error)
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




