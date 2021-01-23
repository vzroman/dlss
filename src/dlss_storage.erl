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

-record(sgm,{str,key,lvl}).

-define(BATCH_SIZE,1000).

%%=================================================================
%%	STORAGE SERVICE API
%%=================================================================
-export([
  %-----Service API-------
  is_storage/1,
  get_storages/0,
  get_segments/0,get_segments/1,
  new_root_segment/1,
  root_segment/1,
  segment_params/1,
  add/2,add/3,
  remove/1,
  get_type/1,
  is_local/1,
  spawn_segment/1,spawn_segment/2,
  hog_parent/1,
  absorb_segment/1,
  get_children/1,
  parent_segment/1
]).

%%=================================================================
%%	STORAGE READ/WRITE API
%%=================================================================
-export([
  read/2,read/3,dirty_read/2,
  write/3,write/4,dirty_write/3,
  delete/2,delete/3,dirty_delete/2,
  scan_interval/3, scan_interval/4
]).

%%=================================================================
%%	STORAGE ITERATOR API
%%=================================================================
-export([
  first/1,dirty_first/1,
  last/1,dirty_last/1,
  next/2,dirty_next/2,
  prev/2,dirty_prev/2
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
    { ok, #sgm{ str = Str, lvl = Lvl, key = Key } }->
      % The start key except for '_' is wrapped into a tuple
      % to make the schema properly ordered by start keys
      StartKey =
        case Key of
          { K } -> K;
          _-> Key
        end,
      { ok, #{ storage => Str, level => Lvl, key => StartKey } };
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

%---------Spawn a segment----------------------------------------
spawn_segment(Segment) ->
  spawn_segment(Segment,'$start_of_table').
spawn_segment(Name, SplitKey) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> spawn_segment( Segment, SplitKey );
    Error -> Error
  end;
spawn_segment(#sgm{key = { Key } }, SplitKey)
  when SplitKey =/= '$start_of_table', SplitKey < Key ->
  % The splitting cannot be less than the start key of the storage
  ?ERROR({invalid_split_key, SplitKey});
spawn_segment(#sgm{str = Str, lvl = Lvl, key = Key} = Sgm, SplitKey)->

  % Obtain the segment name
  Segment=dlss_segment:dirty_read(dlss_schema,Sgm),

  % Get segment params
  Params = dlss_segment:get_info(Segment),

  % Generate an unique name within the storage
  ChildName=new_segment_name(Str),

  ?LOGINFO("create a new child segment ~p from ~p with params ~p",[
    ChildName,
    Segment,
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

  StartKey=
    if
      SplitKey =:='$start_of_table' -> Key ;
      true -> { SplitKey }
    end,

  % Add the segment to the schema
  case dlss:transaction(fun()->
    % Set a lock on the schema
    dlss_backend:lock({table,dlss_schema},read),
    % Add the segment
    ok = dlss_segment:write( dlss_schema, Sgm#sgm{ key=StartKey, lvl= Lvl + 1 }, ChildName, write )
  end) of
    {ok,ok} -> ok;
    Error -> Error
  end.

%---------Hog parent segment----------------------------------------
hog_parent(Segment)->
  case segment_by_name(Segment) of
    { ok, #sgm{ lvl = Lvl } } when Lvl =< 1->
      ?ERROR( not_low_level_segment );
    { ok, #sgm{ key = Key } = Sgm }->
      Parent = parent_segment( Segment ),
      % Start key
      Start=
        case Key of
          '_' -> dlss_segment:dirty_first(Parent);
          { K } -> K
        end,
      % End Key
      Stop=
        case next_sibling( Sgm ) of
          #sgm{ key = { Next } }-> dlss_segment:dirty_prev( Parent, Next );
          _-> dlss_segment:dirty_last(Parent)
        end,
      if
        Stop=:='$end_of_table' ->
          % if we are here the the parent is either empty or there are
          % only the next sibling's keys
          ok;
        true ->
          ?LOGINFO("~p hog parent ~p from ~p to ~p",[ Segment, Parent, Start, Stop ]),
          hog_parent( Start, Stop, Parent, Segment ),
          ?LOGINFO("~p hog has finished hogging the parent ~p from ~p to ~p",[ Segment, Parent, Start, Stop ])
      end
  end.

hog_parent( '$end_of_table', _Stop, _Parent, _Segment )->
  ok;
hog_parent( Key, Stop, Parent, Segment )
  when Key =/='$end_of_table',Key =< Stop->

  Rows = dlss_segment:dirty_scan(Parent,Key,Stop,?BATCH_SIZE),
  [if
     V=:='@deleted@' ->
       ok = dlss_segment:dirty_delete( Segment, K ),
       ok = dlss_segment:dirty_delete( Parent, K );
     true ->
       ok = dlss_segment:dirty_write( Segment, K, V ),
       ok = dlss_segment:dirty_delete( Parent, K )
   end || {K,V} <-Rows],
  if
    length(Rows)>=?BATCH_SIZE ->
      {LastKey,_}=lists:last(Rows),
      hog_parent(LastKey,Stop,Parent,Segment);
    true -> ok
  end;
hog_parent( _Key, _Stop, _Parent, _Segment )->
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

%------------Get children segments-----------------------------------------
get_children(Name) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> get_children(Segment);
    Error -> Error
  end;
get_children(Sgm)->
  get_children(dlss_segment:dirty_next(dlss_schema,Sgm),Sgm,[]).

get_children(#sgm{ str = S, lvl = NextLvl },#sgm{ str = S, lvl = Lvl}, Acc)
  when NextLvl =< Lvl->
  lists:reverse(Acc);
get_children(#sgm{ str = S } = Next, #sgm{str = S} = Sgm,Acc)->
  Table = dlss_segment:dirty_read( dlss_schema, Next ),
  get_children(dlss_segment:dirty_next(dlss_schema,Next),Sgm,[{Next,Table}|Acc]);
get_children(_Other,_Sgm,Acc)->
  % next storage or '$end_of_table'
  lists:reverse(Acc).

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
  % The value is replaced with the special flag,
  % Actual delete is performed during rebalancing
  write( Storage, Key, '@deleted@', Lock ).
dirty_delete(Storage, Key)->
  dirty_write( Storage, Key, '@deleted@' ).

%-------------Interval scan----------------------------------------------
scan_interval(Storage, StartKey, EndKey) ->
  scan_interval(Storage, StartKey, EndKey, infinity).

scan_interval(_Storage, StartKey, EndKey, _Limit)
    when StartKey > EndKey andalso
        (StartKey =/= '$start_of_table' andalso StartKey =/= '$end_of_table') ->
  [];
scan_interval(Storage, StartKey, EndKey, _Limit)
    when StartKey == EndKey andalso
        (StartKey =/= '$start_of_table' andalso StartKey =/= '$end_of_table') ->
  case dirty_read(Storage, StartKey) of
    not_found ->
      [];
    Value ->
      [{StartKey, Value}]
  end;
scan_interval(Storage, StartKey, EndKey, Limit) ->
  Segments = find_all_segments(Storage, StartKey, EndKey),
  SortedSegments = lists:sort(fun level_sort_fun/2, Segments),
  SortedValues = lists:map(
    fun([S, _Level]) -> 
      dlss_segment:dirty_scan(S, StartKey, EndKey, Limit)
    end,
    SortedSegments
  ),
  Full = lists:foldl(
    fun(Values, OrddictAcc) ->
      lists:foldl(
        fun
          ({DeletedKey, '@deleted@'}, DictIn) ->
            orddict:erase(DeletedKey, DictIn);
          ({Key, Value}, DictIn) ->
            orddict:store(Key, Value, DictIn)
        end,
        OrddictAcc,
        Values
      )
    end,
    orddict:new(),
    SortedValues
  ),
  limit_output(orddict:to_list(Full), Limit).

limit_output(Values, infinity) ->
  Values;
limit_output(Values, Limit) when Limit > 0 ->
  {First, _} = lists:split(min(Limit, length(Values)), Values),
  First.

level_sort_fun([_, Level0], [_, Level1]) ->
  Level1 < Level0.

find_all_segments(Storage, StartKey, EndKey) ->
  MatchHead = #kv{key = #sgm{str=Storage, key='$2', lvl='$3'}, value='$1'},
  Result = ['$1', '$2', '$3'],
  MatchSpec = [{MatchHead, [], [Result]}],
  AllSegments = mnesia:dirty_select(dlss_schema, MatchSpec),
  SegmentsPerLvl = lists:foldl(
    fun([SegmentName, Key, Lvl], Acc) ->
        maps:update_with(
          Lvl,
          fun(ExistingSet) -> ordsets:add_element({SegmentName, Key}, ExistingSet) end,
          ordsets:from_list([{SegmentName, Key}]),
          Acc
        )
    end,
    #{},
    AllSegments
  ),
  maps:fold(
    fun(Lvl, OrdSet, AccIn) ->
      OrderedKeys = ordsets:to_list(OrdSet),
      FilteredSegments = filter_segments(OrderedKeys, {StartKey}, {EndKey}, Lvl),
      lists:append(FilteredSegments, AccIn)
    end,
    [],
    SegmentsPerLvl
  ).

filter_segments([{Segment, _Key} | Rest], {'$start_of_table'}, EndKey, Lvl) ->
  do_filter_segments(Rest, {'$start_of_table'}, EndKey, [[Segment, Lvl]], Lvl);
filter_segments(Segments, StartKey, EndKey, Lvl) ->
  do_filter_segments(Segments, StartKey, EndKey, [], Lvl).
  
do_filter_segments([{Segment, _LastKey}], _, {'$end_of_table'}, AccIn, Lvl) ->
  lists:reverse([[Segment, Lvl] | AccIn]);
do_filter_segments([{Segment, LastKey}], _, EndKey, AccIn, Lvl) 
    when EndKey >= LastKey ->
  lists:reverse([[Segment, Lvl] | AccIn]);
do_filter_segments([_], _, _, AccIn, _) ->
  lists:reverse(AccIn);
do_filter_segments([{FSeg, _} | RestKeys], {'$start_of_table'}, {'$end_of_table'}, AccIn, Lvl) ->
  NewAcc = [[FSeg, Lvl] | AccIn],
  do_filter_segments(RestKeys, {'$start_of_table'}, {'$end_of_table'}, NewAcc, Lvl);
do_filter_segments([{FSeg, FKey}, {SSeg, SKey} | RestKeys], {'$start_of_table'}, EndKey, AccIn, Lvl)
    when EndKey >= FKey ->
  NewAcc = [[FSeg, Lvl] | AccIn],
  do_filter_segments([{SSeg, SKey} | RestKeys], {'$start_of_table'}, EndKey, NewAcc, Lvl);
do_filter_segments(_, {'$start_of_table'}, _EndKey, AccIn, _Lvl) ->
  lists:reverse(AccIn);
do_filter_segments([_, {SSeg, SKey} | RestKeys], StartKey, {'$end_of_table'}, AccIn, Lvl)
    when StartKey >= SKey ->
  do_filter_segments([{SSeg, SKey} | RestKeys], StartKey, {'$end_of_table'}, AccIn, Lvl);
do_filter_segments([{FSeg, _FKey} | RestKeys], StartKey, {'$end_of_table'}, AccIn, Lvl) ->
  NewAcc = [[FSeg, Lvl] | AccIn],
  do_filter_segments(RestKeys, StartKey, {'$end_of_table'}, NewAcc, Lvl);
do_filter_segments([{FSeg, FKey}, {SSeg, SKey} | RestKeys], StartKey, EndKey, AccIn, Lvl)
    when StartKey =< SKey andalso StartKey >= FKey ->
  NewAcc = [[FSeg, Lvl] | AccIn],
  do_filter_segments([{SSeg, SKey} | RestKeys], StartKey, EndKey, NewAcc, Lvl);
do_filter_segments([{FSeg, FKey}, {SSeg, SKey} | RestKeys], StartKey, EndKey, AccIn, Lvl)
    when EndKey =< SKey andalso EndKey >= FKey ->
  NewAcc = [[FSeg, Lvl] | AccIn],
  do_filter_segments([{SSeg, SKey} | RestKeys], StartKey, EndKey, NewAcc, Lvl);
do_filter_segments([_, {SSeg, SKey} | RestKeys], StartKey, EndKey, AccIn, Lvl) ->
  do_filter_segments([{SSeg, SKey} | RestKeys], StartKey, EndKey, AccIn, Lvl).

%%=================================================================
%%	Iterate
%%=================================================================

%---------FIRST-----------------------
first(Storage)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The scanning starts at the lowest level
  Lowest = #sgm{ str = Storage, key = '~', lvl = '_' },
  Segments = key_segments( parent_segment(Lowest),[]),
  first(Segments,'$end_of_table').

dirty_first(Storage)->
  % The scanning starts at the lowest level
  Lowest = #sgm{ str = Storage, key = '~', lvl = '_' },
  Segments = key_segments( parent_segment(Lowest),[]),
  dirty_first(Segments,'$end_of_table').

first([S1|Rest],Acc)->
  F = dlss_segment:first(S1),
  Acc1= next_acc( F, Acc ),
  first(Rest,Acc1);
first([],F)-> F.

dirty_first([S1|Rest], Acc)->
  F = dlss_segment:dirty_first(S1),
  Acc1 = next_acc(F, Acc),
  dirty_first(Rest,Acc1);
dirty_first([],F)-> F.

%---------LAST------------------------
last(Storage)->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The scanning starts at the lowest level
  Highest = #sgm{ str = Storage, key = [], lvl = '_' },
  Segments = key_segments( parent_segment(Highest),[]),
  last(Segments,'$end_of_table').

dirty_last(Storage)->
  % The scanning starts at the lowest level
  Highest = #sgm{ str = Storage, key = [], lvl = '_' },
  Segments = key_segments( parent_segment(Highest),[]),
  dirty_last(Segments,'$end_of_table').

last([S1|Rest], Acc)->
  L = dlss_segment:last(S1),
  Acc1 = prev_acc( L, Acc ),
  last(Rest,Acc1);
last([],L)-> L.

dirty_last([S1|Rest], Acc)->
  L = dlss_segment:dirty_last(S1),
  Acc1 = prev_acc( L, Acc),
  dirty_last(Rest,Acc1);
dirty_last([],L)-> L.

%---------NEXT------------------------
next( Storage, Key )->
  % Set a lock on the schema
  dlss_backend:lock({table,dlss_schema},read),
  % The safe iterator
  Iter = fun(Segment)-> safe_next(Segment,Key) end,
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

safe_next(Segment,Key)->
  case dlss_segment:next(Segment,Key) of
    '$end_of_table' -> '$end_of_table';
    Next ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dlss_segment:dirty_read(Segment,Next) of
        '@deleted@' -> safe_next(Segment,Next);
        _->Next
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
  Iter = fun(Segment)-> safe_prev(Segment,Key) end,
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

safe_prev(Segment,Key)->
  case dlss_segment:prev(Segment,Key) of
    '$end_of_table' -> '$end_of_table';
    Prev ->
      % In the safe mode we check if the key is already delete.
      % As 'next' has already locked the table, we can do it in dirty mode
      case dlss_segment:dirty_read(Segment,Prev) of
        '@deleted@' -> safe_prev(Segment,Prev);
        _->Prev
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




