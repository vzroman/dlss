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

%%=================================================================
%%	STORAGE SERVICE API
%%=================================================================
-export([
  %-----Service API-------
  get_storages/0,
  get_segments/0,get_segments/1,
  root_segment/1,
  segment_params/1,
  add/2,
  remove/1,
  get_type/1,
  spawn_segment/1,spawn_segment/2
]).

%%=================================================================
%%	STORAGE READ/WRITE API
%%=================================================================
-export([
  dirty_read/2
]).

%%-----------------------------------------------------------------
%%  Service API
%%-----------------------------------------------------------------
get_storages()->
  MS=[{
    #kv{key = #sgm{str = '$1',key = '_',lvl = 0},value = '_'},
    [],
    ['$1']
  }],
  mnesia:dirty_select(dlss_schema,MS).

get_segments()->
  MS=[{
    #kv{key = #sgm{str = '_',key = '_',lvl = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  mnesia:dirty_select(dlss_schema,MS).


get_segments(Storage)->
  MS=[{
    #kv{key = #sgm{str = Storage,key = '_',lvl = '_'}, value = '$1'},
    [],
    ['$1']
  }],
  mnesia:dirty_select(dlss_schema,MS).

root_segment(Storage)->
  case dlss_segment:dirty_read(dlss_schema,#sgm{str=Storage,key = '_',lvl = 0}) of
    not_found->{error,invalid_storage};
    Segment->{ok,Segment}
  end.

get_type(Storage)->
  {ok,Root}=root_segment(Storage),
  #{ type:= T }=dlss_segment:get_info(Root),
  T.

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
  case root_segment(Name) of
    {ok,_}->?ERROR(already_exists);
    _->ok
  end,

  % Default options
  Attributes=table_attributes(Type,maps:merge(#{
    nodes=>[node()],
    local=>false
  },Options)),

   % Generate an unique name within the storage
  Root=new_segment_name(Name),

  ?LOGINFO("create a new storage ~p of type ~p with root segment ~p with attributes ~p",[
    Name,
    Type,
    Root,
    Attributes
  ]),
  case mnesia:create_table(Root,[
    {attributes,record_info(fields,kv)},
    {record_name,kv},
    {type,ordered_set}|
    Attributes
  ]) of
    {atomic,ok}->ok;
    {aborted,Reason}->
      ?LOGERROR("unable to create a root segment ~p of type ~p with attributes ~p for storage ~p, error ~p",[
        Root,
        Type,
        Attributes,
        Name,
        Reason
      ]),
      ?ERROR(Reason)
  end,

  % Add the storage to the schema
  ok=dlss_segment:dirty_write(dlss_schema,#sgm{str=Name,key='_',lvl=0},Root).

remove(Name)->
  ?LOGWARNING("removing storage ~p",[Name]),
  Start=#sgm{str=Name,key='_',lvl = -1},
  remove(Name,dlss_segment:dirty_next(dlss_schema,Start)).

remove(Storage,#sgm{str=Storage}=Sgm)->
  Table=dlss_segment:dirty_read(dlss_schema,Sgm),
  ?LOGWARNING("removing segment ~p storage ~p",[Table,Storage]),
  ok=dlss_segment:dirty_delete(dlss_schema,Sgm),

  case mnesia:delete_table(Table) of
    {atomic,ok}->ok;
    {aborted,Reason}->
      ?LOGERROR("unable to remove segment ~p storage ~p, reason ~p",[
        Table,
        Storage,
        Reason
      ])
  end,
  reset_id(Storage),
  remove(Storage,dlss_segment:dirty_next(dlss_schema,Sgm));
remove(Storage,_Sgm)->
  ?LOGINFO("storage ~p removed",[Storage]).

%---------Create/Remove a segment----------------------------------------
spawn_segment(Segment) ->
  spawn_segment(Segment,'$start_of_table').
spawn_segment(Name, FromKey) when is_atom(Name)->
  case segment_by_name(Name) of
    { ok, Segment }-> spawn_segment( Segment, FromKey );
    Error -> Error
  end;
spawn_segment(#sgm{str = Str, lvl = Lvl, key = Key} = Sgm, FromKey)->

  % Obtain the segment name
  Segment=dlss_segment:dirty_read(dlss_schema,Sgm),

  % Get segment params
  Params = #{ type:=Type } = dlss_segment:get_info(Segment),

  % Default options
  Attributes=table_attributes(Type,Params),

  % Generate an unique name within the storage
  ChildName=new_segment_name(Str),

  ?LOGINFO("create a new child segment ~p from ~p with attributes ~p",[
    ChildName,
    Segment,
    Attributes
  ]),
  case mnesia:create_table(ChildName,[
    {attributes,record_info(fields,kv)},
    {record_name,kv},
    {type,ordered_set}|
    Attributes
  ]) of
    {atomic,ok}->ok;
    {aborted,Reason}->
      ?LOGERROR("unable to create a new child segment ~p from ~p with attributes ~p for storage ~p, error ~p",[
        ChildName,
        Segment,
        Attributes,
        Str,
        Reason
      ]),
      ?ERROR(Reason)
  end,

  StartKey=
    if
      FromKey =:='$start_of_table' -> Key ;
      true -> { FromKey }
    end,

  % Add the segment to the schema
  ok = dlss_segment:dirty_write( dlss_schema, Sgm#sgm{ key=StartKey, lvl= Lvl + 1 }, ChildName ).




%%=================================================================
%%	Read/Write
%%=================================================================
dirty_read(Storage,Key)->

  %Segments=key_segments(Storage,Key),

  ok.
%%=================================================================
%%	Internal stuff
%%=================================================================
new_segment_name(Storage)->
  Id=get_unique_id(Storage),
  Name="dlss_"++atom_to_list(Storage)++"_"++integer_to_list(Id),
  list_to_atom(Name).

get_unique_id(Storage)->
  mnesia:dirty_update_counter(dlss_schema,{id,Storage},1).
reset_id(Storage)->
  mnesia:dirty_delete(dlss_schema,{id,Storage}).


table_attributes(Type,#{
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

segment_by_name(Name)->
  MS=[{
    #kv{key = '$1', value = Name},
    [],
    ['$1']
  }],
  case mnesia:dirty_select(dlss_schema,MS) of
    [Key]->{ ok, Key };
    _-> { error, not_found }
  end.




