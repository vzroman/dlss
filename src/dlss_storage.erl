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
%%	STORAGE SEGMENT API
%%=================================================================
-export([
  %-----Service API-------
  get_storages/0,
  get_segments/0,get_segments/1,
  add/2,
  remove/1

  %-----Read/Write API
]).

%%-----------------------------------------------------------------
%%  Service API
%%-----------------------------------------------------------------
get_storages()->
  Start=#sgm{str='_',key='_',lvl = '_'},
  get_storages(dlss_segment:dirty_next(dlss_schema,Start),[]).
get_storages(#sgm{str = Str}=Sgm,[Str|_]=Acc)->
  get_storages(dlss_segment:dirty_next(dlss_schema,Sgm),Acc);
get_storages(#sgm{str = Str}=Sgm,Acc)->
  get_storages(dlss_segment:dirty_next(dlss_schema,Sgm),[Str|Acc]);
get_storages(_Sgm,Acc)->
  lists:reverse(Acc).

get_segments()->
  Start=#sgm{str='_',key='_',lvl = '_'},
  get_segments('_',dlss_segment:dirty_next(dlss_schema,Start),[]).

get_segments(Storage)->
  Start=#sgm{str=Storage,key='_',lvl = -1},
  get_segments(Storage,dlss_segment:dirty_next(dlss_schema,Start),[]).

get_segments(Storage,#sgm{str = Str}=Sgm,Acc)
  when Str=:=Storage;Storage=:='_'->
  Table=dlss_segment:dirty_read(dlss_schema,Sgm),
  get_segments(Storage,dlss_segment:dirty_next(dlss_schema,Sgm),[Table|Acc]);
get_segments(_Storage,_Sgm,Acc)->
  lists:reverse(Acc).

%---------Create a new storage----------------------------------------
add(Name,Type)->
  add(Name,Type,#{}).
add(Name,Type,Options)->

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
  ok=dlss_segment:dirty_write(dlss_schema,#sgm{str=Name,lvl=0,key='_'},Root).

remove(Name)->
  ?LOGWARNING("removing storage ~p",[Name]),
  Start=#sgm{str=Name,key='_',lvl = '_'},
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



