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

-record(sgm,{str,lvl,tbl,key,rt}).

%%=================================================================
%%	STORAGE SEGMENT API
%%=================================================================
-export([
  %-----Service API-------
  get_storages/0,
  get_segments/0,get_segments/1

  %-----Read/Write API
]).

%%-----------------------------------------------------------------
%%  Service API
%%-----------------------------------------------------------------
get_storages()->
  Start=#sgm{str='_',lvl = '_',tbl='_',key='_',rt='_'},
  get_storages(dlss_segment:dirty_next(dlss_schema,Start),[]).
get_storages(#sgm{str = Str}=Sgm,Acc)->
  get_storages(dlss_segment:dirty_next(dlss_schema,Sgm),[Str|Acc]);
get_storages(_Sgm,Acc)->
  lists:reverse(Acc).

get_segments()->
  Start=#sgm{str='_',lvl = '_',tbl='_',key='_',rt='_'},
  get_segments('_',dlss_segment:dirty_next(dlss_schema,Start),[]).

get_segments(Storage)->
  Start=#sgm{str=Storage,lvl = '_',tbl='_',key='_',rt='_'},
  get_segments(Storage,dlss_segment:dirty_next(dlss_schema,Start),[]).

get_segments(Storage,#sgm{str = Str,tbl=Table}=Sgm,Acc)
  when Str=:=Storage;Storage=:='_'->
  get_segments(Storage,dlss_segment:dirty_next(dlss_schema,Sgm),[Table|Acc]);
get_segments(_Storage,_Sgm,Acc)->
  lists:reverse(Acc).


