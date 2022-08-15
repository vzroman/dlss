 %%----------------------------------------------------------------
%% Copyright (c) 2022 Faceplate
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

-module(dlss_copy).

-include("dlss.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  copy/2,copy/3
]).

copy( Source, Target )->
  copy( Source, Target, #{}).
copy( Source, Target, Options0 )->
  Options = maps:merge(#{
    start_key =>undefined,
    end_key => undefined,
    not_match => undefined,
    hash => <<>>,
    sync => false,
    attempts => 3
  }, Options0),

  #{ type := Type } = dlss_segment:get_info(Source),
  Module =
    if
      Type =:= disc -> dlss_copy_disc;
      true -> dlss_copy_ram
    end,

  ReadNode = get_read_node( Source),
  if
    ReadNode =:= node() ->
      ?LOGINFO("~p copy to ~p, module ~p, options ~p",[Source, Target, Module, Options]),
      local_copy(Source, Target, Module, Options );
    true ->
      AccessMode = mnesia:table_info(Source, access_mode),
      if
        AccessMode =:= read_only->
          ?LOGINFO("~p passive copy to ~p, module ~p, options ~p",[Source, Target, Module, Options]),
          remote_copy(Source, Target, Module, Options );
        true->
          ?LOGINFO("~p active copy to ~p, module ~p, options ~p",[Source, Target, Module, Options]),
          active_copy(Source, Target, Module, Options )
      end
  end.

get_read_node( Table )->
  Node = mnesia:table_info( Table, where_to_read ),
  if
    Node =:= nowhere ->throw({unavailable,Table});
    true-> Node
  end.

local_copy( Source, Target, Module, Options)->

  TargetRef = Module:init_target( Target, Options ),

  OnBatch =
    fun(Batch, Hash)->
      ?LOGDEBUG("~p write batch",[Target]),
      Module:write_batch(Batch, TargetRef),
      crypto:hash_update(Hash, term_to_binary( Batch ))
    end,

  SourceRef = Module:init_source( Source, OnBatch, Options ),



  FinalHash = do_copy(SourceRef, Module, Options ),
  Module:dump_target( TargetRef ),

  ?LOGINFO("finish local copying: source ~p, target ~p, hash ~ts", Source, Target, base64:encode( FinalHash )),

  FinalHash.

do_copy(SourceRef, Module, #{
  hash := InitHash0
})->

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  FinalHash =
    case Module:fold(SourceRef, {[], InitHash, 0}) of

    end,

  crypto:hash_final( FinalHash ).







