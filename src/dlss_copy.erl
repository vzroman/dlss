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

%%=================================================================
%%	Remote API
%%=================================================================
-export([
  remote_copy_request/5
]).

-define(PROPS,[
  type,
  user_properties,
  storage_properties,
  record_name,
  load_order,
  access_mode,
  majority,
  index,
  local_content,
  attributes,
  version
]).

init_props( Source )->
  All =
    maps:from_list(mnesia:table_info( Source, all )),
  maps:to_list( maps:with(?PROPS,All)).


copy( Source, Target )->
  copy( Source, Target, #{}).
copy( Source, Target, Options0 )->
  Options = maps:merge(#{
    start_key =>undefined,
    end_key => undefined,
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

  ?LOGINFO("finish local copying: source ~p, target ~p, hash ~ts",[Source, Target, base64:encode( FinalHash )]),

  FinalHash.

remote_copy( Source, Target, Module, Options)->

  Trap = process_flag(trap_exit,true),

  FinalHash =
    try  do_remote_copy( Source, Target, Module, Options )
    after
      process_flag(trap_exit,Trap)
    end,

  ?LOGINFO("finish remote copying: source ~p, target ~p, hash ~ts", [Source, Target, base64:encode( FinalHash )] ),

  FinalHash.

do_remote_copy( Source, Target, Module, #{
  hash := InitHash0,
  attempts := Attempts
} = Options )->

  ReadNode = get_read_node( Source ),
  TargetRef = Module:init_target( Target, init_props( Source ) ),

  Self = self(),
  OnBatch =
   fun(Batch0, Hash0)->
     Batch = term_to_binary( Batch0 ),
     Hash = crypto:hash_update(Hash0, Batch),
     Zip = zlib:zip( Batch ),
     Self ! {write_batch, self(), Zip, crypto:hash_final( Hash)},
     receive
       {confirmed, Self}-> Hash
     end
   end,

  Worker = spawn_link(ReadNode, ?MODULE, remote_copy_request,[Self,Source,Module,OnBatch,Options]),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  ?LOGINFO("copy ~p from ~p...",[Source,ReadNode]),

  FinalHash =
    try remote_copy_loop(Worker, Module, TargetRef, InitHash)
    catch
      _:Error->
        Module:rollback_target( TargetRef ),
        case Error of
          invalid_hash->
            ?LOGERROR("~p invalid remote hash from ~p, left attempts ~p",[Source,ReadNode,Attempts-1]);
          {interrupted,Reason}->
            ?LOGERROR("~p copying from ~p interrupted, reason ~p, left attempts ~p",[Source,ReadNode,Reason,Attempts-1]);
          Other->
            ?LOGERROR("unexpected error on copying ~p from ~p, error ~p, no attempts left",[Source,ReadNode,Other]),
            throw(Other)
        end,
        if
          Attempts > 0->
            do_remote_copy( Source, Target, Module, Options#{ attempts => Attempts -1});
          true->
            throw( Error )
        end
    after
      exit(Worker,shutdown)
    end,

  Module:dump_target( TargetRef ),

  FinalHash.

remote_copy_request(Owner, Source, Module, OnBatch, Options)->

  ?LOGINFO("remote copy request on ~p, options ~p", [Source, Options]),

  SourceRef = Module:init_source( Source, OnBatch, Options ),

  FinalHash = do_copy( SourceRef, Module, Options ),

  ?LOGINFO("finish remote copy request on ~p",[Source]),

  unlink( Owner ),
  Owner ! {finish,self(),FinalHash}.

remote_copy_loop(Worker, Module, TargetRef, Hash0)->
  receive
     {write_batch, Worker, Zip, WorkerHash }->
       Batch = zlib:unzip( Zip ),
       Hash = crypto:hash_update(Hash0, Batch),
       case crypto:hash_final(Hash) of
         WorkerHash ->
           Worker ! {confirmed, self()},
           Module:write_batch(binary_to_term(Batch), TargetRef);
         _->
           throw(invalid_hash)
       end,
       remote_copy_loop(Worker, Module, TargetRef, Hash);
     {finish,Worker,WorkerFinalHash}->
       case crypto:hash_final(Hash0) of
         WorkerFinalHash -> WorkerFinalHash;
         _-> throw(invalid_hash)
       end;
     {'EXIT',Worker,Reason}->
       throw({interrupted,Reason});
     {'EXIT',_Other,Reason}->
       throw({exit,Reason})
 end.


active_copy(Source, Target, Module, Options )->
  % TODO
  remote_copy(Source, Target, Module, Options).

do_copy(SourceRef, Module, #{
  hash := InitHash0
})->

   InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

   FinalHash = Module:fold(SourceRef, {[], InitHash, 0}),

   crypto:hash_final( FinalHash ).



