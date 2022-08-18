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
-include("dlss_copy.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  copy/2,copy/3,
  split/2, split/3,
  get_size/1
]).

%%=================================================================
%%	Remote API
%%=================================================================
-export([
  remote_copy_request/5,
  remote_batch/3
]).

-export([
  debug/2
]).

-record(acc,{acc, module, batch, size, on_batch, stop }).
-record(r_acc,{i, module, head, tail, h_key, t_key, hash}).

 -define(OPTIONS(O),maps:merge(#{
   start_key =>undefined,
   end_key => undefined,
   hash => <<>>,
   sync => false,
   attempts => 3
 }, O)).

%%-----------------------------------------------------------------
%%  Utilities
%%-----------------------------------------------------------------
get_module( Table )->
  #{ type := Type } = dlss_segment:get_info(Table),
  if
    Type =:= disc -> dlss_copy_disc;
    true -> dlss_copy_ram
  end.

init_props( Source )->
  All =
    maps:from_list(mnesia:table_info( Source, all )),
  maps:to_list( maps:with(?PROPS,All)).

get_read_node( Table )->
 Node = mnesia:table_info( Table, where_to_read ),
 if
   Node =:= nowhere ->throw({unavailable,Table});
   true-> Node
 end.

init_source(Source, Module, Options)->
  S = Module:init_source( Source, Options ),
  S#source{name = Source,module = Module}.

init_target(Target, Source, Module, Options)->
 case lists:member( Target, dlss:get_local_segments()) of
   true ->
     % It's not add_copy, simple copy
     T = Module:init_target(Target,Options),
     T#target{ name = Target, module = Module, trick = false };
   _->
     % TRICK mnesia, Add a copy to this node before ask mnesia to add it
     Props = init_props(Source),
     Module:init_copy( Target, Props ),

     T0 = Module:init_target(Target,Options),
     T1 = T0#target{ name = Target, module = Module, trick = true },

     % Set the guard to rollback target if I die
     Me = self(),
     Guard = spawn_link(fun()->
       process_flag(trap_exit,true),
       receive
         {commit,Me}->ok;
         {rollback,Me}->ok;
         {'EXIT',Me,Reason}->
           ?LOGERROR("~p local receiver died, reason ~p",[Target,Reason]),
           Module:drop_target( Target )
       end
     end),

     T1#target{guard = Guard}
 end.

commit_target(#target{module = Module, guard = Guard } = T)->
  Module:dump_target( T ),
  if
    is_pid( Guard ), Guard=/=self() -> Guard ! {commit,self()} ;
    true -> ok
  end.


rollback_target(#target{trick = true, module = Module,name = Target, guard = Guard} )->
  Module:drop_target( Target ),
  Guard ! {rollback,self()};
rollback_target(_T)->
  ok.

%%=================================================================
%%	API
%%=================================================================
copy( Source, Target )->
  copy( Source, Target, #{}).
copy( Source, Target, Options0 )->
  Options = ?OPTIONS(Options0),

  Module = get_module( Source ),
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

local_copy( Source, Target, Module, #{
  hash := InitHash0
} = Options)->

  TargetRef = init_target( Target, Source, Module, Options),

  OnBatch =
    fun(Batch, Size, Hash)->
      ?LOGINFO("DEBUG: ~p write batch, size ~s, length ~s",[
        Target,
        ?PRETTY_SIZE(Size),
        ?PRETTY_COUNT(length(Batch))
      ]),
      Module:write_batch(Batch, TargetRef),
      crypto:hash_update(Hash, term_to_binary( Batch ))
    end,

  SourceRef = init_source( Source, Module, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  FinalHash0 = do_copy(SourceRef, Module, OnBatch, InitHash ),
  FinalHash = crypto:hash_final( FinalHash0 ),

  commit_target( TargetRef ),

  ?LOGINFO("finish local copying: source ~p, target ~p, hash ~s",[Source, Target, ?PRETTY_HASH(FinalHash)]),

  FinalHash.

remote_copy( Source, Target, Module, Options)->

  Trap = process_flag(trap_exit,true),

  FinalHash =
    try  do_remote_copy( Source, Target, Module, Options )
    after
      process_flag(trap_exit,Trap)
    end,

  ?LOGINFO("finish remote copying: source ~p, target ~p, hash ~s", [Source, Target, ?PRETTY_HASH( FinalHash )] ),

  FinalHash.

do_remote_copy( Source, Target, Module, #{
  hash := InitHash0,
  attempts := Attempts
} = Options )->

  ReadNode = get_read_node( Source ),
  TargetRef = init_target( Target, Source, Module, Options ),

  Self = self(),
  InitState = #{
    source => Source,
    target => Target,
    hash => InitHash0,
    owner => Self,
    size => 0,
    batch => []
  },

  Worker = spawn_link(ReadNode, ?MODULE, remote_copy_request,[Self,Source,Module,Options,InitState]),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),

  % The remote worker needs a confirmation of the previous batch before it sends the next one
  Worker ! {confirmed, self()},

  %-------Enter the copy loop----------------------
  ?LOGINFO("copy ~p from ~p...",[Source,ReadNode]),
  FinalHash =
    try remote_copy_loop(Worker, Module, TargetRef, InitHash)
    catch
      _:Error->
        rollback_target( TargetRef ),
        case Error of
          invalid_hash->
            ?LOGERROR("~p invalid remote hash from ~p, left attempts ~p",[Source,ReadNode,Attempts-1]);
          {interrupted,Reason}->
            ?LOGERROR("~p copying from ~p interrupted, reason ~p, left attempts ~p",[Source,ReadNode,Reason,Attempts-1]);
          Other->
            ?LOGERROR("unexpected error on copying ~p from ~p, error ~p",[Source,ReadNode,Other]),
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

  commit_target( TargetRef ),

  FinalHash.


remote_copy_request(Owner, Source, Module, Options, #{
  hash := InitHash0
} = InitState0)->

  ?LOGINFO("remote copy request on ~p, options ~p", [Source, Options]),

  SourceRef = init_source( Source, Module, Options ),

  InitHash = crypto:hash_update(crypto:hash_init(sha256),InitHash0),
  InitState = InitState0#{
    hash => InitHash
  },

  TailState =
    #{batch := TailBatch, hash := TailHash} = do_copy( SourceRef, Module, fun remote_batch/3, InitState ),

  % Send the tail batch if exists
  case TailBatch of [] -> send_batch( TailState ); _->ok end,

  FinalHash = crypto:hash_final( TailHash ),

  ?LOGINFO("finish remote copy request on ~p, final hash ~s",[Source,?PRETTY_HASH(FinalHash)]),

  Owner ! {finish, self(), FinalHash}.

unzip_batch( [Zip|Rest], {Acc0,Hash0})->
  Batch = zlib:unzip( Zip ),
  Hash = crypto:hash_update(Hash0, Batch),
  unzip_batch(Rest ,{[Batch|Acc0], Hash});
unzip_batch([], Acc)->
  Acc.

remote_copy_loop(Worker, Module, #target{name = Target} =TargetRef, Hash0)->
  receive
     {write_batch, Worker, ZipBatch, ZipSize, WorkerHash }->

       ?LOGINFO("DEBUG: ~p batch received, size ~s, hash ~s",[Target,?PRETTY_SIZE(ZipSize),?PRETTY_HASH(WorkerHash)]),
       {Hash, BatchList} = unzip_batch( lists:reverse(ZipBatch), {[],Hash0}),

       % Check hash
       case crypto:hash_final(Hash) of
         WorkerHash -> Worker ! {confirmed, self()};
         LocalHash->
           ?LOGERROR("~p invalid remote hash ~s, local hash ~s",[Target,?PRETTY_HASH(WorkerHash), ?PRETTY_HASH(LocalHash)]),
           Worker!{not_confirmed,self()},
           throw(invalid_hash)
       end,

       % Dump batch
       [ begin
           Batch = binary_to_term( BatchBin ),

           ?LOGINFO("DEBUG: ~p write batch size ~s, length ~p",[
             Target,
             ?PRETTY_SIZE(size( BatchBin )),
             ?PRETTY_COUNT(length(Batch))
           ]),

           Module:write_batch(Batch, TargetRef)

         end || BatchBin <- BatchList ],

       remote_copy_loop(Worker, Module, TargetRef, Hash);
     {finish,Worker,WorkerFinalHash}->
       % Finish
       ?LOGINFO("DEBUG: ~p remote worker finished, final hash ~s",[Target, ?PRETTY_HASH(WorkerFinalHash)]),
       case crypto:hash_final(Hash0) of
         WorkerFinalHash -> WorkerFinalHash;
         LocalFinalHash->
           ?LOGERROR("~p invalid remote final hash ~s, local final hash ~s",[
             Target,
             ?PRETTY_HASH(WorkerFinalHash),
             ?PRETTY_HASH(LocalFinalHash)
           ]),
           throw(invalid_hash)
       end;
     {'EXIT',Worker,Reason}->
       throw({interrupted,Reason});
     {'EXIT',_Other,Reason}->
       throw({exit,Reason})
 end.

% Zip and stockpile local batches until they reach ?REMOTE_BATCH_SIZE
remote_batch(Batch0, Size, #{
  size := TotalZipSize0,
  batch := ZipBatch,
  hash := Hash0,
  source := Source,
  target := Target
}=State) when TotalZipSize0 < ?REMOTE_BATCH_SIZE->

  Batch = term_to_binary( Batch0 ),
  Hash = crypto:hash_update(Hash0, Batch),
  Zip = zlib:zip( Batch ),

  ZipSize = size(Zip),
  TotalZipSize = TotalZipSize0 + ZipSize,

  ?LOGINFO("DEBUG: add zip: source ~p, target ~p, size ~s, zip size ~p, total zip size ~p",[
   Source, Target, ?PRETTY_SIZE(Size), ?PRETTY_SIZE(ZipSize), ?PRETTY_SIZE(TotalZipSize)
  ]),

  State#{
   size => TotalZipSize,
   batch => [Zip|ZipBatch],
   hash => Hash
  };

% The batch is ready, send it
remote_batch(Batch0, Size, #{
  owner := Owner
}=State)->
   % First we have to receive a confirmation of the previous batch
   receive
     {confirmed, Owner}->
       send_batch( State ),
       remote_batch(Batch0, Size,State#{ batch=>[], size =>0 });
     {not_confirmed,Owner}->
       throw(invalid_hash)
   end.

send_batch(#{
  size := ZipSize,
  batch := ZipBatch,
  hash := Hash,
  source := Source,
  target := Target,
  owner := Owner
})->

 ?LOGINFO("send batch: source ~p, target ~p, zip size ~s, length ~p",[
   Source,
   Target,
   ?PRETTY_SIZE(ZipSize),
   length(ZipBatch)
 ]),

 Owner ! {write_batch, self(), ZipBatch, ZipSize, crypto:hash_final(Hash) }.


active_copy(Source, Target, Module, Options )->
  % TODO
  remote_copy(Source, Target, Module, Options).

do_copy(SourceRef, Module, OnBatch, InAcc)->

  Acc0 = #acc{
    module = Module,
    batch = [],
    acc = InAcc,
    size = 0,
    on_batch = OnBatch,
    stop = SourceRef#source.stop
  },

  case try Module:fold(SourceRef, fun iterator/2, Acc0)
  catch
    _:{stop,Stop}-> Stop;
    _:{final,Final}->{final,Final}
  end of
    #acc{batch = [], acc = FinalAcc}-> FinalAcc;
    #acc{batch = Tail, size = Size, acc = TailAcc, on_batch = OnBatch}->
      OnBatch(Tail, Size, TailAcc);
    {final,FinalAcc}-> FinalAcc
  end.
%----------------------WITH STOP KEY-----------------------------
iterator({K,V},#acc{module = Module, batch = Batch, size = Size0, stop = Stop} = Acc)
 when Size0 < ?BATCH_SIZE, Stop =/= undefined, K < Stop->

 {Action,Size} = Module:action({K,V}),
 Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{module = Module, batch = Batch, on_batch = OnBatch, acc = InAcc0, size = Size0, stop = Stop} = Acc)
  when Stop =/= undefined, K < Stop->

  {Action,Size} = Module:action({K,V}),

  Acc#acc{batch = [Action], acc = OnBatch(Batch, Size0, InAcc0), size = Size};

% stop key reached
iterator(_Rec,#acc{stop = Stop} = Acc)
 when Stop =/= undefined->
 throw({stop, Acc});

%----------------------NO STOP KEY-----------------------------
iterator({K,V},#acc{module = Module, batch = Batch, size = Size0} = Acc)
 when Size0 < ?BATCH_SIZE->

 {Action,Size} = Module:action({K,V}),
 Acc#acc{batch = [Action|Batch], size = Size0 + Size};

% Batch is ready
iterator({K,V},#acc{module = Module, batch = Batch, on_batch = OnBatch, size = Size0, acc = InAcc0} = Acc) ->

  {Action,Size} = Module:action({K,V}),
  Acc#acc{batch = [Action], acc =OnBatch(Batch, Size0, InAcc0), size = Size}.

%------------------SPLIT---------------------------------------
% Splits are always local
%--------------------------------------------------------------
split( Source, Target )->
  split( Source, Target, #{}).
split( Source, Target, Options0 )->

  Options = ?OPTIONS( Options0 ),
  Module = get_module( Source ),

  SourceRef = init_source( Source, Module, Options ),
  TargetRef = init_target(Target, Source, Module, Options),

  % Target considered to be empty
  InitHash = crypto:hash_update(crypto:hash_init(sha256),<<>>),
  InitAcc = #r_acc{
    module = Module,
    head = 0,
    hash = InitHash
  },

  #r_acc{ hash = FinalHash0, t_key = SplitKey0 } =
    Module:init_reverse(Source,
      fun
        ('$end_of_table')->
          InitAcc#r_acc{ hash = <<>>, t_key = '$end_of_table' };
        ({I,TSize,TKey})->
          Acc0 = InitAcc#r_acc{i=I, t_key = TKey ,tail = TSize},
          do_split(Module, Source, Target, SourceRef, TargetRef, Acc0 )
      end),

  FinalHash = crypto:hash_final( FinalHash0 ),
  SplitKey = Module:decode_key(SplitKey0),

  Module:dump_source( SourceRef ),
  commit_target( TargetRef ),

  ?LOGINFO("split finish: source ~p, target ~p, split key ~p, hash ~s",[
    Source,
    Target,
    SplitKey,
    ?PRETTY_HASH( FinalHash )
  ]),

  {SplitKey,FinalHash}.

do_split(Module, Source, Target, SourceRef, TargetRef, Acc0)->

  OnBatch =
    fun(Batch, Size, #r_acc{hash = Hash,head = H} = Acc)->

      % Enter the reverse loop
      HKey = Module:get_key(hd(Batch)),
      NextAcc =
        try reverse_loop(Acc#r_acc{head = H + Size, h_key = HKey })
        catch
          _:stop-> throw({final,Acc#r_acc{ h_key = Module:get_key( lists:last(Batch) )}})
        end,

      ?LOGINFO("DEBUG: split ~p, target ~p, write batch size ~s, length ~s, head ~s, tail ~s, hkey ~p, tkey ~p",[
        Source,
        Target,
        ?PRETTY_SIZE(Size),
        ?PRETTY_COUNT(length(Batch)),
        ?PRETTY_SIZE(H+Size),
        ?PRETTY_SIZE(NextAcc#r_acc.tail),
        Module:decode_key(HKey),
        Module:decode_key(NextAcc#r_acc.t_key)
      ]),

      Module:write_batch(Batch, TargetRef),
      Module:drop_batch(Batch, SourceRef),

      NextAcc#r_acc{ hash = crypto:hash_update(Hash, term_to_binary( Batch ))}
    end,

  % Run split
  do_copy(SourceRef, Module, OnBatch, Acc0).

%% THIS IS THE MEDIAN!
reverse_loop(#r_acc{h_key = HKey, t_key = TKey}) when HKey >= TKey->
  throw(stop);

reverse_loop(#r_acc{i=I, module = Module, head = H,tail = T,t_key = TKey} = Acc) when T < H->
 case Module:prev(I,TKey) of
   {K,Size} ->
     reverse_loop(Acc#r_acc{tail = T + Size, t_key = K});
   '$end_of_table' ->
     % Can we reach here?
     throw(stop)
 end;
% We reached the head size
reverse_loop(Acc)->
  Acc.

get_size( Table )->

 Module = get_module( Table ),
 ReadNode = get_read_node( Table ),
 if
   ReadNode =:= node()->
     Module:get_size( Table );
   true->
     case rpc:call(ReadNode, Module, get_size, [ Table ]) of
       {badrpc, _Error} -> -1;
       Result -> Result
     end
 end.

debug(Storage, Count)->
  spawn(fun()->fill(Storage, Count) end).

fill(S,C) when C>0 ->
  if C rem 100000 =:= 0-> ?LOGINFO("DEBUG: write ~p",[C]); true->ignore end,
  dlss:dirty_write(S, {x, erlang:phash2({C}, 200000000), erlang:phash2({C}, 200000000)}, {y, binary:copy(integer_to_binary(C), 100)}),
  fill(S,C-1);
fill(_S,_C)->
  ok.


