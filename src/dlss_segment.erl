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

-module(dlss_segment).

-include("dlss.hrl").

-behaviour(gen_server).

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
  %----OPTIMIZED SCANNING------------------
  select/2,dirty_select/2,
  dirty_scan/3, dirty_scan/4
]).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  get_info/1,
  is_empty/1,
  add_node/2,
  remove_node/2,
  get_size/1
]).

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

%%====================================================================
%%		Test API
%%====================================================================
-ifdef(TEST).

-export([
  split/2,
  split/5
]).

-endif.

-record(state,{segment,cycle}).

-define(DEFAULT_SCAN_CYCLE,5000).

-define(MAX_SCAN_INTERVAL_BATCH,1000).

-define(MB,1048576).

-define(PROCESS(Segment), list_to_atom( atom_to_list(Segment) ++ "_sup" ) ).

%%=================================================================
%%	STORAGE SEGMENT API
%%=================================================================
%-------------ITERATOR----------------------------------------------
first(Segment)->
  mnesia:first(Segment).
dirty_first(Segment)->
  mnesia:dirty_first(Segment).

last(Segment)->
  mnesia:last(Segment).
dirty_last(Segment)->
  mnesia:dirty_last(Segment).

next(Segment,Key)->
  mnesia:next(Segment,Key).
dirty_next(Segment,Key)->
  mnesia:dirty_next(Segment,Key).

prev(Segment,Key)->
  mnesia:prev(Segment,Key).
dirty_prev(Segment,Key)->
  mnesia:dirty_prev(Segment,Key).

%-------------INTERVAL SCAN----------------------------------------------
dirty_scan(Segment,From,To)->
  dirty_scan(Segment,From,To,infinity).
dirty_scan(Segment,From,To,Limit)->

  % Find out the type of the storage
  StorageType=mnesia_lib:storage_type_at_node(node(),Segment),

  % Define where to stop
  StopGuard=
    if
      To=:='$end_of_table' ->[];  % Nowhere
      true -> [{'=<','$1',{const,To}}]    % Stop at the To key
    end,

  MS=[{#kv{key='$1',value='$2'},StopGuard,[{{'$1','$2'}}]}],

  % TODO. There is a significant issue with ets based storage type
  % As the ets:select doesn't stop when the Key is
  % bigger than the To it leads to scanning the whole table until it meets the
  % Limit demand. It might lead ta scanning the table to its end
  % even if there are no more keys within the range. It
  % can dramatically decrease the performance. But the 'select' in a major
  % set of cases is still more efficient than the 'dirty_next' iterator
  % especially if the table is located on the other node.

  % Initialize the continuation
  case mnesia_lib:db_select_init(StorageType,Segment,MS,1) of
    {[],'$end_of_table'}->[]; % The segment is empty
    {[],_Cont}->[];           % there are no keys less than To
    '$end_of_table'->[]; % this format is returned by the ets backend

    {[{FirstKey,FirstValue}],Cont}->

      % Define the from which to start
      {StartKey,Head}=
        if
          From=:='$start_of_table';From=:=FirstKey ->{FirstKey,[{FirstKey,FirstValue}]} ;
          true ->
            case mnesia:dirty_read(Segment,From) of
              [#kv{value = FromValue}]->
                { From, [{From,FromValue}] };
              _->
                {From,[]}
            end
        end,
      if
        Limit =:=1,length(Head)=:=1 -> Head ;
        true ->
          Limit1=
            if
              is_integer(Limit) -> Limit - length(Head) ;
              true -> Limit
            end,

          % Initialize the continuation with the key to start from
          Cont1=init_continuation(Cont,StartKey,Limit1),

          BatchSize =
            if
              is_integer(Limit1) -> Limit1 ;
              true -> ?MAX_SCAN_INTERVAL_BATCH
            end,
          % Run the search
          Head++run_continuation(Cont1,StorageType,MS,Limit1,BatchSize,[])
      end
  end.

init_continuation('$end_of_table',_StartKey,_Limit)->
  '$end_of_table';
init_continuation({Segment,_LastKey,Par3,_Limit,Ref,Par6,Par7,Par8},StartKey,Limit)->
  % This is the form of ets ordered_set continuation
  Limit1 =
    if
      Limit=:=infinity -> ?MAX_SCAN_INTERVAL_BATCH;
      true -> Limit
    end,
  {Segment,StartKey,Par3,Limit1,Ref,Par6,Par7,Par8};
init_continuation({_LastKey,_Limit,Fun},StartKey,Limit)->
  Limit1 =
    if
      Limit=:=infinity -> ?MAX_SCAN_INTERVAL_BATCH;
      true -> Limit
    end,
  % This is the form of mnesia_leveldb continuation
  {StartKey,Limit1,Fun}.

run_continuation('$end_of_table',_StorageType,_MS,_Limit,_BatchSize,Acc)->
  % The end of table is reached
  lists:append(lists:reverse(Acc));
run_continuation(_Cont,_StorageType,_MS,Limit,_BatchSize,Acc) when Limit=:=0->
  % The limit is reached
  lists:append(lists:reverse(Acc));
run_continuation(_Cont,_StorageType,_MS,Limit,_BatchSize,Acc) when Limit<0->
  % The limit is passed over, we need to cut off the excessive records
  Result = lists:append(lists:reverse(Acc)),
  { Head, _} = lists:split( length(Result) + Limit , Result ),
  Head;
run_continuation(Cont,StorageType,MS,Limit,BatchSize,Acc)->
  % Run the search
  {Result,Cont1}=
    case mnesia_lib:db_select_cont(StorageType,Cont,MS) of
      {R,C} -> {R,C};
      '$end_of_table'->{[],'$end_of_table'}
    end,
  % Update the acc
  Acc1=
    case Result of
      []->Acc;
      _->[Result|Acc]
    end,
  % If the result is less than the limit the its the last batch
  if
    length(Result)<BatchSize->
      % The last scanned records are out of range already. There is no sense
      % to keep scanning
      lists:append(lists:reverse(Acc1));
    true->
      % The result is full, there might be more records
      % Update the limit
      Limit1=
        if
          is_integer(Limit)-> Limit-length(Result);
          true -> Limit
        end,
      run_continuation(Cont1,StorageType,MS,Limit1,BatchSize,Acc1)
  end.

%-------------SELECT----------------------------------------------
select(Segment,MS)->
  mnesia:select(Segment,MS).
dirty_select(Segment,MS)->
  mnesia:dirty_select(Segment,MS).

%-------------READ----------------------------------------------
read( Segment, Key )->
  read( Segment, Key, _Lock = none).
read( Segment, Key, Lock)->
  case mnesia:read(Segment,Key,Lock) of
    [#kv{value = Value}]->Value;
    _->not_found
  end.
dirty_read(Segment,Key)->
  case mnesia:dirty_read(Segment,Key) of
    [#kv{value = Value}]->Value;
    _->not_found
  end.

%-------------WRITE----------------------------------------------
write(Segment,Key,Value)->
  write(Segment,Key,Value, _Lock = none).
write(Segment,Key,Value,Lock)->
  mnesia:write(Segment,#kv{key = Key,value = Value}, Lock).

dirty_write(Segment,Key,Value)->
  mnesia:dirty_write(Segment,#kv{key = Key,value = Value}).

%-------------DELETE----------------------------------------------
delete(Segment,Key)->
  delete(Segment,Key,_Lock=none).
delete(Segment,Key,Lock)->
  mnesia:delete(Segment,Key,Lock).
dirty_delete(Segment,Key)->
  mnesia:dirty_delete(Segment,Key).

%%=================================================================
%%	Service API
%%=================================================================
get_info(Segment)->
  Local = mnesia:table_info(Segment,local_content),
  { Type, Nodes }=get_nodes(Segment),
  #{
    type => Type,
    local => Local,
    nodes => Nodes
  }.

is_empty(Segment)->
  case dirty_first(Segment) of
    '$end_of_table'->true;
    _->false
  end.

add_node(Segment,Node)->
  #{ type:=Type } = get_info(Segment),
  MnesiaType =
    if
      Type =:= ram      -> ram_copies;
      Type =:= ramdisc  -> disc_copies;
      Type =:= disc     -> leveldb_copies
    end,
  case mnesia:add_table_copy(Segment,Node,MnesiaType) of
    {atomic,ok} -> ok;
    {aborted,Reason}->{error,Reason}
  end.

remove_node(Segment,Node)->
  case mnesia:del_table_copy(Segment,Node) of
    {atomic,ok}->ok;
    {aborted,Reason}->{error,Reason}
  end.

%----------Calculate the size (bytes) occupied by the segment-------
get_size(Segment)->
  Memory = mnesia:table_info(Segment,memory),
  case get_info(Segment) of
    #{type := disc} -> Memory;
    _ ->
      % for ram and ramdisc tables the mnesia returns a number of allocated words
      erlang:system_info(wordsize) * Memory
  end.

%%=================================================================
%%	API
%%=================================================================
start(Segment)->
  dlss_schema_sup:start_segment(Segment).

start_link(Segment)->
  gen_server:start_link({local, ?PROCESS(Segment) }, ?MODULE, [Segment], []).

stop(Segment)->
  case whereis(?PROCESS(Segment)) of
    PID when is_pid(PID)->
      dlss_schema_sup:stop_segment(PID);
    _ ->
      { error, not_started }
  end.

%%=================================================================
%%	OTP
%%=================================================================
init([Segment])->

  ?LOGINFO("starting segment server for ~p pid ~p",[Segment,self()]),

  Cycle=?ENV(segment_scan_cycle, ?DEFAULT_SCAN_CYCLE),

  % Enter the loop
  self()!loop,

  {ok,#state{
    segment = Segment,
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
  segment = Segment,
  cycle = Cycle
}=State)->

  case dlss_storage:segment_params(Segment) of
    {ok, Params} ->
      {ok,_}=timer:send_after(Cycle,loop),
      case lists:member(Segment,dlss_storage:get_segments(maps:get(storage,Params))) of
        true->
          loop( Segment, Params );
        _->
          ok
      end,
      {noreply,State};
    { error, Error} ->
      { stop, Error, State }
  end.


terminate(Reason,#state{segment = Segment})->
  ?LOGINFO("terminating segment server ~p, reason ~p",[Segment,Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%============================================================================
%%	The Loop
%%============================================================================
loop( Segment, #{ level := 0, storage := Storage } )->
  % The segment is the root segment in its storage.
  % We watch its size and when it reaches the limit we
  % create a new root segment for the storage
  Size = get_size( Segment ) / ?MB, % MB
  Limit = ?ENV( segment_limit, ?DEFAULT_SEGMENT_LIMIT),
  if
    Size >= Limit ->
      ?LOGINFO("the root segment ~p in storage ~p reached the limit ~p, size ~p",[ Segment, Storage, Limit, Size ]),
      dlss_storage:new_root_segment(Storage);
    true -> ok
  end;

loop( Segment, #{ storage := Storage, level := Level } )->
  case dlss_storage:get_children( Segment ) of
    [] ->
      % The segment is at the lowest level.
      % Check its size and if it has reached the limit split the segment
      Size = round (get_size( Segment ) / ?MB), % MB
      Limit = ?ENV( segment_limit, ?DEFAULT_SEGMENT_LIMIT),
      if
        Size >= Limit ->
          ?LOGINFO("the segment ~p in storage ~p reached the limit ~p, size ~p, split...",[ Segment, Storage, Limit, Size ]),

          % Create a child segment
          dlss_storage:spawn_segment(Segment),
          ok;
        true ->
          if
            Level > 1->
              % The segment is below the desired level. To take place in upper level it
              % must absorb its parent
              case dlss_storage:has_siblings(Segment) of
                true->
                  % The parent is to absorbed by its children
                  dlss_storage:absorb_parent(Segment);
                _->
                  % If there are no other children it is the splitting
                  Parent = dlss_storage:parent_segment(Segment),

                  ?LOGINFO("start splitting ~p",[Parent]),
                  split( Parent, Segment ),

                  case is_empty(Segment) of
                    false->
                      ?LOGINFO("finish splitting ~p",[Parent]),
                      dlss_storage:level_up( Segment );
                    _->
                      ?LOGERROR("empty head segment ~p after splitting ~p",[Segment,Parent])
                  end,
                  ok
              end;
            true ->
              % the segment is at the respectable level and is not overwhelmed.
              % This is a stable state
              ok
          end
      end;
    _->
      % The segment has children, that are at the level lower than 1.
      % They are to absorb their parent to take a place at its level.
      case is_empty( Segment ) of
        true ->
          % The segment has been absorbed by the children
          ?LOGINFO("the segment ~p in storage ~p is to be absorbed",[Segment,Storage]),
          dlss_storage:absorb_segment( Segment );
        _->
          ok
      end
  end.


%%============================================================================
%%	Internal helpers
%%============================================================================
get_nodes(Segment)->
  Nodes=[{T,mnesia:table_info(Segment,CT)}||{CT,T}<-[
    {disc_copies,ramdisc},
    {ram_copies,ram},
    {leveldb_copies,disc}
  ]],
  case [{T,N}||{T,N}<-Nodes,N=/=[]] of
    [Result]->Result;
    _->throw(invalid_storage_type)
  end.

split( From, To )->
  #{ type:=Type } = get_info( From ),
  Initial = get_size( From ),
  Half = Initial / 2,
  ?LOGINFO("start moving records ~p MB records from ~p to ~p, type ~p",[
    From,
    To,
    Half/ ?MB,
    Type
  ]),

  BatchSize = ?MAX_SCAN_INTERVAL_BATCH*100,
  OnBatch=
    fun({Key,_V})->
      ToSize = get_size(To),
      if
        ToSize>=Half -> stop ;
        true ->
          ?LOGINFO("move batch from ~p to ~p, last key ~p, parent size ~p, child size ~p, left size ~p MB",[
            From,
            To,
            Key,
            get_size(From) / ?MB,
            ToSize / ?MB,
            round((Half - ToSize)/?MB)
          ]),
          next
      end
    end,

  if
    Type =:= disc->
      ?LOGINFO("DISC based storage split algorithm"),
      split_disc( From, To, BatchSize, OnBatch, [] );
    true ->
      ?LOGINFO("RAM based storage split algorithm"),
      split_ram( From, To, '$start_of_table', BatchSize, OnBatch, [] )
  end.

split_disc( From, To, OnBatch, BatchSize, Batch ) when length(Batch)>=BatchSize->
  % Prepare bulk insert, remove deleted records
  Deleted = mnesia_eleveldb:encode_val('@deleted@'),
  Batch1 = [ {K,V} || {K,V} <-lists:reverse(Batch), V=/=Deleted ],

  % The bulk insert
  mnesia_eleveldb:bulk_insert( To, Batch1 ),
  case OnBatch( lists:last(BatchSize) ) of
    next->
      split_disc( From, To, OnBatch, BatchSize, [] );
    _->
      ok
  end;
split_disc( From, To, OnBatch, BatchSize, Batch )->



split_ram( From, To, Key, Size, I ) when (I rem 100) =:=0->
  ToSize = get_size(To),
  if
    ToSize>=Size -> ok ;
    true ->
      ?LOGINFO("move key from ~p to ~p, parent size ~p, child size ~p, left size ~p MB",[
        From,
        To,
        get_size(From) / ?MB,
        ToSize / ?MB,
        round((Size - ToSize)/?MB)
      ]),
      split_ram( From, To, Key, Size, I+1 )
  end;
split_ram( From, To, Key, Size, I )->
  Rows = dlss_segment:dirty_scan(From,Key,'$end_of_table',?MAX_SCAN_INTERVAL_BATCH),
  if
    length(Rows)>=?MAX_SCAN_INTERVAL_BATCH ->
      move_rows(Rows, From, To),
      {LastKey,_}=lists:last(Rows),
      split_ram( From, To, LastKey, Size, I+1 );
    true ->
      % It is the end of the From segment take only half of the keys
      { Head ,_ } = lists:split( length(Rows) div 2, Rows ),
      move_rows(Head, From, To)
  end.

move_rows( Rows, From, To )->
  [if
     V=:='@deleted@' ->
       ok = dlss_segment:dirty_delete( From, K );
     true ->
       ok = dlss_segment:dirty_write( To, K, V ),
       ok = dlss_segment:dirty_delete( From, K )
   end || {K,V} <-Rows],
  ok.

