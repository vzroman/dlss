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

-module(dlss_rebalance).

-include("dlss.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  on_init/0,
  dump_segment/1,
  %-----------------Copy-------------------------------------------
  copy/6,
  delete_until/2,

  disc_bulk_read/2,
  disc_bulk_write/2,

  ets_bulk_read/2,
  ets_bulk_write/2
]).

%% API
-export([
  dump_test/2
]).

-define(TEMPEXT,".dlss_temp").
-define(BACKUPEXT,".dlss_backup").

-define(TEMP(N), mnesia_lib:dir(lists:concat([N, ?TEMPEXT]))).
-define(BACKUP(N), mnesia_lib:dir(lists:concat([N, ?BACKUPEXT]))).
-define(DATA(N),mnesia_lib:tab2dcd(N)).

-define(BATCH_SIZE, 1024 ).

%%=================================================================
%%	COPY
%%=================================================================
copy( Source, Target, Copy, FromKey0, OnBatch, Hash )->

  #{ type:= Type }=dlss_segment:get_info( Source ),
  { Read, Write, FromKey } =
    if
      Type =:= disc ->
        _From =
          if
            FromKey0 =:= '$start_of_table' -> FromKey0 ;
            true -> mnesia_eleveldb:encode_key(FromKey0)
          end,
        { disc_bulk_read, disc_bulk_write, _From };
      true ->
        { ets_bulk_read, ets_bulk_write, FromKey0 }
    end,

  ReadNode = mnesia:table_info( Source, where_to_read ),
  if
    ReadNode =:= nowhere ->?ERROR({unavailable,Source});
    true -> ok
  end,

  ReadBatch=
    fun( From )-> rpc:call(ReadNode, ?MODULE, Read, [ Source, From ])  end,

  WriteBatch=
    fun( Batch )->?MODULE:Write( Target, Batch )  end,

  HashRef0 = crypto:hash_update( crypto:hash_init(sha256), Hash ),

  HashRef = copy_loop( ReadBatch ,WriteBatch, FromKey, Copy, OnBatch, HashRef0, _Count = 0 ),

  if
    Type =/=disc -> dump_segment( Target );
    true -> ok
  end,

  { ok, crypto:hash_final( HashRef ) }.


copy_loop( Read, Write, FromKey, OnItem, OnBatch, Hash0, Count0 )->
  case Read( FromKey ) of
    Batch when is_list(Batch), length(Batch)>0 ->
      ToWrite = copy_items( Batch, OnItem ),
      Hash1 = update_hash( ToWrite, Hash0 ),
      Write( ToWrite ),
      if
        length(ToWrite) < length(Batch) ->
          % Stop is requested by the clients callback
          Hash1;
        length(Batch) < ?BATCH_SIZE->
          % There are no more records to keep seeking through if the batch is not full
          Hash1;
        true ->
          { LastKey, _} = lists:last( Batch ),
          Count1 = Count0 + length(ToWrite),
          case OnBatch( LastKey, Count1 ) of
            stop->
              % Stop is requested by the client's OnBatch callback
              Hash1;
            _->
              copy_loop( Read, Write, LastKey, OnItem, OnBatch, Hash1, Count1 )
          end
      end;
      ReadError -> ?ERROR( ReadError )
  end.

copy_items( ['$end_of_table'|_], _OnItem )->
  [];
copy_items( [Rec|Tail], OnItem )->
  case OnItem( Rec ) of
    stop-> [];
    Rec1 ->
      [ Rec1 | copy_items( Tail, OnItem ) ]
  end;
copy_items( [], _OnItem )->
  [].


update_hash([ Rec | Tail ], HashRef )->
  update_hash( Tail, rec_hash( Rec, HashRef ) );
update_hash( [], HashRef )->
  HashRef.
rec_hash( {put,K,V}, HashRef )->
  crypto:hash_update(HashRef,<<"@put@",K/binary,"@",V/binary>>);
rec_hash( {delete,K}, HashRef )->
  crypto:hash_update(HashRef,<<"@delete@",K/binary>>).

delete_until( Segment, ToKey )->
  #{ type:= Type }=dlss_segment:get_info( Segment ),
  delete_until( Type, Segment, ToKey ).
delete_until( disc, Segment, ToKey0 )->
  ToKey = mnesia_eleveldb:encode_key( ToKey0 ),
  delete_disc_until( disc_bulk_read(Segment,'$start_of_table'), Segment, ToKey );
delete_until( _EtsBased, Segment, ToKey )->

  mnesia:ets(fun()->
    delete_ets_until( dlss_segment:dirty_scan( Segment, '$start_of_table', ToKey, ?BATCH_SIZE ), ToKey, Segment )
  end),

  % Dump the segment after cleaning
  dump_segment( Segment ),

  ok.

delete_disc_until( Records, Segment, ToKey )->
  ToDelete = [ K || {K,_V} <- Records, K=<ToKey ],
  mnesia_eleveldb:bulk_delete( Segment, ToDelete ),
  if
    length(ToDelete) >= ?BATCH_SIZE ->
      delete_disc_until( disc_bulk_read(Segment,lists:last(ToDelete)), Segment, ToKey );
    true ->
      % There are no more records to seek through
      ok
  end.

delete_ets_until( Records, ToKey, Segment ) when length(Records) > 0 ->
  [ dlss_segment:delete( Segment, K ) || {K,_V} <- Records ],
  if
    length(Records)>=?BATCH_SIZE ->
      {LastKey, _} = lists:last( Records ),
      delete_ets_until( dlss_segment:dirty_scan( Segment, LastKey, ToKey, ?BATCH_SIZE ), ToKey, Segment );
    true ->
      ok
  end;
delete_ets_until( [], _ToKey, _Segment )->
  ok.


%%=================================================================
%%	Bulk read/write callbacks
%%=================================================================
disc_bulk_read( Segment, FromKey )->
  lists:reverse(mnesia_eleveldb:dirty_iterator( Segment,fun( Rec, Acc )->
    if
      length(Acc) >= ?BATCH_SIZE -> stop ;
      true -> [ Rec|Acc ]
    end
  end, [], FromKey )).

disc_bulk_write( Segment, Records )->

  ok = mnesia_eleveldb:bulk_insert( Segment, [ {K,V} || {put, K, V} <- Records ] ),
  ok = mnesia_eleveldb:bulk_delete( Segment, [K || {delete, K} <- Records ] ),

  ok.

ets_bulk_read( Segment, FromKey )->
  dlss_segment:dirty_scan( Segment, FromKey, '$end_of_table', ?BATCH_SIZE ).

ets_bulk_write( Segment, Records )->

  mnesia:ets(fun()->
    [ case R of
        {put, K, V}->
          ok = dlss_segment:write(Segment, K, V);
        {delete, K}->
          ok = dlss_segment:delete( Segment, K )
      end|| R <- Records ]
  end),
  ok.


%%=================================================================
%%	Helpers
%%=================================================================
dump_segment( Segment )->

  Temp = ?TEMP( Segment ),
  Backup = ?BACKUP( Segment ),
  Data = ?DATA( Segment ),

  case filelib:is_regular( Data ) of
    true->
      try
         ok = file:rename( Data, Backup ),
         dump_ets( Segment, Temp, Data ),
         file:delete( Backup )
      catch
          _:Error->
            file:delete( Temp ),
            file:delete( Data ),
            ok = file:rename( Backup, Data ),
            ?ERROR( { dump_segment_error, Error } )
      end;
    _->
      ?ERROR({ no_data_file, Segment })
  end,
  ok.

on_init()->

  Dir = mnesia_lib:dir(),
  BackupFiles = filelib:wildcard( "*" ++ ?BACKUPEXT, Dir ),
  TempFiles = filelib:wildcard( "*" ++ ?TEMPEXT, Dir ),

  if
    length(BackupFiles) + length(TempFiles)=:=0 ->
      ok;
    true ->
      ?LOGINFO("starting rebalance recovery dir ~p, temp ~p, backup ~p",[
        Dir,
        filelib:wildcard( "*" ++ ?TEMPEXT, Dir ) ,
        filelib:wildcard( "*" ++ ?BACKUPEXT, Dir )
      ]),

      % Restore backups
      [ begin
          Name = lists:sublist( F, length(F) - length(?BACKUPEXT) ),
          Data = ?DATA( Name ),
          case filelib:is_regular( Data ) of
            true->
              ?LOGWARNING("skipping to recover segment ~p from backup because datafile exits ~p",[ Name, Data ]);
            _->
              Backup = ?BACKUP( Name ),
              ok = file:rename( Backup, Data ),
              ?LOGWARNING("recover segment ~p from backup ~p",[ Name, Backup ])
          end
        end || F <- filelib:wildcard( "*" ++ ?BACKUPEXT, Dir ) ],

      % Remove temp files
      [ begin
          Name = lists:sublist( F, length(F) - length(?TEMPEXT) ),
          Temp = ?TEMP( Name ),
          ?LOGWARNING("segment ~p remove temp file ~p",[ Name, Temp ]),
          file:delete( Temp )
        end|| F <- filelib:wildcard( "*" ++ ?TEMPEXT, Dir ) ]
  end,

  ok.

dump_ets( Segment, Temp, Data )->

  file:delete( Temp ),
  Log  = mnesia_log:open_log({ Segment, ets2dcd}, mnesia_log:dcd_log_header(), Temp, false ),
  mnesia_lib:db_fixtable(ram_copies, Segment, true),
  ok = ets2dcd( mnesia_lib:db_init_chunk(ram_copies, Segment, 1000), Segment, Log),
  mnesia_lib:db_fixtable(ram_copies, Segment, false),
  mnesia_log:close_log(Log),

  ok = file:rename(Temp, Data).


ets2dcd('$end_of_table', _Tab, _Log) ->
  ok;
ets2dcd({Recs, Cont}, Tab, Log) ->
  ok = disk_log:log_terms(Log, Recs),
  ets2dcd(mnesia_lib:db_chunk(ram_copies, Cont), Tab, Log).


%=============================================================================
% Test helpers
%=============================================================================
% dlss_rebalance:dump_test(test1,ramdisc)
dump_test(Table,Type)->

  dlss_backend:create_segment(Table,#{
    type => Type,
    nodes => [node()],
    local => false
  }),

  mnesia:ets(fun()->
    [ dlss_segment:write(Table,{x, V}, {y, V}) || V <- lists:seq(1, 20000000) ]
             end),

  %dump_segment(Table).
  ok.