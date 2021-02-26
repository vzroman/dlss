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
  dump_segment/1,
  on_init/0
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

