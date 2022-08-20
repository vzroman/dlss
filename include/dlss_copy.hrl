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

-ifndef(DLSS_COPY).
-define(DLSS_COPY,1).

-define(BATCH_SIZE, 31457280). % 30 MB Default eleveldb buffer size
-define(REMOTE_BATCH_SIZE, 4194304). % 4 MB
-define(FLUSH_TAIL_TIMEOUT,1000).

-define(DEFAULT_OPTIONS,#{
  start_key =>undefined,
  end_key => undefined,
  hash => <<>>,
  sync => false,
  attempts => 3
}).
-define(OPTIONS(O),maps:merge(?DEFAULT_OPTIONS,O)).

% Log formats
-define(LOG_LOCAL(Source,Target),io_lib:format("local copy: ~p:~p",[Source,Target])).
-define(LOG_SEND(Source,Node,Target),io_lib:format("remote copy: source ~p target ~p:~p",[Source,Node,Target])).
-define(LOG_RECEIVE(Node,Source,Target),io_lib:format("remote copy: source ~p:~p target ~p",[Node,Source,Target])).

% Copy properties
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

-record(source,{name,module,ref,start,stop}).
-record(target,{name,module,ref,sync,trick,guard}).


-endif.
