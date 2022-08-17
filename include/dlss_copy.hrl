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

%-define(BATCH_SIZE, 10485760). % 10 MB
-define(BATCH_SIZE, 52428800). % 50 MB

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

-record(source,{name,ref,start,stop}).
-record(target,{name,ref,sync,trick}).


-endif.
