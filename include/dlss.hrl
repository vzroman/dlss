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

-ifndef(DLSS_STRUCT).
-define(DLSS_STRUCT,1).

-define(MB,1048576).

-define(DEFAULT_MAX_RESTARTS,10).
-define(DEFAULT_MAX_PERIOD,1000).
-define(DEFAULT_STOP_TIMEOUT,600000). % 10 min.

-define(DEFAULT_SEGMENT_LIMIT,#{
  0 => 512,
  1 => 4096
}). % MB


-record(kv,{key,value}).

-define(ERROR(Error),erlang:error(Error)).

-define(ENV(Key,Default),application:get_env(dlss,Key,Default)).
-define(ENV(OS,Config,Default),
  (fun()->
    case os:getenv(OS) of
      false->?ENV(Config,Default);
      Value->Value
    end
  end)()
).

-define(UNDEFINED,undefined).
-define(RAND(List),
  begin
    _@I = erlang:phash2(make_ref(),length(List)),
    lists:nth(_@I+1, List)
  end).

-define(A2B(Atom),unicode:characters_to_binary(atom_to_list(Atom))).

-define(PRETTY_SIZE(S),dlss_util:pretty_size(S)).
-define(PRETTY_COUNT(C),dlss_util:pretty_count(C)).
-define(PRETTY_HASH(H),binary_to_list(base64:encode(H))).

-ifndef(TEST).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-else.

-define(LOGERROR(Text),ct:pal("error: "++Text)).
-define(LOGERROR(Text,Params),ct:pal("error: "++Text,Params)).
-define(LOGWARNING(Text),ct:pal("warning: "++Text)).
-define(LOGWARNING(Text,Params),ct:pal("warning: "++Text,Params)).
-define(LOGINFO(Text),ct:pal("info: "++Text)).
-define(LOGINFO(Text,Params),ct:pal("info: "++Text,Params)).
-define(LOGDEBUG(Text),ct:pal("debug: "++Text)).
-define(LOGDEBUG(Text,Params),ct:pal("debug: "++Text,Params)).

-endif.


-endif.
