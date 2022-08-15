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

-ifndef(DLSS_ELEVELDB).
-define(DLSS_ELEVELDB,1).

-define(REF(T),mnesia_eleveldb:get_ref(T)).
-define(DECODE_KEY(K),mnesia_eleveldb:decode_key(K)).
-define(ENCODE_KEY(K),mnesia_eleveldb:encode_key(K)).
-define(DECODE_VALUE(V),element(3,mnesia_eleveldb:decode_val(V))).
-define(ENCODE_VALUE(V),{[],[],mnesia_eleveldb:decode_val(V)}).

-define(DATA_START, <<2>>).

-define(MOVE(I,K),eleveldb:iterator_move(I,K)).
-define(NEXT(I),eleveldb:iterator_move(I,next)).


-endif.
