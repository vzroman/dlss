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

-ifndef(SCHEMA).
-define(SCHEMA,dlss_schema).

-define(UNDEFINED,undefined).

%---------------------Storage types-------------------------------
-define(T_RAM,ram).
-define(T_RAMDISC,ramdisc).
-define(T_DISC,disc).

% Type modules
-define(M_RAM,dlss_ram).
-define(M_RAMDISC,dlss_ramdisc).
-define(M_DISC,dlss_disc).

%----------------------Internal API-------------------------------
-define(SCHEMA_READ(K),
  case dlss_ramdisc:read(?SCHEMA,[K]) of
    []->?UNDEFINED;
    [_@V]->_@V
  end).
-define(SCHEMA_WRITE(K,V),dlss_ramdisc:write(?SCHEMA,[{K,V}])).
-define(SCHEMA_DELETE(K),dlss_ramdisc:delete(?SCHEMA,[K])).
-define(SCHEMA_MATCH(P),dlss_ramdisc:match(?SCHEMA, P)).

%-------------------------Nodes API---------------------------------
-define(NODES, ?SCHEMA_READ(nodes)).

-define(READY_NODES,
  case ?SCHEMA_READ(ready_nodes) of
    ?UNDEFINED->[];
    _@RNs->_@RNs
  end).

-define(N_ADD(N),?SCHEMA_WRITE(nodes,[N|(?NODES -- [N])).
-define(N_REMOVE(N),?SCHEMA_WRITE(nodes,?NODES -- [N]).

-define(N_UP(N),?SCHEMA_WRITE(ready_nodes,[N|(?READY_NODES -- [N])).
-define(N_DOWN(N),?SCHEMA_WRITE(ready_nodes,(?READY_NODES -- [N]).

%-------------------------Segments API------------------------------
-define(S_MODULE(S),?SCHEMA_READ({S,module})).
-define(S_TYPE(S),
  _@M = ?S_MODULE(S),
  if
    _@M =:= ?M_RAM->?T_RAM;
    _@M =:= ?M_RAMDISC->?T_RAMDISC;
    _@M =:= ?M_DISC -> ?T_DISC
  end
).

-define(S_NODES(S),
  case ?SCHEMA_READ({S,nodes}) of
    ?UNDEFINED ->[];
    _@SNs->_@SNs
  end).
-define(X_NODES(Ns),Ns -- (Ns -- ?READY_NODES)).
-define(S_READY_NODES(S),?X_NODES(?S_NODES(S), ?READY_NODES)).

-define(I_HAVE(S),
  case ?SCHEMA_READ({S,i_have}) of
    ?UNDEFINED->false;
    _->true
  end).

-define(S_SOURCE(S),
  case ?I_HAVE(Segment) of
    true -> node();
    _->
      case ?S_READY_NODES(Segment) of
        []->?UNDEFINED;
        Nodes -> ?RAND( Nodes )
      end
  end).

-define(S_CREATE(S,M,Ns),
  case ?S_MODULE(S) of
    ?UNDEFINED->
      ?SCHEMA_WRITE({Segment, module}, Module),
      ?SCHEMA_WRITE({Segment, nodes}, Nodes ),
      case lists:member( node(), Nodes ) of
        true->
          ok = ?SCHEMA_WRITE({Segment, i_have}, true );
        _->
          ok
      end;
    _->
      ok  % triangle? ok or throw?
  end).
-define(S_DELETE,
  begin
    ?SCHEMA_DELETE( {Segment, i_have} ),
    ?SCHEMA_DELETE( {Segment, module} ),
    ?SCHEMA_DELETE( {Segment, nodes} )
  end).

-define(S_ADD, ?SCHEMA_WRITE({Segment,i_have}, true)).
-define(S_REMOVE, ?SCHEMA_DELETE({Segment,i_have})).

-define(S_ADD_NODE(S,N),?SCHEMA_WRITE({S, nodes}, [N|?S_NODES(S)--[N]] )).
-define(S_REMOVE_NODE(S,N),?SCHEMA_WRITE({S, nodes}, ?S_NODES(S)--[N] )).

%-------------------------Utilities---------------------------------------
-define(RAND(List),
  begin
    _@I = erlang:phash2(make_ref(),length(List)),
    lists:nth(_@I+1, List)
  end).

-endif.
