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


-module(dlss_util).

-export([
    pretty_size/1,
    pretty_count/1
]).

pretty_size( Bytes )->
    pretty_print([
        {"TB", 2, 40},
        {"GB", 2, 30},
        {"MB", 2, 20},
        {"KB", 2, 10},
        {"B", 2, 0}
    ], Bytes).

pretty_count( Count )->
    pretty_print([
        {"bn", 10, 9},
        {"mn", 10, 6},
        {"ths", 10, 3},
        {"items", 10, 0}
    ], Count).

pretty_print( Units, Value )->
    Units1 = eval_units( Units, round(Value) ),
    Units2 = head_units( Units1 ),
    string:join([ integer_to_list(N) ++" " ++ U || {N,U} <- Units2 ],", ").


eval_units([{Unit, Base, Pow}| Rest], Value )->
    UnitSize = round( math:pow(Base, Pow) ),
    [{ Value div UnitSize, Unit} | eval_units(Rest, Value rem UnitSize)];
eval_units([],_Value)->
    [].

head_units([{N1,U1}|Rest]) when N1 > 0 ->
    case Rest of
        [{N2,U2}|_] when N2 > 0->
            [{N1,U1},{N2,U2}];
        _ ->
            [{N1,U1}]
    end;
head_units([Item])->
    [Item];
head_units([_Item|Rest])->
    head_units(Rest).


