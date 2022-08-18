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

-module(dlss).

%%=================================================================
%%	Service API
%%=================================================================
-export([
  add_node/1,remove_node/1,
  get_nodes/0,
  get_ready_nodes/0,
  get_active_nodes/0,
  get_storages/0,
  get_storage_type/1,
  get_storage_root/1,
  is_local_storage/1,
  get_storage_efficiency/1,
  rebalance_storage/1,
  get_segments/0,get_segments/1,
  get_node_segments/1,
  get_local_segments/0,
  get_segment_info/1,
  get_segment_params/1,
  verify_hash/0, verify_hash/1,
  get_segment_size/1,
  add_storage/2,add_storage/3,
  storage_limits/1, storage_limits/2,
  default_limits/0, default_limits/1,
  backend_env/0,backend_env/1,
  add_segment_copy/2, remove_segment_copy/2,
  remove_storage/1,
  stop/0
]).

%%=================================================================
%%	Data API
%%=================================================================
-export([
  transaction/1, sync_transaction/1,
  read/2, read/3, dirty_read/2,
  write/3, write/4, dirty_write/3,
  delete/2, delete/3, dirty_delete/2,
  first/1, dirty_first/1, last/1, dirty_last/1,
  next/2, dirty_next/2, prev/2, dirty_prev/2,
  dirty_range_select/3, dirty_range_select/4
]).

-type storage_type() :: ram | ramdisc | disc.
-type segment_info() :: #{ type => storage_type(), local => true | false, nodes => list()}.
-type lock_type() :: write | sticky_write | read | none.

%%---------------------------------------------------------------
%%	SERVICE API
%%---------------------------------------------------------------

%-----------------------------------------------------------------
%% @doc Add a new node to the schema.
%% Returns true when a new node is added or false in other case
%% @end
%-----------------------------------------------------------------
-spec add_node(Node :: node()) -> true | false.
add_node(Node)->
  dlss_node:add( Node ).


%-----------------------------------------------------------------
%% @doc  Remove a node from the schema.
% Returns {atomic, ok} when node is removed or {aborted, Reason}
% in negative case
%% @end
%-----------------------------------------------------------------
-spec remove_node(Node :: node()) -> {atomic, ok} | {aborted, Reason :: term()}.
remove_node(Node)->
  dlss_node:remove( Node ).

%-----------------------------------------------------------------
%% @doc	Get list of all dlss nodes
% Returns
% [node1,node2 ..]
% where the type of a name of a node is an atom
%% @end
%-----------------------------------------------------------------
-spec get_nodes() -> ListOfNode :: list().
get_nodes()->
  dlss_node:get_nodes().

%-----------------------------------------------------------------
%% @doc	Get list of ready dlss nodes
% Returns
% [node1,node2 ..]
% where the type of a name of a node is an atom
%% @end
%-----------------------------------------------------------------
-spec get_ready_nodes() -> ListOfNode :: list().
get_ready_nodes()->
  dlss_node:get_ready_nodes().

%-----------------------------------------------------------------
%% @doc	Get list of active dlss nodes
% Returns
% [node1,node2 ..]
% where the type of a name of a node is an atom
%% @end
%-----------------------------------------------------------------
get_active_nodes()->
  dlss_backend:get_active_nodes().

%-----------------------------------------------------------------
%% @doc	Get list of all dlss storages.
% Returns:
% [storage1,storage2 ..]
% where the type of name of storage is the atom
%% @end
%-----------------------------------------------------------------
-spec get_storages() -> ListOfStorage :: list().
get_storages()->
  dlss_storage:get_storages().

%-----------------------------------------------------------------
%% @doc	Get storage type.
%% Returns: ram | ramdisc | disc.
%% Each type defines where is the storage was added.
%% @end
%-----------------------------------------------------------------
-spec get_storage_type(Storage :: atom()) -> storage_type() | no_return().
get_storage_type(Storage) ->
  dlss_storage:get_type(Storage).

%-----------------------------------------------------------------
%% @doc	Get storage root segment.
%% Returns: the name of the current root segment of the storage.
%% @end
%-----------------------------------------------------------------
-spec get_storage_root(Storage :: atom()) -> storage_type() | no_return().
get_storage_root(Storage) ->
  dlss_storage:root_segment(Storage).
%-----------------------------------------------------------------
%% @doc Check if the storage has local only content
%% @end
%-----------------------------------------------------------------
-spec is_local_storage(Storage :: atom()) -> boolean() | no_return().
is_local_storage(Storage) ->
  dlss_storage:is_local(Storage).

%-----------------------------------------------------------------
%% @doc Calculates the storage efficiency for iterate operations like
%%  first, next, prev, last, dirty_range_select.
%%  Their efficiency depends on the count of @deleted@ keys in the root
%%  segment.
%%  The value is in range 0-1 the bigger the better
%% @end
%-----------------------------------------------------------------
-spec get_storage_efficiency(Storage :: atom()) -> float().
get_storage_efficiency(Storage) ->
  dlss_storage_srv:get_efficiency(Storage).

%-----------------------------------------------------------------
%% @doc Triggers the storage rebalancing procedure. A new root
%%  segment will be created and the previous root segment will be
%%  merged with low level segments
%% @end
%-----------------------------------------------------------------
-spec rebalance_storage(Storage :: atom()) -> ok | no_return().
rebalance_storage(Storage) ->
  dlss_storage_srv:rebalance(Storage).

%-----------------------------------------------------------------
%% @doc Get list of all dlss segments.
% Returns:
% [dlss_storage1_1,dlss_storage1_2,dlss_storage2_1 ..],
% where the element of list is the name of segments and has type
% of atom
%% @end
%-----------------------------------------------------------------
-spec get_segments() -> AllSegments :: list().
get_segments()->
  dlss_storage:get_segments().

%-----------------------------------------------------------------
%% @doc Get list of dlss segments for the Storage.
% Returns:
% [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3 ..]
% where the element of list is the name of segments of storage storage1
% and has type of atom
%% @end
%-----------------------------------------------------------------
-spec get_segments(Storage :: atom()) -> StorageSegments :: list().
get_segments(Storage)->
  dlss_storage:get_segments(Storage).

%-----------------------------------------------------------------
%% @doc Get list of dlss segments for the Node.
% Returns:
% [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3 ..]
% where the element of list is the name of segments of storage storage1
% and has type of atom
%% @end
%-----------------------------------------------------------------
-spec get_node_segments(Node :: atom()) -> NodeSegments :: list().
get_node_segments(Node)->
  dlss_storage:get_segments(Node).

%-----------------------------------------------------------------
%% @doc Get list of dlss segments that has local copies.
% Returns:
% [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3 ..]
% where the element of list is the name of segments
% and has type of atom
%% @end
%-----------------------------------------------------------------
get_local_segments()->
  dlss_segment:get_local_segments().

%-----------------------------------------------------------------
%% @doc  Get segment info.
% Returns map:
% #{
%   type => Type,      :: disc | ram | ramdisc
%   local => Local,    :: true | false
%   nodes => Nodes     :: list of atom() [node(),..]
%   }
% or throws Error
%% @end
%-----------------------------------------------------------------
-spec get_segment_info(Segment :: atom()) -> SegmentInfo :: segment_info() | no_return().
get_segment_info(Segment) ->
  dlss_segment:get_info(Segment).

%-----------------------------------------------------------------
%% @doc  Get segment params.
% Returns map:
% #{
%   storage => NameOfStorageSegmentBelongsTo,
%   level => LevelOfSegmentInStorage,
%   key => TheFirstKeyInSegment,
%   version => VersionOfSegment,
%   copies => #{
%     <node1> => #{ Params as hash etc },
%     <node2> => #{ Params as hash etc },
%     ...
%   }
% }
% or throws Error
%% @end
%-----------------------------------------------------------------
-spec get_segment_params(Segment :: atom()) -> SegmentInfo :: segment_info() | no_return().
get_segment_params(Segment) ->
  dlss_storage:segment_params(Segment).

%-----------------------------------------------------------------
%% @doc  Get segment size.
% Returns number of bytes occupied by the segment
% or throws Error
%% @end
%-----------------------------------------------------------------
-spec get_segment_size(Segment :: atom()) -> Size :: integer() | no_return().
get_segment_size(Segment) ->
  dlss_segment:get_size(Segment).

%-----------------------------------------------------------------
%% @doc  Verify hash values for the node's segments.
%% @end
%-----------------------------------------------------------------
-spec verify_hash() -> ok | no_return().
verify_hash() ->
  [ dlss:verify_hash( N ) || N <- get_ready_nodes()],
  ok.
%-----------------------------------------------------------------
%% @doc  Verify hash values for the node's segments.
% As input function gets Name of storage as atom,
%% @end
%-----------------------------------------------------------------
-spec verify_hash(Node :: atom()) -> ok | no_return().
verify_hash(Node) ->
  case node() of
    Node -> dlss_storage_srv:verify_hash();
    _ -> spawn(Node, fun()->dlss:verify_hash(Node) end)
  end.
%-----------------------------------------------------------------
%% @doc 	Add storage.
% It adds a new storage to dlss_schema with creating a new Root Segment (table)
% As input function gets Name of storage as atom and Type as atom.
% Returns ok, or throws Error
%% @end
%-----------------------------------------------------------------
-spec add_storage(Name :: atom(), Type :: storage_type()) -> ok | no_return().
add_storage(Name,Type)->
  dlss_storage:add(Name,Type).

%-----------------------------------------------------------------
%% @doc  Add storage with Options.
% Function adds a new storage to dlss_schema with creating a new Root Segment (table)
% As input function gets Name of storage as atom,
% Type as atom, and Options as map of
% #{
%   type:=Type            :: disc | ram | ramdisc
%   nodes:=Nodes,         :: list of atom() [node(),..]
%   local:=IsLocal,       :: true | false
%   limits:=Limits        :: map
% }
% Options might be used to change default values of nodes.
% Returns: ok, or throws Error
%% @end
%-----------------------------------------------------------------
-spec add_storage(Name :: atom(), Type :: storage_type(), Options :: segment_info()) -> ok | no_return().
add_storage(Name,Type,Options)->
  dlss_storage:add(Name,Type,Options).

%-----------------------------------------------------------------
%% @doc 	Remove storage.
% Function removes the storage from dlss_schema and deletes all related
% segments (tables)
% As input function gets Name of storage as atom
% Returns ok, or throws Error
%% @end
%-----------------------------------------------------------------
-spec remove_storage(Name :: atom()) -> ok | no_return().
remove_storage(Name)->
  dlss_storage:remove(Name).

%-----------------------------------------------------------------
%% @doc  Get default limits for storages.
% Function returns actual default limits settings
% Returns limits in format:
% #{
%   0 := BufferLimit      :: integer (MB)
%   1 := SegmentLimit    :: integer (MB)
% }
%% @end
%-----------------------------------------------------------------
default_limits()->
  dlss_storage:default_limits().

%-----------------------------------------------------------------
%% @doc  Set default storage limits.
% Limits has format:
% #{
%   0 := BufferLimit      :: integer (MB)
%   1 := SegmentLimit    :: integer (MB)
% }
% Returns: ok, or throws Error
%% @end
%-----------------------------------------------------------------
default_limits( Limits )->
  dlss_storage:default_limits( Limits ).

%-----------------------------------------------------------------
%% @doc  Get storage limits by storage name Storage.
% Function returns actual limits settings for the Storage
% As input function gets Name of storage as atom,
% Returns limits in format:
% #{
%   0 := BufferLimit      :: integer (MB)
%   1 := SegmentLimit    :: integer (MB)
% }
%% @end
%-----------------------------------------------------------------
storage_limits( Storage )->
  dlss_storage:storage_limits( Storage ).

%-----------------------------------------------------------------
%% @doc  Set storage limits by storage name Storage.
% Limits has format:
% #{
%   0 := BufferLimit      :: integer (MB)
%   1 := SegmentLimit    :: integer (MB)
% }
% Returns: ok, or throws Error
%% @end
%-----------------------------------------------------------------
storage_limits( Storage, Limits )->
  dlss_storage:storage_limits( Storage, Limits ).

%-----------------------------------------------------------------
%% @doc  Get backend settings.
% Function returns actual limits settings the backend
% Returns limits in format of a map
%% @end
%-----------------------------------------------------------------
backend_env()->
  dlss_backend:env().

%-----------------------------------------------------------------
%% @doc  Set backend settings.
% Function sets settings for the backend
% Returns ok or throws Error
%% @end
%-----------------------------------------------------------------
backend_env( Settings )->
  dlss_backend:env( Settings ).

%-----------------------------------------------------------------
%% @doc
%	Stop the DLSS
% Returns ok, or { error, Reason}.
%% @end
%-----------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: any() }.
stop()->
  application:stop(dlss).

%-----------------------------------------------------------------
%% @doc Add segment_copy to node.
% Function copies the Segment (table) and puts to Node.
% As input function gets Name of segment (atom), and Node
% Returns ok, or { error, Reason}.
%% @end
%-----------------------------------------------------------------
-spec add_segment_copy(Segment :: atom(), Node :: node()) -> ok | { error, Reason :: any() }.
add_segment_copy(Segment,Node)->
  dlss_storage:add_segment_copy( Segment, Node ).

%-----------------------------------------------------------------
%% @doc Remove segment_copy from node.
% Function removes the Segment (table) from Node.
% As input function gets Name of segment (atom), and Node
% Returns ok, or { error, Reason}.
%% @end
%-----------------------------------------------------------------
-spec remove_segment_copy(Segment :: atom(), Node :: node()) -> ok | { error, Reason :: any() }.
remove_segment_copy(Segment,Node)->
  dlss_storage:remove_segment_copy(Segment,Node).

%%---------------------------------------------------------------
%%	DATA API
%%---------------------------------------------------------------
%-----------------------------------------------------------------
%% @doc	Wrap the procedure into the ACID transaction.
%% @end
%-----------------------------------------------------------------
-spec transaction(Fun :: fun()) -> {ok, FunResult :: any()} | {error, Reason :: any()}.
transaction(Fun)->
  % We use the backend transaction engine
  dlss_backend:transaction(Fun).

%-----------------------------------------------------------------
%% @doc	Wrap the procedure into the ACID transaction.
%% Sync transaction wait all changes are applied
%% @end
%-----------------------------------------------------------------
-spec sync_transaction(Fun :: fun()) -> {ok, FunResult :: any()} | {error, Reason :: any()}.
sync_transaction(Fun)->
  dlss_backend:sync_transaction(Fun).

%%=================================================================
%%	Read/Write/Delete
%%=================================================================

%---------------------Read-----------------------------------------
%% @doc Read.
%% Function reads the value from Storage with Key.
% The function needs to be wrapped in transaction.
% Returns Value or not_found.
%% @end
%------------------------------------------------------------------
-spec read(Storage :: atom(), Key :: any()) -> Value :: any() | not_found.
read(Storage, Key ) ->
    dlss_storage:read(Storage, Key).

%---------------------Read with lock-------------------------------
%% @doc Read.
%% Function reads the value from Storage with Key and lock_type.
% The function needs to be wrapped in transaction.
% Returns Value or not_found.
%% @end
%------------------------------------------------------------------
-spec read(Storage :: atom(), Key :: any(), Lock :: lock_type()) -> Value :: any() | not_found.
read(Storage, Key, Lock) ->
  dlss_storage:read(Storage, Key, Lock).

%---------------------Dirty Read-----------------------------------
%% @doc Dirty Read.
% Function reads the value from Storage with Key.
% There is no needs of wrapping in transaction, when using dirty_read
% Returns Value or not_found.
%% @end
%------------------------------------------------------------------
-spec dirty_read(Storage :: atom(), Key :: any()) -> Value :: any() | not_found.
dirty_read(Storage, Key ) ->
  dlss_storage:dirty_read(Storage, Key).


%---------------------Write---------------------------------------
%% @doc Write.
% Function writes the Value to the Storage with Key.
% If there is a Key in Storage it just updates,
% else it adds new #kv{key:=Key,value:=Value} to the Storage.
% The function needs to be wrapped in transaction.
% Returns ok or throws Error.
%% @end
%------------------------------------------------------------------
-spec write(Storage :: atom(), Key :: any(), Value :: any()) -> ok | no_return().
write(Storage, Key, Value)->
  dlss_storage:write(Storage, Key, Value).

%---------------------Write with lock------------------------------
%% @doc Write.
% Function writes the Value to the Storage with Key and write_locks.
% If there is a Key in Storage it just updates,
% else it adds new #kv{key:=Key,value:=Value} to the Storage.
% The function needs to be wrapped in transaction.
% Returns ok or throws Error.
%% @end
%------------------------------------------------------------------
-spec write(Storage :: atom(), Key :: any(), Value :: any(), Lock :: lock_type()) -> ok | no_return().
write(Storage, Key, Value, Lock)->
  dlss_storage:write(Storage, Key, Value, Lock).

%---------------------Dirty Write----------------------------------
%% @doc Dirty Write.
% Function writes the Value to the Storage with Key.
% If there is a Key in Storage it just updates,
% else it adds new #kv{key:=Key,value:=Value} to the Storage.
% There is no needs of wrapping in transaction, when using dirty_write
% Returns ok or throws Error.
%% @end
%------------------------------------------------------------------
-spec dirty_write(Storage :: atom(), Key :: any(), Value :: any()) -> ok | no_return().
dirty_write(Storage, Key, Value)->
  dlss_storage:dirty_write(Storage, Key, Value).

%---------------------Delete---------------------------------------
%% @doc Delete.
% Function updates the Value to the '@deleted@' in the Storage with Key,
% which will be ignored on read().
% The function needs to be wrapped in transaction.
% Returns ok or throws Error.
%% @end
%------------------------------------------------------------------
-spec delete(Storage :: atom(), Key :: any()) -> ok | no_return().
delete(Storage, Key)->
  dlss_storage:delete(Storage, Key).

%---------------------Delete---------------------------------------
%% @doc Delete.
% Function updates the Value to the '@deleted@' in the Storage with Key
% and write_lock, which will be ignored on read().
% The function needs to be wrapped in transaction.
% Returns ok or throws Error.
%% @end
%------------------------------------------------------------------
-spec delete(Storage :: atom(), Key :: any(), Lock :: lock_type()) -> ok | no_return().
delete(Storage, Key, Lock)->
  dlss_storage:delete(Storage, Key, Lock).

%---------------------Dirty Delete---------------------------------
%% @doc Dirty Delete.
% Function updates the Value to the '@deleted@' in the Storage with Key
% which will be ignored on read().
% There is no needs of wrapping in transaction, when using dirty_delete
% Returns ok or throws Error.
%% @end
%------------------------------------------------------------------
-spec dirty_delete(Storage :: atom(), Key :: any()) -> ok | no_return().
dirty_delete(Storage, Key)->
  dlss_storage:dirty_delete(Storage, Key).

%%=================================================================
%%	Iterate
%%=================================================================
%-----------------------------------------------------------------
%% @doc First.
% Function gets the first Key of the Storage
% As input function gets Name of storage as atom
% The function needs to be wrapped in transaction.
% Returns Key, or throws Error
%% @end
%-----------------------------------------------------------------
-spec first(Storage :: atom()) -> Key :: any() | no_return().
first(Storage)->
  dlss_storage:first(Storage).

%-----------------------------------------------------------------
%% @doc	Dirty First.
% Function gets the first Key of the Storage
% As input function gets Name of storage as atom
% There is no needs of wrapping in transaction, when using dirty_first
% Returns Key, or throws Error
%% @end
%-----------------------------------------------------------------
-spec dirty_first(Storage :: atom()) -> Key :: any() | no_return().
dirty_first(Storage)->
  dlss_storage:dirty_first(Storage).

%-----------------------------------------------------------------
%% @doc	Last.
% Function gets the last Key of the Storage
% As input function gets Name of storage as atom
% The function needs to be wrapped in transaction.
% Returns Key, or throws Error
%% @end
%-----------------------------------------------------------------
-spec last(Storage :: atom()) -> Key :: any() | no_return().
last(Storage)->
  dlss_storage:last(Storage).

%-----------------------------------------------------------------
%% @doc	Dirty Last.
% Function gets the last Key of the Storage
% As input function gets Name of storage as atom
% There is no needs of wrapping in transaction, when using dirty_last
% Returns Key, or throws Error
%% @end
%-----------------------------------------------------------------
-spec dirty_last(Storage :: atom()) -> Key :: any() | no_return().
dirty_last(Storage)->
  dlss_storage:dirty_last(Storage).

%-----------------------------------------------------------------
%% @doc	Next.
% Function gets the next key of the Storage, from given Key
% As input function gets Name of storage as atom and pivot Key
% The function needs to be wrapped in transaction.
% Returns RetKey, or '$end_of_table' or  throws Error
%% @end
%-----------------------------------------------------------------
-spec next(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
next( Storage, Key )->
  dlss_storage:next( Storage, Key ).

%-----------------------------------------------------------------
%% @doc	Dirty Next.
% Function gets the next key of the Storage, from given Key
% As input function gets Name of storage as atom and pivot Key
% There is no needs of wrapping in transaction, when using dirty_next
% Returns RetKey, or '$end_of_table' or  throws Error
%% @end
%-----------------------------------------------------------------
-spec dirty_next(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
dirty_next(Storage,Key)->
  dlss_storage:dirty_next(Storage,Key).

%-----------------------------------------------------------------
%% @doc	Previous.
% Function gets the previous key of the Storage, from given Key
% As input function gets Name of storage as atom and pivot Key
% The function needs to be wrapped in transaction.
% Returns RetKey, or '$end_of_table' or  throws Error
%% @end
%-----------------------------------------------------------------
-spec prev(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
prev( Storage, Key )->
  dlss_storage:prev( Storage, Key ).

%-----------------------------------------------------------------
%% @doc	Dirty Previous.
% Function gets the previous key of the Storage, from given Key
% As input function gets Name of storage as atom and pivot Key
% There is no needs of wrapping in transaction, when using dirty_previous
% Returns RetKey, or '$end_of_table' or  throws Error
%% @end
%-----------------------------------------------------------------
-spec dirty_prev(Storage :: atom(), Key :: any()) -> RetKey :: any() | '$end_of_table' | no_return().
dirty_prev(Storage,Key)->
  dlss_storage:dirty_prev(Storage,Key).


%-----------------------------------------------------------------
%% @doc	Dirty Range Select.
% Function searches the storage for keys relating to the defined range of keys.
% StartKey and EndKey define the range to run trough.
% '$start_of_table' and '$end_of_table' are supported.
% Returns a list of found items [{ Key, Value}|...]
%% @end
%-----------------------------------------------------------------
-spec dirty_range_select(Storage :: atom(), StartKey :: any(), EndKey ::any() ) -> Items :: list() | no_return().
dirty_range_select(Storage, StartKey, EndKey) ->
  dlss_storage:dirty_range_select(Storage,StartKey,EndKey).

%-----------------------------------------------------------------
%% @doc	Dirty Range Select.
% Function searches the storage for keys relating to the defined range of keys.
% StartKey and EndKey define the interval to run trough.
% Limit defines the maximum number of item to return
% '$start_of_table' and '$end_of_table' are supported.
% Returns a list of found items [{ Key, Value}|...]
%% @end
%-----------------------------------------------------------------
-spec dirty_range_select(Storage :: atom(), StartKey :: any(), EndKey ::any(), Limit :: integer() ) -> Items :: list() | no_return().
dirty_range_select(Storage, StartKey, EndKey, Limit) ->
  dlss_storage:dirty_range_select(Storage,StartKey,EndKey,Limit).

% Update API docs
% edoc:files(["src/dlss.erl"],[{dir, "doc"}]).