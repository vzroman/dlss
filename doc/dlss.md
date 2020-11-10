* * *

# Module dlss

*   [Data Types](#types)
*   [Function Index](#index)
*   [Function Details](#functions)

## <a name="types">Data Types</a>

### <a name="type-lock_type">lock_type()</a>

<tt>lock_type() = [write_locks()](#type-write_locks) | [read_locks()](#type-read_locks)</tt>

### <a name="type-read_locks">read_locks()</a>

<tt>read_locks() = read</tt>

### <a name="type-segment_info">segment_info()</a>

<tt>segment_info() = #{type => [storage_type()](#type-storage_type), local => true | false, nodes => list()}</tt>

### <a name="type-storage_type">storage_type()</a>

<tt>storage_type() = ram | ramdisc | disc</tt>

### <a name="type-write_locks">write_locks()</a>

<tt>write_locks() = write | sticky_write</tt>

## <a name="index">Function Index</a>

<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index">

<tbody>

<tr>

<td valign="top"><a href="#add_node-1">add_node/1</a></td>

<td>Add a new node to the schema.</td>

</tr>

<tr>

<td valign="top"><a href="#add_segment_copy-2">add_segment_copy/2</a></td>

<td>Add segment_copy to node.</td>

</tr>

<tr>

<td valign="top"><a href="#add_storage-2">add_storage/2</a></td>

<td>Add storage.</td>

</tr>

<tr>

<td valign="top"><a href="#add_storage-3">add_storage/3</a></td>

<td>Add storage with Options.</td>

</tr>

<tr>

<td valign="top"><a href="#delete-2">delete/2</a></td>

<td>Delete.</td>

</tr>

<tr>

<td valign="top"><a href="#delete-3">delete/3</a></td>

<td>Delete.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_delete-2">dirty_delete/2</a></td>

<td>Dirty Delete.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_first-1">dirty_first/1</a></td>

<td>Dirty First.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_last-1">dirty_last/1</a></td>

<td>Dirty Last.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_next-2">dirty_next/2</a></td>

<td>Dirty Next.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_prev-2">dirty_prev/2</a></td>

<td>Dirty Previous.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_read-2">dirty_read/2</a></td>

<td>Dirty Read.</td>

</tr>

<tr>

<td valign="top"><a href="#dirty_write-3">dirty_write/3</a></td>

<td>Dirty Write.</td>

</tr>

<tr>

<td valign="top"><a href="#first-1">first/1</a></td>

<td>First.</td>

</tr>

<tr>

<td valign="top"><a href="#get_segment_info-1">get_segment_info/1</a></td>

<td>Get segment info.</td>

</tr>

<tr>

<td valign="top"><a href="#get_segments-0">get_segments/0</a></td>

<td>Get list of all dlss segments.</td>

</tr>

<tr>

<td valign="top"><a href="#get_segments-1">get_segments/1</a></td>

<td>Get list of dlss segments for the Storage.</td>

</tr>

<tr>

<td valign="top"><a href="#get_storage_type-1">get_storage_type/1</a></td>

<td>Get storage type.</td>

</tr>

<tr>

<td valign="top"><a href="#get_storages-0">get_storages/0</a></td>

<td>Get list of all dlss storages.</td>

</tr>

<tr>

<td valign="top"><a href="#last-1">last/1</a></td>

<td>Last.</td>

</tr>

<tr>

<td valign="top"><a href="#next-2">next/2</a></td>

<td>Next.</td>

</tr>

<tr>

<td valign="top"><a href="#prev-2">prev/2</a></td>

<td>Previous.</td>

</tr>

<tr>

<td valign="top"><a href="#read-2">read/2</a></td>

<td>Read.</td>

</tr>

<tr>

<td valign="top"><a href="#read-3">read/3</a></td>

<td>Read.</td>

</tr>

<tr>

<td valign="top"><a href="#remove_node-1">remove_node/1</a></td>

<td>Remove a node from the schema.</td>

</tr>

<tr>

<td valign="top"><a href="#remove_segment_copy-2">remove_segment_copy/2</a></td>

<td>Remove segment_copy from node.</td>

</tr>

<tr>

<td valign="top"><a href="#remove_storage-1">remove_storage/1</a></td>

<td>Remove storage.</td>

</tr>

<tr>

<td valign="top"><a href="#sync_transaction-1">sync_transaction/1</a></td>

<td>Wrap the procedure into the ACID transaction.</td>

</tr>

<tr>

<td valign="top"><a href="#transaction-1">transaction/1</a></td>

<td>Wrap the procedure into the ACID transaction.</td>

</tr>

<tr>

<td valign="top"><a href="#write-3">write/3</a></td>

<td>Write.</td>

</tr>

<tr>

<td valign="top"><a href="#write-4">write/4</a></td>

<td>Write.</td>

</tr>

</tbody>

</table>

## <a name="functions">Function Details</a>

### <a name="add_node-1">add_node/1</a>

<div class="spec">

<tt>add_node(Node::node()) -> true | false</tt>  

</div>

Add a new node to the schema. Returns true when a new node is added or false in other case

### <a name="add_segment_copy-2">add_segment_copy/2</a>

<div class="spec">

<tt>add_segment_copy(Segment::atom(), Node::node()) -> ok | {error, Reason::any()}</tt>  

</div>

Add segment_copy to node. Function copies the Segment (table) and puts to Node. As input function gets Name of segment (atom), and Node Returns ok, or { error, Reason}.

### <a name="add_storage-2">add_storage/2</a>

<div class="spec">

<tt>add_storage(Name::atom(), Type::[storage_type()](#type-storage_type)) -> ok | no_return()</tt>  

</div>

Add storage. It adds a new storage to dlss_schema with creating a new Root Segment (table) As input function gets Name of storage as atom and Type as atom. Returns ok, or throws Error

### <a name="add_storage-3">add_storage/3</a>

<div class="spec">

<tt>add_storage(Name::atom(), Type::[storage_type()](#type-storage_type), Options::[segment_info()](#type-segment_info)) -> ok | no_return()</tt>  

</div>

Add storage with Options. Function adds a new storage to dlss_schema with creating a new Root Segment (table) As input function gets Name of storage as atom, Type as atom, and Options as map of #{ type:=Type :: disc | ram | ramdisc nodes:=Nodes, :: list of atom() [node(),..] local:=IsLocal :: true | false } Options might be used to change default values of nodes and local. Returns: ok, or throws Error

### <a name="delete-2">delete/2</a>

<div class="spec">

<tt>delete(Storage::atom(), Key::any()) -> ok | no_return()</tt>  

</div>

Delete. Function updates the Value to the '@deleted@' in the Storage with Key, which will be ignored on read(). The function needs to be wrapped in transaction. Returns ok or throws Error.

### <a name="delete-3">delete/3</a>

<div class="spec">

<tt>delete(Storage::atom(), Key::any(), Lock::[write_locks()](#type-write_locks)) -> ok | no_return()</tt>  

</div>

Delete. Function updates the Value to the '@deleted@' in the Storage with Key and write_lock, which will be ignored on read(). The function needs to be wrapped in transaction. Returns ok or throws Error.

### <a name="dirty_delete-2">dirty_delete/2</a>

<div class="spec">

<tt>dirty_delete(Storage::atom(), Key::any()) -> ok | no_return()</tt>  

</div>

Dirty Delete. Function updates the Value to the '@deleted@' in the Storage with Key which will be ignored on read(). There is no needs of wrapping in transaction, when using dirty_delete Returns ok or throws Error.

### <a name="dirty_first-1">dirty_first/1</a>

<div class="spec">

<tt>dirty_first(Storage::atom()) -> Key::any() | no_return()</tt>  

</div>

Dirty First. Function gets the first Key of the Storage As input function gets Name of storage as atom There is no needs of wrapping in transaction, when using dirty_first Returns Key, or throws Error

### <a name="dirty_last-1">dirty_last/1</a>

<div class="spec">

<tt>dirty_last(Storage::atom()) -> Key::any() | no_return()</tt>  

</div>

Dirty Last. Function gets the last Key of the Storage As input function gets Name of storage as atom There is no needs of wrapping in transaction, when using dirty_last Returns Key, or throws Error

### <a name="dirty_next-2">dirty_next/2</a>

<div class="spec">

<tt>dirty_next(Storage::atom(), Key::any()) -> RetKey::any() | '$end_of_table' | no_return()</tt>  

</div>

Dirty Next. Function gets the next key of the Storage, from given Key As input function gets Name of storage as atom and pivot Key There is no needs of wrapping in transaction, when using dirty_next Returns RetKey, or '$end_of_table' or throws Error

### <a name="dirty_prev-2">dirty_prev/2</a>

<div class="spec">

<tt>dirty_prev(Storage::atom(), Key::any()) -> RetKey::any() | '$end_of_table' | no_return()</tt>  

</div>

Dirty Previous. Function gets the previous key of the Storage, from given Key As input function gets Name of storage as atom and pivot Key There is no needs of wrapping in transaction, when using dirty_previous Returns RetKey, or '$end_of_table' or throws Error

### <a name="dirty_read-2">dirty_read/2</a>

<div class="spec">

<tt>dirty_read(Storage::atom(), Key::any()) -> Value::any() | not_found</tt>  

</div>

Dirty Read. Function reads the value from Storage with Key. There is no needs of wrapping in transaction, when using dirty_read Returns Value or not_found.

### <a name="dirty_write-3">dirty_write/3</a>

<div class="spec">

<tt>dirty_write(Storage::atom(), Key::any(), Value::any()) -> ok | no_return()</tt>  

</div>

Dirty Write. Function writes the Value to the Storage with Key. If there is a Key in Storage it just updates, else it adds new #kv{key:=Key,value:=Value} to the Storage. There is no needs of wrapping in transaction, when using dirty_write Returns ok or throws Error.

### <a name="first-1">first/1</a>

<div class="spec">

<tt>first(Storage::atom()) -> Key::any() | no_return()</tt>  

</div>

First. Function gets the first Key of the Storage As input function gets Name of storage as atom The function needs to be wrapped in transaction. Returns Key, or throws Error

### <a name="get_segment_info-1">get_segment_info/1</a>

<div class="spec">

<tt>get_segment_info(Segment::atom()) -> SegmentInfo::[segment_info()](#type-segment_info) | no_return()</tt>  

</div>

Get segment info. Returns map: #{ type => Type, :: disc | ram | ramdisc local => Local, :: true | false nodes => Nodes :: list of atom() [node(),..] } or throws Error

### <a name="get_segments-0">get_segments/0</a>

<div class="spec">

<tt>get_segments() -> AllSegments::list()</tt>  

</div>

Get list of all dlss segments. Returns: [dlss_storage1_1,dlss_storage1_2,dlss_storage2_1 ..], where the element of list is the name of segments and has type of atom

### <a name="get_segments-1">get_segments/1</a>

<div class="spec">

<tt>get_segments(Storage::atom()) -> StorageSegments::list()</tt>  

</div>

Get list of dlss segments for the Storage. Returns: [dlss_storage1_1,dlss_storage1_2,dlss_storage1_3 ..] where the element of list is the name of segments of storage storage1 and has type of atom

### <a name="get_storage_type-1">get_storage_type/1</a>

<div class="spec">

<tt>get_storage_type(Storage::atom()) -> [storage_type()](#type-storage_type) | no_return()</tt>  

</div>

Get storage type. Returns: ram | ramdisc | disc. Each type defines where is the storage was added.

### <a name="get_storages-0">get_storages/0</a>

<div class="spec">

<tt>get_storages() -> ListOfStorage::list()</tt>  

</div>

Get list of all dlss storages. Returns: [storage1,storage2 ..] where the type of name of storage is the atom

### <a name="last-1">last/1</a>

<div class="spec">

<tt>last(Storage::atom()) -> Key::any() | no_return()</tt>  

</div>

Last. Function gets the last Key of the Storage As input function gets Name of storage as atom The function needs to be wrapped in transaction. Returns Key, or throws Error

### <a name="next-2">next/2</a>

<div class="spec">

<tt>next(Storage::atom(), Key::any()) -> RetKey::any() | '$end_of_table' | no_return()</tt>  

</div>

Next. Function gets the next key of the Storage, from given Key As input function gets Name of storage as atom and pivot Key The function needs to be wrapped in transaction. Returns RetKey, or '$end_of_table' or throws Error

### <a name="prev-2">prev/2</a>

<div class="spec">

<tt>prev(Storage::atom(), Key::any()) -> RetKey::any() | '$end_of_table' | no_return()</tt>  

</div>

Previous. Function gets the previous key of the Storage, from given Key As input function gets Name of storage as atom and pivot Key The function needs to be wrapped in transaction. Returns RetKey, or '$end_of_table' or throws Error

### <a name="read-2">read/2</a>

<div class="spec">

<tt>read(Storage::atom(), Key::any()) -> Value::any() | not_found</tt>  

</div>

Read. Function reads the value from Storage with Key. The function needs to be wrapped in transaction. Returns Value or not_found.

### <a name="read-3">read/3</a>

<div class="spec">

<tt>read(Storage::atom(), Key::any(), Lock::[lock_type()](#type-lock_type)) -> Value::any() | not_found</tt>  

</div>

Read. Function reads the value from Storage with Key and lock_type. The function needs to be wrapped in transaction. Returns Value or not_found.

### <a name="remove_node-1">remove_node/1</a>

<div class="spec">

<tt>remove_node(Node::node()) -> {atomic, ok} | {aborted, Reason::term()}</tt>  

</div>

Remove a node from the schema. Returns {atomic, ok} when node is removed or {aborted, Reason} in negative case

### <a name="remove_segment_copy-2">remove_segment_copy/2</a>

<div class="spec">

<tt>remove_segment_copy(Segment::atom(), Node::node()) -> ok | {error, Reason::any()}</tt>  

</div>

Remove segment_copy from node. Function removes the Segment (table) from Node. As input function gets Name of segment (atom), and Node Returns ok, or { error, Reason}.

### <a name="remove_storage-1">remove_storage/1</a>

<div class="spec">

<tt>remove_storage(Name::atom()) -> ok | no_return()</tt>  

</div>

Remove storage. Function removes the storage from dlss_schema and deletes all related segments (tables) As input function gets Name of storage as atom Returns ok, or throws Error

### <a name="sync_transaction-1">sync_transaction/1</a>

<div class="spec">

<tt>sync_transaction(Fun::function()) -> {ok, FunResult::any()} | {error, Reason::any()}</tt>  

</div>

Wrap the procedure into the ACID transaction. Sync transaction wait all changes are applied

### <a name="transaction-1">transaction/1</a>

<div class="spec">

<tt>transaction(Fun::function()) -> {ok, FunResult::any()} | {error, Reason::any()}</tt>  

</div>

Wrap the procedure into the ACID transaction.

### <a name="write-3">write/3</a>

<div class="spec">

<tt>write(Storage::atom(), Key::any(), Value::any()) -> ok | no_return()</tt>  

</div>

Write. Function writes the Value to the Storage with Key. If there is a Key in Storage it just updates, else it adds new #kv{key:=Key,value:=Value} to the Storage. The function needs to be wrapped in transaction. Returns ok or throws Error.

### <a name="write-4">write/4</a>

<div class="spec">

<tt>write(Storage::atom(), Key::any(), Value::any(), Lock::[write_locks()](#type-write_locks)) -> ok | no_return()</tt>  

</div>

Write. Function writes the Value to the Storage with Key and write_locks. If there is a Key in Storage it just updates, else it adds new #kv{key:=Key,value:=Value} to the Storage. The function needs to be wrapped in transaction. Returns ok or throws Error.

* * *

