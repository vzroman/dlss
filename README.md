dlss
=====

**DLSS is a distributed key-value storage for Erlang/OTP applications.**

# Features

  * Keys and values are arbitrary Erlang terms.
  * Data is stored sorted by key.
  * Each storage is segmented. Segments can be distributed among Erlang nodes conforming a cluster. 
  * Full support of ACID transactions (many thanks to mnesia)
  * Support next types of storage:
    - ram. In-memory storage. Fast but not persistent (erlang ets is under the hood).
    - ramdisc. All the data is kept in memory (erlang ets), but the storage also provides persistence. Read/Scan operations are as fast as for 'ram' type. Write operations are a bit slower because of maintaining a sequential log.
    - disc. Persistent type storage. Read/Write/Scan operations are slower than for 'ram' and 'ramdisc' types. But the storage resides on the disc (except for buffer and cache) and therefore does not require a lot of RAM. This type of storage uses leveldb under the hood. Many thanks to developers of leveldb, eleveldb, and mnesia_eleveldb projects. You work is great!
  
Build
-----

    $ rebar3 compile
