# Architecture

`ray-hive` separates driver planning, immutable physical plans, and worker execution.

```text
HiveConnectionOptions + HiveReadOptions
  -> HivePlanner on the Ray driver
    -> HS2 schema-only operation
      -> immutable HiveTaskSpec
        -> Ray ReadTask
          -> task-owned I/O thread
            -> session -> operation -> sequential FetchResults
              -> Arrow Table blocks
```

The driver connects to HiveServer2 for table schema planning. Authenticated table reads therefore
resolve the endpoint-bound credential provider on both the trusted driver and workers. Raw SQL with
an explicit schema skips that planning connection. Neither process connects to the Hive Metastore
or storage, and the driver closes all planning resources before returning a Dataset. A task spec
contains a validated statement, complete expected schema, endpoint, non-plaintext credential
provider, timeouts, and fetch policy. It contains no live socket, operation handle, file path,
manifest, Hadoop split, or storage credential.

Immutable read options validate raw SQL and unsafe predicate confinement before planning. The
validator rejects server-side variable substitution as well as lexical statement escape, so an
invalid query cannot reach credential resolution, DNS, sockets, or session creation.

Each worker task owns one RPC client, session, operation, and single-thread executor. Only the I/O
thread performs Thrift RPCs. The generator thread submits one request at a time and yields one
Arrow block at a time. It can invoke a separate socket-only abort handle when a blocking RPC
prevents cooperative cancellation; that handle never sends protocol messages.

`query_timeout` covers server query execution through the FINISHED state. Time spent by downstream
consumers after a block is yielded does not consume that budget. The connection budget covers DNS
resolution and every TCP/TLS address attempt; individual fetch and cleanup RPCs remain bounded by
their dedicated RPC timeout. Since the platform resolver cannot be cancelled safely, each process
permits at most one late daemon resolver after a deadline so repeated timeouts cannot grow an
unbounded set of threads or block process exit.

The default strict plan creates one read task and submits one final data query. Multiple
`FetchResults` responses become multiple Arrow Tables. Source parallelism is deliberately one
because an HS2 operation is a sequential cursor. Downstream Ray transforms can consume the blocks
in parallel.

The top-level `read_hive()` facade owns strict Ray task settings. The lower-level
`ray_hive.datasource.HiveServer2Datasource` is a DeveloperAPI for advanced integrations; direct
Ray use is not a strict or no-replay profile.

Generated Apache Hive 4.2.0 TCLIService bindings are isolated below
`ray_hive._hs2.generated`. The native adapter immediately normalizes generated mutable objects into
typed internal values. Driver alternatives must implement the same protocol boundary and pass the
same conformance cases; they are never selected by fallback.
