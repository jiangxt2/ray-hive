# Consistency

Strict mode is the default and only candidate for the Core profile. It uses one session, one data
operation, one sequence of fetch calls, and one Ray read task. All blocks therefore belong to the
same Hive query snapshot. Schema metadata is fetched again from the data operation and compared
with the complete planned Hive descriptor before the first block is published.

The public `read_hive()` facade forces `max_retries=0`. HiveServer2 provides no public resume token,
and Ray may consume streaming blocks before the remote task finishes. Re-executing a failed query
could append a new snapshot to already consumed blocks. The connector therefore never replays a
data query. A failure after output is classified as `TRANSPORT_AFTER_OUTPUT`, is non-retryable, and
fails the Dataset.

Ray `Dataset.limit()` remains a Ray-side row limit. The datasource passes Ray's
`per_task_row_limit` to `ReadTask`; it does not translate that optimizer hint into Hive `LIMIT`.
`HiveReadOptions.limit` is part of the source query and has different semantics.

Ray expression predicate pushdown is disabled in V1 because the supported Ray releases cannot return a partially
accepted predicate plus a residual. `HiveReadOptions.filter` uses ray-hive's complete typed
predicate tree and is compiled entirely into the source statement. A later `Dataset.filter()`
remains above the read.

Experimental independent-query mode creates one session and complete Hive query per explicit split
predicate. Those queries can observe different snapshots. It rejects a global source limit and is
not suitable for claims requiring one ACID or Iceberg snapshot.
