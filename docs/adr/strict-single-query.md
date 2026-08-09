# Strict reads use one Hive query

Status: accepted

HiveServer2 exposes a sequential result cursor and no reliable resume token. Ray can make generator
blocks visible before the read task terminates, so retrying the task after failure can mix already
published results with a second query snapshot.

The Core design therefore creates one Ray read task, submits one final Hive data query, and forces
`max_retries=0`. It streams multiple Arrow Tables from sequential fetch calls. Object loss or
worker death fails the read instead of reconstructing the source query.

Parallel source queries remain an explicit experimental mode with independent-snapshot semantics.
They cannot be presented as a transparent performance implementation of strict reads.
