# ray-hive

`ray-hive` is an independent HiveServer2 datasource for Ray Data. HiveServer2 owns
table-format, storage, authorization, and query execution. Ray workers receive Arrow batches only
through the HS2 endpoint; they do not read HDFS, object storage, table manifests, or the Hive
Metastore.

The project is currently Alpha. The initial compatibility target is Python 3.12/3.13, Ray 2.55.1,
PyArrow 19.0.1, and Apache Hive 4.2.0. Production Core status will not be claimed until the real
Hive, security, network-purity, cancellation, and Ray cluster conformance suites pass.

## Install

```bash
pip install ray-hive
```

The current native profile supports the binary Thrift transport with `NOSASL`. HTTP, SASL,
Kerberos, LDAP, CUSTOM authentication, and alternative drivers fail closed until their dedicated
extras and real infrastructure matrices are implemented and verified.

## Read a table

```python
from ray_hive import (
    HiveConnectionOptions,
    HiveReadOptions,
    HiveTableIdentifier,
    read_hive,
)

dataset = read_hive(
    connection=HiveConnectionOptions(
        host="hiveserver2.example.net",
        port=10000,
        database="analytics",
        auth="NOSASL",
        username="ray-reader",
    ),
    read=HiveReadOptions(
        table=HiveTableIdentifier(database="analytics", table="events"),
        columns=("event_id", "event_time", "score"),
    ),
)
```

The Alpha native NOSASL profile neither resolves nor sends a password. `SecretRef` and custom
credential providers are available for authenticated profiles under development: references are
bound to the configured endpoint and resolved only by a trusted process immediately before it
opens an HS2 session. Table reads perform schema planning on the Ray driver, so authenticated table
reads require the provider to be resolvable on both the driver and workers. Raw SQL with an explicit
schema skips driver-side HS2 planning and can keep credential resolution worker-only.

Structured source predicates are available from `ray_hive.sql`:

```python
from ray_hive.sql import col

read = HiveReadOptions(
    table=HiveTableIdentifier("analytics", "events"),
    filter=col("score").ge(0) & col("event_type").isin(["open", "close"]),
)
```

Raw SQL and `unsafe_where_sql` are trusted-code escape hatches. They are redacted from object
representations and default diagnostics, but they are not parameterized authorization boundaries.

## Consistency and parallelism

The default `strict` mode executes exactly one Hive query in one Ray read task and streams multiple
Arrow blocks from that operation. This preserves the single Hive query snapshot, including ACID
and Iceberg semantics supplied by the server. The public facade forces Ray task
`max_retries=0`; a failure after partial output fails the Dataset instead of replaying and appending
a second query attempt.

HS2 exposes one sequential result stream, so initial source parallelism is one. Blocks can be
processed in parallel after they enter Ray. Experimental `independent_queries` requires explicit
typed split predicates and uses multiple independent Hive snapshots; it is not equivalent to
strict mode and rejects a global source limit.

Ray `Dataset.filter()` remains a Ray-side filter in V1. The connector intentionally declines Ray
predicate pushdown because Ray 2.55.1 has no partial-residual contract. Explicit
`HiveReadOptions.filter` predicates are validated as a complete expression and pushed into Hive.

## Security boundaries

- TLS certificate verification is enabled whenever TLS is selected, unless a caller explicitly
  disables it.
- Session configuration is allowlisted.
- Credentials, full SQL, operation secrets, and row values are excluded from default repr and
  diagnostics.
- The client does not accept filesystem paths, Hadoop configuration, HMS endpoints, storage
  credentials, or table locations.
- Client-side query validation does not replace HiveServer2 authorization.

See `docs/security.md`, `docs/consistency.md`, and `docs/compatibility.md` before evaluating the
connector for a production environment.

## Development

```bash
uv sync --extra dev --extra telemetry
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/python -m pytest tests/unit tests/contract --cov=ray_hive
```

SQL, schema, protocol, transport, authentication, lifecycle, cancellation, or distributed Ray
changes must also pass the real `ray-hive-it` infrastructure. Never substitute mocks for that
gate or use broad Docker cleanup commands.

The generated Hive 4.2.0 TCLIService code is reproducible with:

```bash
.venv/bin/python scripts/generate_thrift_stubs.py --build-image --check
```

## License

Apache License 2.0. The generated TCLIService bindings are derived from Apache Hive's Apache-2.0
IDL; exact provenance and checksums are recorded beside the generated package.
