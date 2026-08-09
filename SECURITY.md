# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use the repository's private GitHub
security advisory interface. Include affected versions, impact, a minimal reproduction, and any
known mitigation. Do not include production credentials, complete connection URLs, SQL literals,
operation handles, or row data.

## Trust boundary

`ray-hive` resolves secret references on Ray workers. Use it only on a trusted Ray cluster with
authenticated control-plane access, restricted job submission, and worker-level secret isolation.
An endpoint-bound reference prevents accidental reuse by this connector; it cannot sandbox
arbitrary code already authorized to run on that worker.

Workers need access to HiveServer2 and the Ray cluster only. Do not give them HDFS, Hive Metastore,
S3, MinIO, or other storage credentials for this connector. HiveServer2 authorization remains the
security boundary for table and row access.

## Transport security

TLS verification is enabled by default when TLS is selected. Configure a trusted CA and expected
server name. Disabling verification is an explicit insecure choice and must not be used for
production traffic. The current Alpha native profile supports binary + NOSASL only; use it solely
inside an appropriately protected network until authenticated profiles pass their real security
matrix.

HTTP redirects across hosts, ZooKeeper service discovery, automatic driver fallback, and silent
transport downgrade are not supported.

## Sensitive diagnostics

Default repr, exceptions, metrics, and traces exclude credential values, full SQL, SQL literals,
operation secrets, and row data. Query IDs and fingerprints are trace context, never metric labels.
Callers remain responsible for not logging their own raw SQL or resolved credentials.
