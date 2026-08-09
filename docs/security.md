# Security design

The security boundary is HiveServer2 authorization plus a trusted Ray cluster. The connector is a
query client, not a storage client. Its public configuration intentionally has no filesystem,
location, HDFS, object-storage, Iceberg file-I/O, or Hive Metastore options.

`SecretRef` is a serializable reference, not a secret container. Constructing
`HiveConnectionOptions` binds it to endpoint, driver, authentication mode, TLS server name, and
audience context. An authenticated profile resolves it immediately before `OpenSession`; the
NOSASL profile neither resolves nor sends password material. Plaintext material is not copied into
`HiveTaskSpec`. Provider repr and public errors are redacted.

Table schema planning opens an HS2 session on the trusted Ray driver, so an authenticated table
read requires the same provider reference to be resolvable in the driver and worker environments.
Raw SQL with an explicit schema has no driver HS2 operation and can use worker-only injection. Both
nodes are restricted to the approved HS2 endpoint and receive no storage or metastore credentials.

This mechanism prevents accidental credential reuse by ray-hive. It does not protect a secret from
arbitrary code already permitted to execute in the trusted driver or worker environment. Use Ray
job submission controls, workload identities, least-privilege Hive accounts, and isolated
environment injection.

TLS uses an explicit client context. Verification and hostname checks are on by default when TLS
is enabled. Disabling verification is explicit. The connector never silently changes driver,
transport, or authentication mode.

Session settings are allowlisted and translated to Hive's official `set:hiveconf:` OpenSession
configuration. The native client also disables V9 task-serialized `binaryColumns` until that path
has its own decoder and conformance evidence.

Metrics contain low-cardinality driver, transport, protocol, auth, latency, batch, and normalized
error dimensions. Query IDs, operation public IDs, endpoint identity, and query fingerprints may
appear only in sanitized trace context. Full SQL, literals, credential values, operation secrets,
and row data never enter default metrics or logs.
