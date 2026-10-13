# ray-hive

`ray-hive` is an independent, HS2-only datasource for Ray Data. HiveServer2 remains responsible
for storage access, table formats, authorization, query execution, and snapshot semantics. The
connector turns one sequential HS2 result stream into multiple Arrow blocks without exposing Hive
Metastore, HDFS, object-store, manifest, or file-location configuration.

The package is Alpha and supports final Ray releases `>=2.55.1,<2.60` with PyArrow 19.0.1 and
Apache Hive 4.2.0. Only native binary + NOSASL is currently implemented. See the compatibility and conformance
pages before treating a tested case as a broader support claim.

The stable public surface includes top-level `read_hive()`, its configuration and error types, and
the built-in `ray_hive.sql` predicate builder. Builder classes are not subclassing extension
points. The datasource module is a DeveloperAPI without the facade's strict retry guarantee. Raw
SQL is limited to one conservatively validated top-level `SELECT`, while table reads always render
`SELECT` statements.

Start with the project [README](https://github.com/jiangxt2/ray-hive#readme), then review the
[consistency](consistency.md) and [security](security.md) contracts.
The repository [support policy](https://github.com/jiangxt2/ray-hive/blob/master/SUPPORT.md) defines
the version evidence, deprecation rules, maintenance ownership, and community support boundary.
