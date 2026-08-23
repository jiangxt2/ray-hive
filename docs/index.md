# ray-hive

`ray-hive` is an independent, HS2-only datasource for Ray Data. HiveServer2 remains responsible
for storage access, table formats, authorization, query execution, and snapshot semantics. The
connector turns one sequential HS2 result stream into multiple Arrow blocks without exposing Hive
Metastore, HDFS, object-store, manifest, or file-location configuration.

The package is Alpha and supports final Ray releases `>=2.55.1,<2.57` with PyArrow 19.0.1 and
Apache Hive 4.2.0. Only native binary + NOSASL is currently implemented. See the compatibility and conformance
pages before treating a tested case as a broader support claim.

Start with the project [README](https://github.com/jiangxt2/ray-hive#readme), then review the
[consistency](consistency.md) and [security](security.md) contracts.
