# Changelog

All notable changes to this project are documented in this file.

## 1.2

- See the [ray-hive 1.2 release notes](release-notes/v1.2.md).

## 1.1

- See the [ray-hive 1.1 release notes](release-notes/v1.1.md).

## 1.0

- Establish the independent Apache-2.0 project and narrow compatibility baseline.
- Add immutable public configuration, endpoint-bound credential references, safe predicates, and
  stable error contracts.
- Add reproducible Apache Hive 4.2.0 TCLIService bindings, native V6 columnar decoding, schema
  planning, schema drift detection, and deterministic resource cleanup.
- Add strict Ray Data execution with one query, multiple blocks, a task-owned I/O thread, bounded
  cancellation, and `max_retries=0`.
