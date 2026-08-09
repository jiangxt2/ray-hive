# Conformance

The repository vendors `spec/hs2-arrow/v1` so builds and tests do not depend on another project or
the network. Stable case IDs describe protocol input, Arrow output, error properties, and resource
postconditions. A published case ID never changes meaning; incompatible changes create a new major
spec version.

`scripts/check_spec.py` validates JSON schemas, IDL provenance, required capability and error IDs,
case uniqueness, and fixture checksums. Generated binding reproduction is a separate byte-for-byte
check.

Unit and generated-Thrift contract tests currently exercise the protocol lifecycle, V6 vectors,
NULL bitmap, V9 fail-closed detection, strict task retry configuration, and cleanup ordering. Those
tests do not substitute for real Hive tables, authentication, authorization, ACID, Iceberg,
network-purity, or Ray failure injection.

Production Core status requires the full Hive 4.2.0 case matrix, security and network evidence, and
Ray cluster cases to pass for the exact release commit. Unsupported cases are reported as
unsupported; results from another connector or adjacent Hive version are never inherited.
