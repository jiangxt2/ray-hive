# Error contract

Every public exception derives from `HiveError` and carries a stable `error_code`, `retryable`,
`partial_output`, and optional normalized `operation_state`. Conformance asserts these properties
before implementation-specific class names.

| Stable code | Public exception | Default retryable | Partial output |
|---|---|---:|---:|
| `CONFIG` | `HiveConfigurationError` | false | false |
| `AUTH` | `HiveAuthenticationError` | false | false |
| `PERMISSION` | `HivePermissionError` | false | false |
| `QUERY_COMPILE` | `HivePlanningError` | false | false |
| `QUERY_EXECUTION` | `HiveQueryError` | false | false |
| `SCHEMA` | `HiveSchemaError` | false | false |
| `PROTOCOL` | `HiveProtocolError` | false | false |
| `TRANSPORT_BEFORE_OUTPUT` | `HiveTransportError` | true | false |
| `TRANSPORT_AFTER_OUTPUT` | `HiveTransportError` | false | true |
| `CANCELLED` | `HiveCancellationError` | false | false |
| `CLEANUP` | `HiveCleanupError` | false | false |

`HiveDependencyError` is a configuration subtype. `HiveCompatibilityError` is a protocol subtype.
Server SQLState, numeric error code, and a bounded diagnostic inform classification. Server text is
normalized and has quoted literals and common secret assignments redacted before it becomes a
public message. Exception repr excludes messages, SQL, credentials, handles, and data.

The table records construction defaults. Once any Arrow block has been published, every subsequent
`HiveError` has `partial_output=true` and is non-retryable while preserving its concrete class and
stable error code. A transport failure additionally changes to `TRANSPORT_AFTER_OUTPUT`.
