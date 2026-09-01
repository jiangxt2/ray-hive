# Copyright 2026 ray-hive contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Immutable public configuration models."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast

import pyarrow as pa

from ray_hive._credentials import CredentialContext, CredentialProvider, SecretRef
from ray_hive._errors import HiveConfigurationError, HivePlanningError
from ray_hive._sql import (
    Predicate,
    quote_identifier,
    validate_raw_select,
    validate_structured_predicate,
    validate_unsafe_where,
)

_ALLOWED_SESSION_OPTIONS = frozenset(
    {
        "hive.cli.print.header",
        "hive.fetch.task.conversion",
        "hive.local.time.zone",
        "hive.resultset.use.unique.column.names",
        "hive.server2.thrift.resultset.serialize.in.tasks",
    }
)


class HiveDriver(StrEnum):
    """Stable identifiers for explicitly selected HS2 drivers."""

    NATIVE = "native"
    PYHIVE = "pyhive"
    IMPYLA = "impyla"
    JDBC = "jdbc"


class HiveTransport(StrEnum):
    """Supported HiveServer2 transport families."""

    BINARY = "binary"
    HTTP = "http"


class HiveConsistency(StrEnum):
    """Snapshot and query execution policy."""

    STRICT = "strict"
    INDEPENDENT_QUERIES = "independent_queries"


class UnsupportedTypeMode(StrEnum):
    """How verified-but-not-native Hive types are represented."""

    ERROR = "error"
    STRING = "string"


@dataclass(frozen=True, slots=True)
class HiveTableIdentifier:
    """A database-qualified Hive table identifier."""

    database: str
    table: str

    def __post_init__(self) -> None:
        for label, value in (("database", self.database), ("table", self.table)):
            if not isinstance(value, str) or not value or "\x00" in value:
                raise HiveConfigurationError(f"Hive {label} must be non-empty and contain no NUL")
            try:
                quote_identifier(value)
            except HivePlanningError as exc:
                raise HiveConfigurationError(f"Invalid Hive {label}: {exc}") from exc

    @classmethod
    def parse(cls, value: str, *, default_database: str | None = None) -> HiveTableIdentifier:
        """Parse an unambiguous ``database.table`` or qualified shorthand."""

        parts = value.split(".")
        if len(parts) == 2 and all(parts):
            return cls(database=parts[0], table=parts[1])
        if len(parts) == 1 and parts[0] and default_database:
            return cls(database=default_database, table=parts[0])
        raise HiveConfigurationError(
            "Table shorthand must be 'database.table' or have a default database"
        )


@dataclass(frozen=True, slots=True)
class HiveTLSOptions:
    """TLS configuration with certificate verification enabled by default."""

    enabled: bool = False
    verify: bool = True
    ca_file: str | None = None
    server_name: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool) or not isinstance(self.verify, bool):
            raise HiveConfigurationError("TLS enabled and verify options must be booleans")
        for label, value in (("ca_file", self.ca_file), ("server_name", self.server_name)):
            if value is not None and (not isinstance(value, str) or not value or "\x00" in value):
                raise HiveConfigurationError(f"TLS {label} must be a non-empty string")
        if self.ca_file and not self.verify:
            raise HiveConfigurationError(
                "A CA file cannot be used when TLS verification is disabled"
            )
        if self.server_name and not self.enabled:
            raise HiveConfigurationError("TLS server_name requires TLS to be enabled")


@dataclass(frozen=True, slots=True)
class HiveConnectionOptions:
    """Immutable HiveServer2 connection and session configuration."""

    host: str
    port: int = 10000
    database: str = "default"
    driver: HiveDriver | str = HiveDriver.NATIVE
    transport: HiveTransport | str = HiveTransport.BINARY
    auth: str = "NOSASL"
    username: str | None = None
    credentials: CredentialProvider | SecretRef | None = field(default=None, repr=False)
    tls: HiveTLSOptions = field(default_factory=HiveTLSOptions)
    session_options: tuple[tuple[str, str], ...] | Mapping[str, str] = ()
    connect_timeout: float = 10.0
    rpc_timeout: float = 60.0

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host or "\x00" in self.host:
            raise HiveConfigurationError("HiveServer2 host must be non-empty and contain no NUL")
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise HiveConfigurationError("HiveServer2 port must be between 1 and 65535")
        if not isinstance(self.database, str) or not self.database or "\x00" in self.database:
            raise HiveConfigurationError("Hive database must be non-empty and contain no NUL")
        if self.username is not None and (
            not isinstance(self.username, str) or "\x00" in self.username
        ):
            raise HiveConfigurationError("Hive username must be a string containing no NUL")
        if not isinstance(self.tls, HiveTLSOptions):
            raise HiveConfigurationError("tls must be a HiveTLSOptions value")
        timeouts = (self.connect_timeout, self.rpc_timeout)
        if any(
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(value)
            or value <= 0
            for value in timeouts
        ):
            raise HiveConfigurationError("Connection and RPC timeouts must be positive")

        try:
            driver = HiveDriver(self.driver)
            transport = HiveTransport(self.transport)
        except ValueError as exc:
            raise HiveConfigurationError(str(exc)) from exc
        if not isinstance(self.auth, str):
            raise HiveConfigurationError("Hive authentication mode must be a string")
        auth = self.auth.upper()
        if auth not in {"NONE", "NOSASL", "KERBEROS", "LDAP", "CUSTOM"}:
            raise HiveConfigurationError(f"Unsupported Hive authentication mode: {auth!r}")
        if transport is HiveTransport.HTTP and not self.tls.enabled and auth != "NOSASL":
            raise HiveConfigurationError("Authenticated HTTP transport requires TLS")

        raw_options = (
            self.session_options.items()
            if isinstance(self.session_options, Mapping)
            else self.session_options
        )
        options = tuple(sorted((str(key), str(value)) for key, value in raw_options))
        unknown = sorted(key for key, _ in options if key not in _ALLOWED_SESSION_OPTIONS)
        if unknown:
            raise HiveConfigurationError(f"Unsupported Hive session option: {unknown[0]!r}")
        if len({key for key, _ in options}) != len(options):
            raise HiveConfigurationError("Hive session options must not contain duplicate keys")
        option_values = dict(options)
        binary_columns = option_values.get(
            "hive.server2.thrift.resultset.serialize.in.tasks", "false"
        )
        if binary_columns.casefold() != "false":
            raise HiveConfigurationError(
                "The initial profile requires task-serialized binaryColumns to remain disabled"
            )

        object.__setattr__(self, "driver", driver)
        object.__setattr__(self, "transport", transport)
        object.__setattr__(self, "auth", auth)
        object.__setattr__(self, "session_options", options)
        if self.credentials is not None:
            if not isinstance(self.credentials, CredentialProvider):
                raise HiveConfigurationError(
                    "credentials must implement the CredentialProvider contract"
                )
            object.__setattr__(self, "credentials", self.credentials.bind(self.credential_context))

    @property
    def endpoint(self) -> str:
        """Return the normalized endpoint identity without credentials."""

        return f"{self.host}:{self.port}"

    @property
    def credential_context(self) -> CredentialContext:
        """Return the complete binding context for runtime credential resolution."""

        return CredentialContext(
            endpoint=self.endpoint,
            driver=str(self.driver),
            auth=self.auth,
            tls_server_name=self.tls.server_name,
        )

    @property
    def normalized_session_options(self) -> tuple[tuple[str, str], ...]:
        """Return session options after immutable allowlist normalization."""

        return cast(tuple[tuple[str, str], ...], self.session_options)


@dataclass(frozen=True, slots=True)
class HiveReadOptions:
    """Immutable logical read options; exactly one of table or SQL is required."""

    table: HiveTableIdentifier | str | None = None
    sql: str | None = field(default=None, repr=False)
    columns: tuple[str, ...] | list[str] | None = None
    filter: Predicate | None = None
    unsafe_where_sql: str | None = field(default=None, repr=False)
    limit: int | None = None
    fetch_rows: int = 10_000
    target_batch_bytes: int = 64 * 1024 * 1024
    query_timeout: float = 600.0
    consistency: HiveConsistency | str = HiveConsistency.STRICT
    split_predicates: tuple[Predicate, ...] | list[Predicate] = ()
    schema: pa.Schema | None = field(default=None, repr=False)
    unsupported_type_mode: UnsupportedTypeMode | str = UnsupportedTypeMode.ERROR

    def __post_init__(self) -> None:
        if (self.table is None) == (self.sql is None):
            raise HiveConfigurationError("Exactly one of table or sql must be supplied")
        sql = self.sql
        if sql is not None and (not isinstance(sql, str) or not sql.strip()):
            raise HiveConfigurationError("Raw SQL must be non-empty")
        if sql is not None and any(
            value is not None for value in (self.columns, self.filter, self.unsafe_where_sql)
        ):
            raise HiveConfigurationError(
                "columns, filter, and unsafe_where_sql require the table API"
            )
        if sql is not None:
            try:
                sql = validate_raw_select(sql)
            except HivePlanningError as exc:
                raise HiveConfigurationError(str(exc)) from exc
        if self.limit is not None and (
            isinstance(self.limit, bool) or not isinstance(self.limit, int) or self.limit <= 0
        ):
            raise HiveConfigurationError("Hive read limit must be positive")
        if (
            isinstance(self.fetch_rows, bool)
            or not isinstance(self.fetch_rows, int)
            or self.fetch_rows <= 0
            or isinstance(self.target_batch_bytes, bool)
            or not isinstance(self.target_batch_bytes, int)
            or self.target_batch_bytes <= 0
            or isinstance(self.query_timeout, bool)
            or not isinstance(self.query_timeout, int | float)
            or not math.isfinite(self.query_timeout)
            or self.query_timeout <= 0
        ):
            raise HiveConfigurationError(
                "Fetch rows, batch bytes, and query timeout must be positive"
            )

        table = self.table
        if isinstance(table, str):
            table = HiveTableIdentifier.parse(table)
        columns = tuple(self.columns) if self.columns is not None else None
        if columns is not None:
            if not columns:
                raise HiveConfigurationError("Projection columns must not be empty")
            if len(set(columns)) != len(columns):
                raise HiveConfigurationError("Projection columns must be unique")
            if any(
                not isinstance(column, str) or not column or "\x00" in column for column in columns
            ):
                raise HiveConfigurationError(
                    "Projection columns must be non-empty and contain no NUL"
                )
            try:
                for column in columns:
                    quote_identifier(column)
            except HivePlanningError as exc:
                raise HiveConfigurationError(f"Invalid projection column: {exc}") from exc
        if self.filter is not None and not isinstance(self.filter, Predicate):
            raise HiveConfigurationError("filter must be a ray-hive Predicate")
        if self.filter is not None:
            try:
                validate_structured_predicate(self.filter)
            except HivePlanningError as exc:
                raise HiveConfigurationError(str(exc)) from exc
        unsafe_where_sql = self.unsafe_where_sql
        if unsafe_where_sql is not None and (
            not isinstance(unsafe_where_sql, str) or not unsafe_where_sql.strip()
        ):
            raise HiveConfigurationError(
                "unsafe_where_sql must be a non-empty string when supplied"
            )
        if unsafe_where_sql is not None:
            try:
                unsafe_where_sql = validate_unsafe_where(unsafe_where_sql)
            except HivePlanningError as exc:
                raise HiveConfigurationError(str(exc)) from exc
        if self.schema is not None and not isinstance(self.schema, pa.Schema):
            raise HiveConfigurationError("schema must be a PyArrow Schema")

        try:
            consistency = HiveConsistency(self.consistency)
            unsupported_type_mode = UnsupportedTypeMode(self.unsupported_type_mode)
        except ValueError as exc:
            raise HiveConfigurationError(str(exc)) from exc
        split_predicates = tuple(self.split_predicates)
        if any(not isinstance(predicate, Predicate) for predicate in split_predicates):
            raise HiveConfigurationError("split_predicates must contain ray-hive Predicates")
        try:
            for predicate in split_predicates:
                validate_structured_predicate(predicate)
        except HivePlanningError as exc:
            raise HiveConfigurationError(str(exc)) from exc
        if sql is not None:
            if consistency is not HiveConsistency.STRICT or split_predicates:
                raise HiveConfigurationError("Raw SQL supports strict consistency only")
            if self.limit is not None:
                raise HiveConfigurationError("Raw SQL must express its own limit")
        elif self.schema is not None:
            raise HiveConfigurationError("Explicit schema is supported only for raw SQL")
        if consistency is HiveConsistency.STRICT and split_predicates:
            raise HiveConfigurationError("split_predicates require independent_queries consistency")
        if consistency is HiveConsistency.INDEPENDENT_QUERIES and not split_predicates:
            raise HiveConfigurationError("independent_queries requires explicit split_predicates")
        if consistency is HiveConsistency.INDEPENDENT_QUERIES and self.limit is not None:
            raise HiveConfigurationError("A global Hive limit is unsafe with independent queries")

        object.__setattr__(self, "table", table)
        object.__setattr__(self, "sql", sql)
        object.__setattr__(self, "columns", columns)
        object.__setattr__(self, "unsafe_where_sql", unsafe_where_sql)
        object.__setattr__(self, "consistency", consistency)
        object.__setattr__(self, "split_predicates", split_predicates)
        object.__setattr__(self, "unsupported_type_mode", unsupported_type_mode)

    @property
    def normalized_consistency(self) -> HiveConsistency:
        """Return the validated consistency enum."""

        return cast(HiveConsistency, self.consistency)

    @property
    def normalized_unsupported_type_mode(self) -> UnsupportedTypeMode:
        """Return the validated unsupported-type policy."""

        return cast(UnsupportedTypeMode, self.unsupported_type_mode)

    def __repr__(self) -> str:
        source = f"table={self.table!r}" if self.table else "sql=<redacted>"
        consistency = self.normalized_consistency
        filter_summary = f"<{type(self.filter).__name__}>" if self.filter else None
        return (
            f"HiveReadOptions({source}, columns={self.columns!r}, filter={filter_summary!r}, "
            f"unsafe_where_sql={'<redacted>' if self.unsafe_where_sql else None!r}, "
            f"limit={self.limit!r}, consistency={consistency.value!r})"
        )
