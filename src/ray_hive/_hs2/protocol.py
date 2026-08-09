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

"""Protocol-neutral HS2 values consumed by planning and execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Protocol

from ray_hive._capabilities import HiveCapabilities
from ray_hive._credentials import HiveCredentials
from ray_hive._models import HiveConnectionOptions


class ProtocolVersion(IntEnum):
    """TCLIService protocol versions used by supported Hive releases."""

    V1 = 0
    V2 = 1
    V3 = 2
    V4 = 3
    V5 = 4
    V6 = 5
    V7 = 6
    V8 = 7
    V9 = 8
    V10 = 9
    V11 = 10


class OperationState(StrEnum):
    """Normalized HS2 operation states."""

    INITIALIZED = "initialized"
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    CANCELLED = "cancelled"
    CLOSED = "closed"
    ERROR = "error"
    TIMED_OUT = "timed_out"
    UNKNOWN = "unknown"

    @property
    def terminal(self) -> bool:
        return self in {
            OperationState.FINISHED,
            OperationState.CANCELLED,
            OperationState.CLOSED,
            OperationState.ERROR,
            OperationState.TIMED_OUT,
        }


class HiveTypeKind(StrEnum):
    """Normalized TTypeId values."""

    BOOLEAN = "BOOLEAN"
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INT = "INT"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    TIMESTAMP = "TIMESTAMP"
    BINARY = "BINARY"
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"
    UNION = "UNIONTYPE"
    USER_DEFINED = "USER_DEFINED"
    DECIMAL = "DECIMAL"
    NULL = "NULL"
    DATE = "DATE"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    INTERVAL_YEAR_MONTH = "INTERVAL_YEAR_MONTH"
    INTERVAL_DAY_TIME = "INTERVAL_DAY_TIME"
    TIMESTAMP_LOCAL_TZ = "TIMESTAMP_WITH_LOCAL_TIME_ZONE"


@dataclass(frozen=True, slots=True)
class HiveType:
    """A fully resolved Hive type descriptor tree."""

    kind: HiveTypeKind
    precision: int | None = None
    scale: int | None = None
    length: int | None = None
    children: tuple[HiveType, ...] = ()
    field_names: tuple[str, ...] = ()
    user_defined_name: str | None = None


@dataclass(frozen=True, slots=True)
class HiveColumnSchema:
    """One result-set column and its complete Hive type descriptor."""

    name: str
    type: HiveType
    position: int
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class HiveResultSchema:
    """Ordered result-set schema returned by GetResultSetMetadata."""

    columns: tuple[HiveColumnSchema, ...]


@dataclass(frozen=True, slots=True)
class SessionHandle:
    """Opaque session token whose secret is excluded from repr."""

    value: object = field(repr=False)


@dataclass(frozen=True, slots=True)
class OperationHandle:
    """Opaque operation token whose secret is excluded from repr."""

    value: object = field(repr=False)
    public_id: str | None = None


@dataclass(frozen=True, slots=True)
class OperationStatus:
    """Normalized state and sanitized failure metadata."""

    state: OperationState
    sql_state: str | None = None
    error_code: int | None = None
    safe_message: str | None = None
    query_id: str | None = None


@dataclass(frozen=True, slots=True)
class HiveColumnVector:
    """One V6 column vector and its packed NULL bitmap."""

    values: tuple[object, ...]
    nulls: bytes


@dataclass(frozen=True, slots=True)
class HiveRowSet:
    """A normalized FetchResults payload."""

    start_row_offset: int
    columns: tuple[HiveColumnVector, ...]
    server_has_more_rows: bool
    binary_columns: bytes | None = field(default=None, repr=False)
    binary_column_count: int | None = None


class AbortHandle(Protocol):
    """Thread-safe socket-only abort boundary for a blocking RPC."""

    def abort(self) -> None:
        """Shut down transport I/O without issuing an HS2 RPC."""


class HiveRPC(Protocol):
    """Serialized HS2 RPC surface owned by exactly one I/O thread."""

    @property
    def abort_handle(self) -> AbortHandle:
        """Return a socket-only abort handle."""

    def open_session(
        self,
        connection: HiveConnectionOptions,
        credentials: HiveCredentials,
    ) -> tuple[SessionHandle, HiveCapabilities]:
        """Open a session and negotiate immutable capabilities."""

    def execute_statement(
        self,
        session: SessionHandle,
        statement: str,
        *,
        timeout_seconds: int,
    ) -> OperationHandle:
        """Submit one asynchronous Hive statement."""

    def get_operation_status(self, operation: OperationHandle) -> OperationStatus:
        """Poll an operation state."""

    def get_result_schema(self, operation: OperationHandle) -> HiveResultSchema:
        """Return complete TTypeDesc-derived result metadata."""

    def fetch_results(self, operation: OperationHandle, *, max_rows: int) -> HiveRowSet:
        """Fetch the next result batch."""

    def cancel_operation(self, operation: OperationHandle) -> None:
        """Request cooperative cancellation."""

    def close_operation(self, operation: OperationHandle) -> None:
        """Release an operation handle."""

    def close_session(self, session: SessionHandle) -> None:
        """Release a session and its remaining operations."""

    def close_transport(self) -> None:
        """Close transport resources."""


class HiveConnectionFactory(Protocol):
    """Importable factory for one task-owned Hive RPC client."""

    def connect(self, connection: HiveConnectionOptions) -> HiveRPC:
        """Create a closed-over, but not yet authenticated, HS2 RPC client."""
