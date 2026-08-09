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

"""Native Apache Hive TCLIService adapter."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from ray_hive._capabilities import CapabilityEvidence, CapabilityState, HiveCapabilities
from ray_hive._credentials import HiveCredentials
from ray_hive._errors import (
    HiveAuthenticationError,
    HiveCompatibilityError,
    HiveError,
    HivePermissionError,
    HivePlanningError,
    HiveProtocolError,
    HiveQueryError,
    HiveTransportError,
)
from ray_hive._hs2.protocol import (
    AbortHandle,
    HiveColumnSchema,
    HiveColumnVector,
    HiveConnectionFactory,
    HiveResultSchema,
    HiveRowSet,
    HiveRPC,
    HiveType,
    HiveTypeKind,
    OperationHandle,
    OperationState,
    OperationStatus,
    SessionHandle,
)
from ray_hive._hs2.transport import NativeTransport, open_native_transport
from ray_hive._models import HiveConnectionOptions, HiveDriver

TCLIService: Any = import_module("ray_hive._hs2.generated.TCLIService.TCLIService")
constants: Any = import_module("ray_hive._hs2.generated.TCLIService.constants")
ttypes: Any = import_module("ray_hive._hs2.generated.TCLIService.ttypes")

_SUCCESS_CODES = {
    ttypes.TStatusCode.SUCCESS_STATUS,
    ttypes.TStatusCode.SUCCESS_WITH_INFO_STATUS,
}
_OPERATION_STATES = {
    ttypes.TOperationState.INITIALIZED_STATE: OperationState.INITIALIZED,
    ttypes.TOperationState.PENDING_STATE: OperationState.PENDING,
    ttypes.TOperationState.RUNNING_STATE: OperationState.RUNNING,
    ttypes.TOperationState.FINISHED_STATE: OperationState.FINISHED,
    ttypes.TOperationState.CANCELED_STATE: OperationState.CANCELLED,
    ttypes.TOperationState.CLOSED_STATE: OperationState.CLOSED,
    ttypes.TOperationState.ERROR_STATE: OperationState.ERROR,
    ttypes.TOperationState.TIMEDOUT_STATE: OperationState.TIMED_OUT,
    ttypes.TOperationState.UKNOWN_STATE: OperationState.UNKNOWN,
}
_TYPE_KINDS = {
    ttypes.TTypeId.BOOLEAN_TYPE: HiveTypeKind.BOOLEAN,
    ttypes.TTypeId.TINYINT_TYPE: HiveTypeKind.TINYINT,
    ttypes.TTypeId.SMALLINT_TYPE: HiveTypeKind.SMALLINT,
    ttypes.TTypeId.INT_TYPE: HiveTypeKind.INT,
    ttypes.TTypeId.BIGINT_TYPE: HiveTypeKind.BIGINT,
    ttypes.TTypeId.FLOAT_TYPE: HiveTypeKind.FLOAT,
    ttypes.TTypeId.DOUBLE_TYPE: HiveTypeKind.DOUBLE,
    ttypes.TTypeId.STRING_TYPE: HiveTypeKind.STRING,
    ttypes.TTypeId.TIMESTAMP_TYPE: HiveTypeKind.TIMESTAMP,
    ttypes.TTypeId.BINARY_TYPE: HiveTypeKind.BINARY,
    ttypes.TTypeId.ARRAY_TYPE: HiveTypeKind.ARRAY,
    ttypes.TTypeId.MAP_TYPE: HiveTypeKind.MAP,
    ttypes.TTypeId.STRUCT_TYPE: HiveTypeKind.STRUCT,
    ttypes.TTypeId.UNION_TYPE: HiveTypeKind.UNION,
    ttypes.TTypeId.USER_DEFINED_TYPE: HiveTypeKind.USER_DEFINED,
    ttypes.TTypeId.DECIMAL_TYPE: HiveTypeKind.DECIMAL,
    ttypes.TTypeId.NULL_TYPE: HiveTypeKind.NULL,
    ttypes.TTypeId.DATE_TYPE: HiveTypeKind.DATE,
    ttypes.TTypeId.VARCHAR_TYPE: HiveTypeKind.VARCHAR,
    ttypes.TTypeId.CHAR_TYPE: HiveTypeKind.CHAR,
    ttypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE: HiveTypeKind.INTERVAL_YEAR_MONTH,
    ttypes.TTypeId.INTERVAL_DAY_TIME_TYPE: HiveTypeKind.INTERVAL_DAY_TIME,
    ttypes.TTypeId.TIMESTAMPLOCALTZ_TYPE: HiveTypeKind.TIMESTAMP_LOCAL_TZ,
}
_SERVER_MESSAGE_LIMIT = 512
_QUOTED_LITERAL = re.compile(r"'(?:''|[^'])*'")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(password|passwd|token|secret|access[_-]?key)\s*=\s*([^\s,;]+)"
)
_QUERY_ID = re.compile(r"(?i)\bquery[\s_-]*id\b\s*[:=]\s*[a-z0-9_-]+")


def _safe_server_message(message: object) -> str | None:
    """Return a bounded diagnostic with literals and common secret values redacted."""

    if not isinstance(message, str):
        return None
    normalized = " ".join(message.split())
    if not normalized:
        return None
    normalized = _QUOTED_LITERAL.sub("'<redacted>'", normalized)
    normalized = _SECRET_ASSIGNMENT.sub(r"\1=<redacted>", normalized)
    normalized = _QUERY_ID.sub("Query ID: <redacted>", normalized)
    if len(normalized) > _SERVER_MESSAGE_LIMIT:
        normalized = normalized[: _SERVER_MESSAGE_LIMIT - 3] + "..."
    return normalized


def _require_success(status: Any, *, action: str) -> None:
    if status is None:
        raise HiveProtocolError(f"Hive {action} response omitted status")
    if status.statusCode in _SUCCESS_CODES:
        return
    sql_state = getattr(status, "sqlState", None)
    safe_message = _safe_server_message(getattr(status, "errorMessage", None))
    detail = f": {safe_message}" if safe_message else ""
    if sql_state and sql_state.startswith("28"):
        raise HiveAuthenticationError(f"Hive {action} authentication failed{detail}")
    if sql_state == "42501":
        raise HivePermissionError(f"Hive {action} was denied{detail}")
    if sql_state and sql_state.startswith("42"):
        raise HivePlanningError(f"Hive {action} could not be planned{detail}")
    raise HiveQueryError(f"Hive {action} failed{detail}")


def _public_operation_id(handle: Any) -> str | None:
    try:
        guid = bytes(handle.operationId.guid)
        return str(uuid.UUID(bytes=guid)) if len(guid) == 16 else guid.hex()
    except (AttributeError, TypeError, ValueError):
        return None


def _qualifier(entry: Any, name: str) -> int | None:
    qualifiers = getattr(entry, "typeQualifiers", None)
    values = getattr(qualifiers, "qualifiers", None) if qualifiers is not None else None
    value = values.get(name) if values else None
    return getattr(value, "i32Value", None) if value is not None else None


def _parse_type_descriptor(descriptor: Any) -> HiveType:
    entries = descriptor.types
    visiting: set[int] = set()

    def parse(pointer: int) -> HiveType:
        if pointer < 0 or pointer >= len(entries):
            raise HiveProtocolError("Hive TTypeDesc contains an invalid type pointer")
        if pointer in visiting:
            raise HiveProtocolError("Hive TTypeDesc contains a recursive type pointer")
        visiting.add(pointer)
        try:
            entry = entries[pointer]
            primitive = entry.primitiveEntry
            if primitive is not None:
                try:
                    kind = _TYPE_KINDS[primitive.type]
                except KeyError as exc:
                    raise HiveCompatibilityError("Hive returned an unknown primitive type") from exc
                return HiveType(
                    kind=kind,
                    precision=_qualifier(primitive, constants.PRECISION),
                    scale=_qualifier(primitive, constants.SCALE),
                    length=_qualifier(primitive, constants.CHARACTER_MAXIMUM_LENGTH),
                )
            if entry.arrayEntry is not None:
                return HiveType(
                    HiveTypeKind.ARRAY,
                    children=(parse(entry.arrayEntry.objectTypePtr),),
                )
            if entry.mapEntry is not None:
                return HiveType(
                    HiveTypeKind.MAP,
                    children=(
                        parse(entry.mapEntry.keyTypePtr),
                        parse(entry.mapEntry.valueTypePtr),
                    ),
                )
            if entry.structEntry is not None:
                fields = tuple(entry.structEntry.nameToTypePtr.items())
                return HiveType(
                    HiveTypeKind.STRUCT,
                    children=tuple(parse(child) for _, child in fields),
                    field_names=tuple(name for name, _ in fields),
                )
            if entry.unionEntry is not None:
                fields = tuple(entry.unionEntry.nameToTypePtr.items())
                return HiveType(
                    HiveTypeKind.UNION,
                    children=tuple(parse(child) for _, child in fields),
                    field_names=tuple(name for name, _ in fields),
                )
            if entry.userDefinedTypeEntry is not None:
                return HiveType(
                    HiveTypeKind.USER_DEFINED,
                    user_defined_name=entry.userDefinedTypeEntry.typeClassName,
                )
            raise HiveProtocolError("Hive TTypeDesc entry has no recognized type arm")
        finally:
            visiting.remove(pointer)

    if not entries:
        raise HiveProtocolError("Hive TTypeDesc is empty")
    return parse(0)


def _normalize_schema(schema: Any) -> HiveResultSchema:
    if schema is None or schema.columns is None:
        raise HiveProtocolError("Hive metadata response omitted the table schema")
    return HiveResultSchema(
        tuple(
            HiveColumnSchema(
                name=column.columnName,
                type=_parse_type_descriptor(column.typeDesc),
                position=column.position,
                comment=column.comment,
            )
            for column in schema.columns
        )
    )


def _normalize_vector(column: Any) -> HiveColumnVector:
    arms = (
        column.boolVal,
        column.byteVal,
        column.i16Val,
        column.i32Val,
        column.i64Val,
        column.doubleVal,
        column.stringVal,
        column.binaryVal,
    )
    populated = [arm for arm in arms if arm is not None]
    if len(populated) != 1:
        raise HiveProtocolError("Hive TColumn must contain exactly one vector arm")
    arm = populated[0]
    return HiveColumnVector(tuple(arm.values), bytes(arm.nulls))


@dataclass(slots=True)
class NativeHiveRPC(HiveRPC):
    """One native TCLIService client whose methods are called serially."""

    native_transport: NativeTransport
    client: Any

    @classmethod
    def connect(cls, connection: HiveConnectionOptions) -> NativeHiveRPC:
        native_transport = open_native_transport(connection)
        return cls(native_transport, TCLIService.Client(native_transport.protocol))

    @property
    def abort_handle(self) -> AbortHandle:
        return self.native_transport.abort_handle

    def _call(self, method_name: str, request: Any) -> Any:
        try:
            method = getattr(self.client, method_name)
            return method(request)
        except HiveError:
            raise
        except Exception as exc:
            raise HiveTransportError(f"Hive {method_name} transport failed") from exc

    def open_session(
        self,
        connection: HiveConnectionOptions,
        credentials: HiveCredentials,
    ) -> tuple[SessionHandle, HiveCapabilities]:
        configuration = {
            "use:database": connection.database,
            "set:hiveconf:hive.server2.thrift.resultset.serialize.in.tasks": "false",
        }
        configuration.update(
            {f"set:hiveconf:{key}": value for key, value in connection.normalized_session_options}
        )
        response = self._call(
            "OpenSession",
            ttypes.TOpenSessionReq(
                client_protocol=ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
                username=credentials.username,
                password=credentials.password if connection.auth != "NOSASL" else None,
                configuration=configuration,
            ),
        )
        _require_success(response.status, action="OpenSession")
        if response.sessionHandle is None:
            raise HiveProtocolError("Hive OpenSession response omitted its handle")
        version = response.serverProtocolVersion
        if version is None:
            raise HiveProtocolError("Hive OpenSession response omitted protocol version")
        columnar = (
            CapabilityState.SUPPORTED
            if version >= ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6
            else CapabilityState.UNSUPPORTED
        )
        binary = CapabilityState.UNSUPPORTED
        capabilities = HiveCapabilities(
            protocol_version=version + 1,
            columnar_rowsets=columnar,
            binary_columns=binary,
            cancel_operation=CapabilityState.SUPPORTED,
            operation_log=CapabilityState.UNKNOWN,
            evidence=(
                CapabilityEvidence("columnar_rowsets", columnar, "OpenSession protocol"),
                CapabilityEvidence("binary_columns", binary, "explicit session configuration"),
                CapabilityEvidence(
                    "cancel_operation",
                    CapabilityState.SUPPORTED,
                    "TCLIService method",
                ),
            ),
        )
        return SessionHandle(response.sessionHandle), capabilities

    def execute_statement(
        self,
        session: SessionHandle,
        statement: str,
        *,
        timeout_seconds: int,
    ) -> OperationHandle:
        response = self._call(
            "ExecuteStatement",
            ttypes.TExecuteStatementReq(
                sessionHandle=session.value,
                statement=statement,
                runAsync=True,
                queryTimeout=timeout_seconds,
            ),
        )
        _require_success(response.status, action="ExecuteStatement")
        if response.operationHandle is None or not response.operationHandle.hasResultSet:
            raise HiveProtocolError("Hive query did not return a result-set operation")
        return OperationHandle(
            response.operationHandle,
            public_id=_public_operation_id(response.operationHandle),
        )

    def get_operation_status(self, operation: OperationHandle) -> OperationStatus:
        response = self._call(
            "GetOperationStatus",
            ttypes.TGetOperationStatusReq(operationHandle=operation.value),
        )
        _require_success(response.status, action="GetOperationStatus")
        state = _OPERATION_STATES.get(response.operationState, OperationState.UNKNOWN)
        return OperationStatus(
            state=state,
            sql_state=response.sqlState,
            error_code=response.errorCode,
            safe_message=_safe_server_message(getattr(response, "errorMessage", None)),
        )

    def get_result_schema(self, operation: OperationHandle) -> HiveResultSchema:
        response = self._call(
            "GetResultSetMetadata",
            ttypes.TGetResultSetMetadataReq(operationHandle=operation.value),
        )
        _require_success(response.status, action="GetResultSetMetadata")
        return _normalize_schema(response.schema)

    def fetch_results(self, operation: OperationHandle, *, max_rows: int) -> HiveRowSet:
        response = self._call(
            "FetchResults",
            ttypes.TFetchResultsReq(
                operationHandle=operation.value,
                orientation=ttypes.TFetchOrientation.FETCH_NEXT,
                maxRows=max_rows,
                fetchType=0,
            ),
        )
        _require_success(response.status, action="FetchResults")
        rowset = response.results
        if rowset is None:
            raise HiveProtocolError("Hive FetchResults response omitted its rowset")
        columns = tuple(_normalize_vector(column) for column in (rowset.columns or ()))
        start_row_offset = rowset.startRowOffset
        if (
            isinstance(start_row_offset, bool)
            or not isinstance(start_row_offset, int)
            or start_row_offset < 0
        ):
            raise HiveProtocolError("Hive FetchResults returned an invalid start row offset")
        # Hive 4.2 SQLOperation creates a fresh ColumnBasedSet for every fetch,
        # so this advisory field resets to zero instead of tracking cursor progress.
        return HiveRowSet(
            start_row_offset=start_row_offset,
            columns=columns,
            server_has_more_rows=bool(getattr(response, "hasMoreRows", False)),
            binary_columns=(
                bytes(rowset.binaryColumns) if rowset.binaryColumns is not None else None
            ),
            binary_column_count=rowset.columnCount,
        )

    def cancel_operation(self, operation: OperationHandle) -> None:
        response = self._call(
            "CancelOperation",
            ttypes.TCancelOperationReq(operationHandle=operation.value),
        )
        _require_success(response.status, action="CancelOperation")

    def close_operation(self, operation: OperationHandle) -> None:
        response = self._call(
            "CloseOperation",
            ttypes.TCloseOperationReq(operationHandle=operation.value),
        )
        _require_success(response.status, action="CloseOperation")

    def close_session(self, session: SessionHandle) -> None:
        response = self._call("CloseSession", ttypes.TCloseSessionReq(sessionHandle=session.value))
        _require_success(response.status, action="CloseSession")

    def close_transport(self) -> None:
        self.native_transport.close()


@dataclass(frozen=True, slots=True)
class NativeConnectionFactory(HiveConnectionFactory):
    """Serializable factory for task-owned native clients."""

    def connect(self, connection: HiveConnectionOptions) -> HiveRPC:
        return NativeHiveRPC.connect(connection)


def resolve_connection_factory(driver: str) -> HiveConnectionFactory:
    """Resolve one explicitly selected driver without cross-driver fallback."""

    if driver == HiveDriver.NATIVE.value:
        return NativeConnectionFactory()
    raise HiveCompatibilityError(
        f"Driver {driver!r} is not installed in the initial native-only profile"
    )
