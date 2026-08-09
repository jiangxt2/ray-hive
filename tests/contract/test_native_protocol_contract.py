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

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

import pytest

from ray_hive import HiveConnectionOptions, HiveTransportError
from ray_hive._credentials import HiveCredentials
from ray_hive._hs2.client import NativeHiveRPC
from ray_hive._hs2.generated.TCLIService import ttypes
from ray_hive._hs2.protocol import OperationState
from tests._fakes import FakeAbortHandle

pytestmark = pytest.mark.contract


def _status() -> Any:
    return ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS)


def _identifier(byte: int) -> Any:
    return ttypes.THandleIdentifier(guid=bytes([byte]) * 16, secret=b"secret" * 3)


@dataclass(slots=True)
class _Transport:
    abort_handle: FakeAbortHandle = field(default_factory=FakeAbortHandle)
    closed: bool = False

    def close(self) -> None:
        self.closed = True


@dataclass(slots=True)
class _GeneratedClient:
    events: list[str] = field(default_factory=list)
    open_request: Any = None
    execute_request: Any = None
    fetches: deque[Any] = field(default_factory=deque)

    def OpenSession(self, request: Any) -> Any:
        self.events.append("OpenSession")
        self.open_request = request
        return ttypes.TOpenSessionResp(
            status=_status(),
            serverProtocolVersion=ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
            sessionHandle=ttypes.TSessionHandle(sessionId=_identifier(1)),
        )

    def ExecuteStatement(self, request: Any) -> Any:
        self.events.append("ExecuteStatement")
        self.execute_request = request
        return ttypes.TExecuteStatementResp(
            status=_status(),
            operationHandle=ttypes.TOperationHandle(
                operationId=_identifier(2),
                operationType=ttypes.TOperationType.EXECUTE_STATEMENT,
                hasResultSet=True,
            ),
        )

    def GetOperationStatus(self, request: Any) -> Any:
        self.events.append("GetOperationStatus")
        return ttypes.TGetOperationStatusResp(
            status=_status(), operationState=ttypes.TOperationState.FINISHED_STATE
        )

    def GetResultSetMetadata(self, request: Any) -> Any:
        self.events.append("GetResultSetMetadata")
        descriptor = ttypes.TTypeDesc(
            types=[
                ttypes.TTypeEntry(
                    primitiveEntry=ttypes.TPrimitiveTypeEntry(type=ttypes.TTypeId.INT_TYPE)
                )
            ]
        )
        return ttypes.TGetResultSetMetadataResp(
            status=_status(),
            schema=ttypes.TTableSchema(
                columns=[ttypes.TColumnDesc("id", descriptor, 1, "primary key")]
            ),
        )

    def FetchResults(self, request: Any) -> Any:
        self.events.append("FetchResults")
        return self.fetches.popleft()

    def CancelOperation(self, request: Any) -> Any:
        self.events.append("CancelOperation")
        return ttypes.TCancelOperationResp(status=_status())

    def CloseOperation(self, request: Any) -> Any:
        self.events.append("CloseOperation")
        return ttypes.TCloseOperationResp(status=_status())

    def CloseSession(self, request: Any) -> Any:
        self.events.append("CloseSession")
        return ttypes.TCloseSessionResp(status=_status())


def _fetch(values: list[int], nulls: bytes, *, offset: int, more: bool) -> Any:
    return ttypes.TFetchResultsResp(
        status=_status(),
        hasMoreRows=more,
        results=ttypes.TRowSet(
            startRowOffset=offset,
            rows=[],
            columns=[ttypes.TColumn(i32Val=ttypes.TI32Column(values=values, nulls=nulls))],
        ),
    )


def test_hs2_proto_001_and_002_generated_lifecycle_and_multifetch() -> None:
    transport = _Transport()
    client = _GeneratedClient(
        fetches=deque(
            [
                _fetch([1, 0], b"\x02", offset=0, more=True),
                _fetch([3], b"\x00", offset=0, more=False),
                _fetch([], b"", offset=0, more=False),
            ]
        )
    )
    rpc = NativeHiveRPC(transport, client)
    connection = HiveConnectionOptions(
        host="hs2.example",
        database="analytics",
        session_options={"hive.local.time.zone": "UTC"},
    )

    session, capabilities = rpc.open_session(
        connection, HiveCredentials(username="reader", password="private-password")
    )
    operation = rpc.execute_statement(
        session, "SELECT id FROM analytics.events", timeout_seconds=30
    )
    status = rpc.get_operation_status(operation)
    schema = rpc.get_result_schema(operation)
    first = rpc.fetch_results(operation, max_rows=2)
    second = rpc.fetch_results(operation, max_rows=2)
    exhausted = rpc.fetch_results(operation, max_rows=2)
    rpc.close_operation(operation)
    rpc.close_session(session)
    rpc.close_transport()

    assert capabilities.protocol_version == 11
    capabilities.require_core()
    assert client.open_request.configuration == {
        "use:database": "analytics",
        "set:hiveconf:hive.server2.thrift.resultset.serialize.in.tasks": "false",
        "set:hiveconf:hive.local.time.zone": "UTC",
    }
    assert client.open_request.password is None
    assert client.execute_request.runAsync is True
    assert client.execute_request.queryTimeout == 30
    assert status.state is OperationState.FINISHED
    assert schema.columns[0].name == "id"
    assert schema.columns[0].position == 1
    assert first.columns[0].values == (1, 0)
    assert first.columns[0].nulls == b"\x02"
    assert first.server_has_more_rows is True
    assert first.start_row_offset == 0
    assert second.start_row_offset == 0
    assert exhausted.start_row_offset == 0
    assert second.server_has_more_rows is False
    assert exhausted.server_has_more_rows is False
    assert transport.closed is True
    assert client.events == [
        "OpenSession",
        "ExecuteStatement",
        "GetOperationStatus",
        "GetResultSetMetadata",
        "FetchResults",
        "FetchResults",
        "FetchResults",
        "CloseOperation",
        "CloseSession",
    ]
    assert "private-password" not in repr(session)
    assert "secret" not in repr(operation)


def test_hs2_proto_004_cancel_and_close_are_distinct_calls() -> None:
    transport = _Transport()
    client = _GeneratedClient()
    rpc = NativeHiveRPC(transport, client)
    connection = HiveConnectionOptions(host="hs2.example")
    session, _ = rpc.open_session(connection, HiveCredentials())
    operation = rpc.execute_statement(session, "SELECT 1", timeout_seconds=5)

    rpc.cancel_operation(operation)
    rpc.close_operation(operation)
    rpc.close_session(session)

    assert client.events[-3:] == ["CancelOperation", "CloseOperation", "CloseSession"]


def test_v9_binary_columns_are_not_misclassified_as_eof() -> None:
    transport = _Transport()
    client = _GeneratedClient(
        fetches=deque(
            [
                ttypes.TFetchResultsResp(
                    status=_status(),
                    hasMoreRows=False,
                    results=ttypes.TRowSet(
                        startRowOffset=0,
                        rows=[],
                        columns=None,
                        binaryColumns=b"serialized-columns",
                        columnCount=1,
                    ),
                )
            ]
        )
    )
    rpc = NativeHiveRPC(transport, client)
    operation = type("Operation", (), {"value": object()})()

    rowset = rpc.fetch_results(operation, max_rows=100)

    assert rowset.columns == ()
    assert rowset.binary_columns == b"serialized-columns"
    assert rowset.binary_column_count == 1


def test_generated_client_exception_is_normalized_as_transport_failure() -> None:
    transport = _Transport()
    client = _GeneratedClient()
    rpc = NativeHiveRPC(transport, client)
    operation = type("Operation", (), {"value": object()})()

    with pytest.raises(HiveTransportError, match="FetchResults transport failed"):
        rpc.fetch_results(operation, max_rows=1)
