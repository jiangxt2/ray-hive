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

import socket
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from ray_hive import (
    HiveAuthenticationError,
    HiveCompatibilityError,
    HiveConnectionOptions,
    HivePermissionError,
    HivePlanningError,
    HiveProtocolError,
    HiveQueryError,
    HiveTLSOptions,
    HiveTransportError,
)
from ray_hive._credentials import HiveCredentials
from ray_hive._hs2 import client as client_module
from ray_hive._hs2 import transport as transport_module
from ray_hive._hs2.client import NativeHiveRPC
from ray_hive._hs2.generated.TCLIService import ttypes
from ray_hive._hs2.protocol import HiveTypeKind, OperationHandle, OperationState, SessionHandle


def _status(
    code: int = ttypes.TStatusCode.SUCCESS_STATUS,
    sql_state: str | None = None,
    error_message: str | None = None,
) -> Any:
    return SimpleNamespace(
        statusCode=code,
        sqlState=sql_state,
        errorMessage=error_message,
    )


def _entry(**selected: object) -> Any:
    arms = {
        "primitiveEntry": None,
        "arrayEntry": None,
        "mapEntry": None,
        "structEntry": None,
        "unionEntry": None,
        "userDefinedTypeEntry": None,
    }
    arms.update(selected)
    return SimpleNamespace(**arms)


def _primitive(type_id: int) -> Any:
    return _entry(primitiveEntry=SimpleNamespace(type=type_id, typeQualifiers=None))


class _Transport:
    def __init__(self) -> None:
        self.abort_handle = SimpleNamespace(abort=lambda: None)
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Client:
    def __init__(self, **responses: object) -> None:
        self.responses = responses

    def __getattr__(self, name: str) -> Any:
        def call(request: object) -> object:
            del request
            response = self.responses[name]
            if isinstance(response, Exception):
                raise response
            return response

        return call


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (None, HiveProtocolError),
        (_status(ttypes.TStatusCode.ERROR_STATUS, "28000"), HiveAuthenticationError),
        (_status(ttypes.TStatusCode.ERROR_STATUS, "42501"), HivePermissionError),
        (_status(ttypes.TStatusCode.ERROR_STATUS, "42S02"), HivePlanningError),
        (_status(ttypes.TStatusCode.ERROR_STATUS, "HY000"), HiveQueryError),
    ],
)
def test_status_failures_have_stable_sanitized_classes(
    status: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        client_module._require_success(status, action="test")


@pytest.mark.parametrize(
    "query_id",
    [
        "Query ID: hive_20260809_private-id",
        "Query ID = hive_20260809_private-id",
        "queryId=hive_20260809_private-id",
        "query_id=hive_20260809_private-id",
    ],
)
def test_status_failure_preserves_bounded_redacted_server_diagnostic(query_id: str) -> None:
    status = _status(
        ttypes.TStatusCode.ERROR_STATUS,
        "42S02",
        f"Table not found for value 'private-literal' password=top-secret; {query_id}",
    )

    with pytest.raises(HivePlanningError) as caught:
        client_module._require_success(status, action="ExecuteStatement")

    message = str(caught.value)
    assert "Table not found" in message
    assert "private-literal" not in message
    assert "top-secret" not in message
    assert "private-id" not in message


def test_type_descriptor_resolves_nested_arms_and_rejects_bad_pointers() -> None:
    descriptor = SimpleNamespace(
        types=[
            _entry(
                structEntry=SimpleNamespace(
                    nameToTypePtr={"items": 1, "attributes": 3, "choice": 6, "custom": 7}
                )
            ),
            _entry(arrayEntry=SimpleNamespace(objectTypePtr=2)),
            _primitive(ttypes.TTypeId.INT_TYPE),
            _entry(mapEntry=SimpleNamespace(keyTypePtr=4, valueTypePtr=5)),
            _primitive(ttypes.TTypeId.STRING_TYPE),
            _primitive(ttypes.TTypeId.BIGINT_TYPE),
            _entry(unionEntry=SimpleNamespace(nameToTypePtr={"number": 2, "text": 4})),
            _entry(userDefinedTypeEntry=SimpleNamespace(typeClassName="example.CustomType")),
        ]
    )

    result = client_module._parse_type_descriptor(descriptor)

    assert result.kind is HiveTypeKind.STRUCT
    assert result.field_names == ("items", "attributes", "choice", "custom")
    assert result.children[0].kind is HiveTypeKind.ARRAY
    assert result.children[1].kind is HiveTypeKind.MAP
    assert result.children[2].kind is HiveTypeKind.UNION
    assert result.children[3].user_defined_name == "example.CustomType"

    with pytest.raises(HiveProtocolError, match="empty"):
        client_module._parse_type_descriptor(SimpleNamespace(types=[]))
    with pytest.raises(HiveProtocolError, match="invalid type pointer"):
        client_module._parse_type_descriptor(
            SimpleNamespace(types=[_entry(arrayEntry=SimpleNamespace(objectTypePtr=4))])
        )
    with pytest.raises(HiveProtocolError, match="recursive"):
        client_module._parse_type_descriptor(
            SimpleNamespace(types=[_entry(arrayEntry=SimpleNamespace(objectTypePtr=0))])
        )
    with pytest.raises(HiveCompatibilityError, match="unknown primitive"):
        client_module._parse_type_descriptor(SimpleNamespace(types=[_primitive(99_999)]))
    with pytest.raises(HiveProtocolError, match="no recognized type arm"):
        client_module._parse_type_descriptor(SimpleNamespace(types=[_entry()]))


def test_schema_vector_and_operation_id_protocol_guards() -> None:
    with pytest.raises(HiveProtocolError, match="omitted the table schema"):
        client_module._normalize_schema(None)
    empty_column = SimpleNamespace(
        boolVal=None,
        byteVal=None,
        i16Val=None,
        i32Val=None,
        i64Val=None,
        doubleVal=None,
        stringVal=None,
        binaryVal=None,
    )
    with pytest.raises(HiveProtocolError, match="exactly one"):
        client_module._normalize_vector(empty_column)
    assert client_module._public_operation_id(object()) is None
    short = SimpleNamespace(operationId=SimpleNamespace(guid=b"short"))
    assert client_module._public_operation_id(short) == b"short".hex()


def test_native_rpc_rejects_malformed_success_responses() -> None:
    connection = HiveConnectionOptions(host="hs2.example")
    missing_handle = SimpleNamespace(status=_status(), sessionHandle=None, serverProtocolVersion=10)
    rpc = NativeHiveRPC(_Transport(), _Client(OpenSession=missing_handle))
    with pytest.raises(HiveProtocolError, match="handle"):
        rpc.open_session(connection, HiveCredentials())

    missing_version = SimpleNamespace(
        status=_status(), sessionHandle=object(), serverProtocolVersion=None
    )
    rpc = NativeHiveRPC(_Transport(), _Client(OpenSession=missing_version))
    with pytest.raises(HiveProtocolError, match="protocol version"):
        rpc.open_session(connection, HiveCredentials())

    no_results = SimpleNamespace(
        status=_status(),
        operationHandle=SimpleNamespace(hasResultSet=False, operationId=None),
    )
    rpc = NativeHiveRPC(_Transport(), _Client(ExecuteStatement=no_results))
    with pytest.raises(HiveProtocolError, match="result-set"):
        rpc.execute_statement(SessionHandle(object()), "SELECT 1", timeout_seconds=1)

    missing_schema = SimpleNamespace(status=_status(), schema=None)
    rpc = NativeHiveRPC(_Transport(), _Client(GetResultSetMetadata=missing_schema))
    with pytest.raises(HiveProtocolError, match="table schema"):
        rpc.get_result_schema(OperationHandle(object()))

    missing_rowset = SimpleNamespace(status=_status(), results=None)
    rpc = NativeHiveRPC(_Transport(), _Client(FetchResults=missing_rowset))
    with pytest.raises(HiveProtocolError, match="omitted its rowset"):
        rpc.fetch_results(OperationHandle(object()), max_rows=1)


def test_native_rpc_preserves_advisory_server_flag_and_unknown_operation_state() -> None:
    vector = ttypes.TColumn(i32Val=ttypes.TI32Column(values=[1], nulls=b"\x00"))
    fetch = SimpleNamespace(
        status=_status(),
        results=ttypes.TRowSet(startRowOffset=0, rows=[], columns=[vector]),
        hasMoreRows=False,
    )
    status = SimpleNamespace(
        status=_status(),
        operationState=999,
        sqlState=None,
        errorCode=None,
        errorMessage="Failure for 'private-literal' token=secret-value",
    )
    rpc = NativeHiveRPC(_Transport(), _Client(FetchResults=fetch, GetOperationStatus=status))

    assert rpc.fetch_results(OperationHandle(object()), max_rows=1).server_has_more_rows is False
    normalized_status = rpc.get_operation_status(OperationHandle(object()))
    assert normalized_status.state is OperationState.UNKNOWN
    assert normalized_status.safe_message is not None
    assert "private-literal" not in normalized_status.safe_message
    assert "secret-value" not in normalized_status.safe_message


def test_native_rpc_rejects_invalid_start_row_offset() -> None:
    fetch = SimpleNamespace(
        status=_status(),
        results=ttypes.TRowSet(startRowOffset=-1, rows=[], columns=[]),
        hasMoreRows=False,
    )
    rpc = NativeHiveRPC(_Transport(), _Client(FetchResults=fetch))

    with pytest.raises(HiveProtocolError, match="start row offset"):
        rpc.fetch_results(OperationHandle(object()), max_rows=1)


def test_driver_resolution_is_explicit_and_never_falls_back() -> None:
    assert client_module.resolve_connection_factory("native") is not None
    with pytest.raises(HiveCompatibilityError, match="not installed"):
        client_module.resolve_connection_factory("pyhive")


class _Socket:
    def __init__(self, *, open_error: Exception | None = None) -> None:
        self.open_error = open_error
        self.handle = SimpleNamespace(shutdown=self._shutdown)
        self.timeouts: list[int] = []
        self.shutdown_calls: list[int] = []
        self.close_calls = 0
        self.resolved_addresses: list[Any] | None = None
        self.open_attempts = 0
        self.handle.close = self._close

    def setTimeout(self, timeout: int) -> None:
        self.timeouts.append(timeout)

    def _shutdown(self, how: int) -> None:
        self.shutdown_calls.append(how)

    def _close(self) -> None:
        self.close_calls += 1

    def _resolveAddr(self) -> list[Any]:
        raise AssertionError("transport must install pre-resolved addresses")

    def _do_open(self, family: int, socket_type: int) -> None:
        assert family == socket.AF_INET
        assert socket_type == socket.SOCK_STREAM
        self.open_attempts += 1

    def open(self) -> None:
        self.resolved_addresses = self._resolveAddr()
        family, socket_type, *_ = self.resolved_addresses[0]
        self._do_open(family, socket_type)
        if self.open_error is not None:
            raise self.open_error


class _Buffered:
    def __init__(self, thrift_socket: _Socket) -> None:
        self.socket = thrift_socket
        self.closed = False

    def open(self) -> None:
        self.socket.open()

    def close(self) -> None:
        self.closed = True


class _ValueErrorBuffered(_Buffered):
    def close(self) -> None:
        super().close()
        raise ValueError("TLS unwrap after socket shutdown")


class _RuntimeErrorBuffered(_Buffered):
    def close(self) -> None:
        super().close()
        raise RuntimeError("transport close failed")


def _install_immediate_resolution(monkeypatch: pytest.MonkeyPatch) -> list[tuple[object, ...]]:
    addresses = [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            "",
            ("127.0.0.1", 10000),
        )
    ]

    def resolve(
        host: str,
        port: int,
        family: int,
        socket_type: int,
        protocol: int,
        flags: int,
    ) -> list[tuple[object, ...]]:
        assert (host, port) == ("hs2.example", 10_000)
        assert family == socket.AF_UNSPEC
        assert socket_type == socket.SOCK_STREAM
        assert protocol == 0
        assert flags == 0
        return addresses

    monkeypatch.setattr(transport_module.socket, "getaddrinfo", resolve)
    return addresses


def test_native_transport_timeout_abort_and_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    addresses = _install_immediate_resolution(monkeypatch)
    created: list[_Socket] = []

    def socket_factory(host: str, port: int) -> _Socket:
        assert (host, port) == ("hs2.example", 10000)
        created.append(_Socket())
        return created[-1]

    monkeypatch.setattr(transport_module.TSocket, "TSocket", socket_factory)
    monkeypatch.setattr(transport_module.TTransport, "TBufferedTransport", _Buffered)
    monkeypatch.setattr(
        transport_module.TBinaryProtocol,
        "TBinaryProtocol",
        lambda buffered: ("protocol", buffered),
    )
    connection = HiveConnectionOptions(host="hs2.example", connect_timeout=1.25, rpc_timeout=2.5)

    opened = transport_module.open_native_transport(connection)
    opened.abort_handle.abort()
    opened.close()

    assert 1 <= created[0].timeouts[0] <= 1250
    assert created[0].timeouts[-1] == 2500
    assert created[0].resolved_addresses == addresses
    assert created[0].open_attempts == 1
    assert created[0].shutdown_calls == [socket.SHUT_RDWR]
    assert created[0].close_calls == 1
    assert opened.transport.closed is True

    monkeypatch.setattr(
        transport_module.TSocket,
        "TSocket",
        lambda host, port: _Socket(open_error=OSError("refused")),
    )
    with pytest.raises(HiveTransportError, match="Unable to connect"):
        transport_module.open_native_transport(connection)


def test_native_transport_rounds_positive_submillisecond_timeout_up() -> None:
    assert transport_module._timeout_milliseconds(0.0004) == 1


def test_native_transport_rejects_unverified_thrift_version_before_dns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transport_module, "package_version", lambda distribution: "0.17.0")

    def unexpected_resolution(*args: object) -> list[Any]:
        del args
        raise AssertionError("DNS must not run for an unsupported Thrift version")

    monkeypatch.setattr(transport_module.socket, "getaddrinfo", unexpected_resolution)

    with pytest.raises(HiveCompatibilityError, match=r"requires Thrift 0\.22\.0"):
        transport_module.open_native_transport(HiveConnectionOptions(host="hs2.example"))


def test_native_transport_rejects_missing_thrift_distribution_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_distribution(distribution: str) -> str:
        raise transport_module.PackageNotFoundError(distribution)

    monkeypatch.setattr(transport_module, "package_version", missing_distribution)

    with pytest.raises(HiveCompatibilityError, match="verify the installed Thrift version"):
        transport_module.open_native_transport(HiveConnectionOptions(host="hs2.example"))


def _empty_addresses() -> list[Any]:
    return []


@pytest.mark.parametrize(
    ("thrift_socket", "message"),
    [
        (SimpleNamespace(_do_open=lambda family, socket_type: None), "hook contract"),
        (SimpleNamespace(_resolveAddr=_empty_addresses, _do_open=None), "hook contract"),
        (
            SimpleNamespace(
                _resolveAddr=lambda unexpected: [],
                _do_open=lambda family, socket_type: None,
            ),
            "hook signature",
        ),
        (
            SimpleNamespace(
                _resolveAddr=_empty_addresses,
                _do_open=lambda family: None,
            ),
            "hook signature",
        ),
    ],
)
def test_native_transport_rejects_incompatible_thrift_socket_hooks(
    thrift_socket: object,
    message: str,
) -> None:
    with pytest.raises(HiveCompatibilityError, match=message):
        transport_module._install_connect_deadline(thrift_socket, [], deadline=1.0)


def test_native_transport_accepts_verified_thrift_socket_hooks() -> None:
    thrift_socket = transport_module.TSocket.TSocket("hs2.example", 10000)

    open_socket = transport_module._require_thrift_socket_hooks(thrift_socket)

    assert tuple(transport_module.inspect.signature(open_socket).parameters) == (
        "family",
        "socktype",
    )


def test_native_transport_rejects_uninspectable_thrift_socket_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def uninspectable_signature(callable_: object) -> object:
        del callable_
        raise ValueError("signature unavailable")

    monkeypatch.setattr(transport_module.inspect, "signature", uninspectable_signature)
    thrift_socket = SimpleNamespace(
        _resolveAddr=_empty_addresses,
        _do_open=lambda family, socket_type: None,
    )

    with pytest.raises(HiveCompatibilityError, match="hook signature"):
        transport_module._install_connect_deadline(thrift_socket, [], deadline=1.0)


@pytest.mark.parametrize("resolver_result", [[], OSError("resolver failed")])
def test_native_transport_rejects_empty_or_failed_address_resolution(
    monkeypatch: pytest.MonkeyPatch,
    resolver_result: object,
) -> None:
    def resolve(*args: object) -> list[Any]:
        del args
        if isinstance(resolver_result, Exception):
            raise resolver_result
        return []

    monkeypatch.setattr(transport_module.socket, "getaddrinfo", resolve)

    with pytest.raises(HiveTransportError, match="Unable to resolve"):
        transport_module._resolve_addresses(
            "hs2.example",
            10_000,
            deadline=transport_module.time.monotonic() + 1,
        )


def test_native_transport_bounds_blocking_address_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    calls = 0

    def blocking_resolution(
        host: str,
        port: int,
        family: int,
        socket_type: int,
        protocol: int,
        flags: int,
    ) -> list[Any]:
        nonlocal calls
        del host, port, family, socket_type, protocol, flags
        calls += 1
        entered.set()
        release.wait()
        return []

    monkeypatch.setattr(transport_module.socket, "getaddrinfo", blocking_resolution)
    connection = HiveConnectionOptions(host="hs2.example", connect_timeout=0.01)

    try:
        with pytest.raises(HiveTransportError, match="address resolution deadline"):
            transport_module.open_native_transport(connection)
        assert entered.wait(timeout=1)
        with pytest.raises(HiveTransportError, match="address resolution deadline"):
            transport_module.open_native_transport(connection)
        assert calls == 1
    finally:
        release.set()
        assert transport_module._DNS_RESOLUTION_SLOT.acquire(timeout=1)
        transport_module._DNS_RESOLUTION_SLOT.release()


def test_native_transport_preserves_connection_deadline_error_after_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_immediate_resolution(monkeypatch)
    thrift_socket = _Socket(
        open_error=HiveTransportError("HiveServer2 connection deadline expired")
    )
    buffered: list[_Buffered] = []

    monkeypatch.setattr(
        transport_module.TSocket,
        "TSocket",
        lambda host, port: thrift_socket,
    )

    def buffered_factory(socket_: _Socket) -> _Buffered:
        buffered.append(_ValueErrorBuffered(socket_))
        return buffered[-1]

    monkeypatch.setattr(
        transport_module.TTransport,
        "TBufferedTransport",
        buffered_factory,
    )

    with pytest.raises(
        HiveTransportError,
        match=r"^HiveServer2 connection deadline expired",
    ) as caught:
        transport_module.open_native_transport(
            HiveConnectionOptions(host="hs2.example", connect_timeout=1.0)
        )

    assert str(caught.value) == "HiveServer2 connection deadline expired"
    assert caught.value.__notes__ == ["Transport cleanup also failed after connect: ValueError"]
    assert buffered[0].closed is True
    assert thrift_socket.close_calls == 1
    assert thrift_socket.handle is None


def test_native_transport_refreshes_deadline_for_each_address_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    thrift_socket = _Socket()
    addresses = _install_immediate_resolution(monkeypatch)
    monotonic_values = iter([9.0, 9.5])
    monkeypatch.setattr(transport_module.time, "monotonic", lambda: next(monotonic_values))
    transport_module._install_connect_deadline(thrift_socket, addresses, deadline=10.0)

    thrift_socket._do_open(socket.AF_INET, socket.SOCK_STREAM)
    thrift_socket._do_open(socket.AF_INET, socket.SOCK_STREAM)

    assert thrift_socket.timeouts == [1000, 500]
    assert thrift_socket.open_attempts == 2


def test_native_transport_close_is_idempotent_after_tls_style_abort() -> None:
    thrift_socket = _Socket()
    buffered = _ValueErrorBuffered(thrift_socket)
    transport = transport_module.NativeTransport(
        buffered,
        object(),
        transport_module.SocketAbortHandle(thrift_socket),
    )

    transport.abort_handle.abort()
    transport.close()
    transport.close()

    assert thrift_socket.close_calls == 1
    assert thrift_socket.handle is None
    assert buffered.closed is True


def test_native_transport_closes_socket_when_transport_close_fails() -> None:
    thrift_socket = _Socket()
    buffered = _RuntimeErrorBuffered(thrift_socket)
    transport = transport_module.NativeTransport(
        buffered,
        object(),
        transport_module.SocketAbortHandle(thrift_socket),
    )

    with pytest.raises(RuntimeError, match="transport close failed"):
        transport.close()
    transport.close()

    assert thrift_socket.close_calls == 1
    assert thrift_socket.handle is None
    assert buffered.closed is True


@pytest.mark.parametrize(
    "connection",
    [
        HiveConnectionOptions(host="hs2.example", transport="http"),
        HiveConnectionOptions(host="hs2.example", auth="LDAP", tls=HiveTLSOptions(enabled=True)),
    ],
)
def test_native_transport_rejects_unverified_profiles(
    connection: HiveConnectionOptions,
) -> None:
    with pytest.raises(HiveCompatibilityError):
        transport_module.open_native_transport(connection)
