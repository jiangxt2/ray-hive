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
from concurrent.futures import Future

import pyarrow as pa
import pytest

from ray_hive import (
    HiveCancellationError,
    HiveCleanupError,
    HiveConnectionOptions,
    HiveProtocolError,
    HiveSchemaError,
    HiveTransportError,
    SecretRef,
)
from ray_hive._capabilities import HiveCapabilities
from ray_hive._credentials import HiveCredentials
from ray_hive._hs2.protocol import (
    HiveColumnSchema,
    HiveColumnVector,
    HiveResultSchema,
    HiveRowSet,
    HiveType,
    HiveTypeKind,
    SessionHandle,
)
from ray_hive._hs2.session import HiveSession, resolve_credentials
from ray_hive._models import HiveConsistency, UnsupportedTypeMode
from ray_hive._schema import result_schema_to_arrow
from ray_hive._task_spec import HiveTaskSpec
from ray_hive._worker import (
    _AbortState,
    _close_abandoned_stream,
    _ExecutionStream,
    _future_result,
    iter_hive_blocks,
)
from tests._fakes import FakeAbortHandle, FakeFactory, FakeRPC


def _schema() -> HiveResultSchema:
    return HiveResultSchema((HiveColumnSchema("id", HiveType(HiveTypeKind.INT), position=1),))


def _spec(
    connection: HiveConnectionOptions | None = None,
    *,
    expected_hive_schema: HiveResultSchema | object | None = ...,
    arrow_schema: pa.Schema | None = None,
) -> HiveTaskSpec:
    schema = _schema()
    expected = schema if expected_hive_schema is ... else expected_hive_schema
    return HiveTaskSpec(
        connection=connection or HiveConnectionOptions(host="hs2.example"),
        statement="SELECT id FROM db.events",
        expected_arrow_schema=arrow_schema or result_schema_to_arrow(schema),
        expected_hive_schema=expected,
        fetch_rows=2,
        target_batch_bytes=1024,
        query_timeout=1,
        consistency=HiveConsistency.STRICT,
        unsupported_type_mode=UnsupportedTypeMode.ERROR,
    )


def _stream(rpc: FakeRPC, spec: HiveTaskSpec | None = None) -> _ExecutionStream:
    task_spec = spec or _spec(
        expected_hive_schema=rpc.schema,
        arrow_schema=result_schema_to_arrow(rpc.schema),
    )
    session = HiveSession.open(rpc, task_spec.connection)
    operation = session.execute(task_spec.statement, timeout_seconds=1)
    return _ExecutionStream(
        task_spec,
        session,
        operation,
        rpc.schema,
        result_schema_to_arrow(rpc.schema),
    )


def test_abort_requested_before_handle_publication_is_delivered() -> None:
    state = _AbortState()
    handle = FakeAbortHandle()

    state.abort()
    state.publish(handle)
    state.abort()

    assert handle.calls == 2


def test_future_deadline_aborts_socket_boundary() -> None:
    state = _AbortState()
    handle = FakeAbortHandle()
    state.publish(handle)

    with pytest.raises(HiveCancellationError, match="deadline"):
        _future_result(
            Future(),
            timeout=0,
            abort_state=state,
            timeout_message="test deadline expired",
        )

    assert handle.calls == 1


def test_session_open_failure_closes_transport_without_masking_primary() -> None:
    class OpenFailureRPC(FakeRPC):
        def open_session(
            self,
            connection: HiveConnectionOptions,
            credentials: HiveCredentials,
        ) -> tuple[SessionHandle, HiveCapabilities]:
            del connection, credentials
            self._record("open_session")
            raise HiveTransportError("open failed")

    rpc = OpenFailureRPC(_schema())
    with pytest.raises(HiveTransportError, match="open failed"):
        HiveSession.open(rpc, HiveConnectionOptions(host="hs2.example"))

    assert rpc.events[-1] == "close_transport"


def test_execution_open_does_not_close_transport_twice_after_session_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OpenFailureRPC(FakeRPC):
        def open_session(
            self,
            connection: HiveConnectionOptions,
            credentials: HiveCredentials,
        ) -> tuple[SessionHandle, HiveCapabilities]:
            del connection, credentials
            self._record("open_session")
            raise HiveTransportError("open failed")

    rpc = OpenFailureRPC(_schema())
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )

    with pytest.raises(HiveTransportError, match="open failed"):
        _ExecutionStream.open(_spec(), lambda handle: None)

    assert rpc.events.count("close_transport") == 1


def test_stream_that_opens_after_timeout_is_cancelled_and_closed() -> None:
    rpc = FakeRPC(_schema())
    stream = _stream(rpc)
    opened: Future[_ExecutionStream] = Future()
    opened.set_result(stream)

    _close_abandoned_stream(opened)

    assert rpc.events[-4:] == [
        "cancel_operation",
        "close_operation",
        "close_session",
        "close_transport",
    ]


def test_session_close_aggregates_failures_and_is_idempotent() -> None:
    class CloseFailureRPC(FakeRPC):
        def close_session(self, session: SessionHandle) -> None:
            super().close_session(session)
            raise OSError("session close")

        def close_transport(self) -> None:
            super().close_transport()
            raise OSError("transport close")

    rpc = CloseFailureRPC(_schema())
    session = HiveSession.open(rpc, HiveConnectionOptions(host="hs2.example"))

    with pytest.raises(HiveCleanupError, match="close Hive session"):
        session.close()
    event_count = len(rpc.events)
    session.close()

    assert len(rpc.events) == event_count
    assert rpc.events[-2:] == ["close_session", "close_transport"]


def test_authenticated_credentials_resolve_and_explicit_username_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HIVE_PASSWORD", "worker-secret")
    connection = HiveConnectionOptions(
        host="hs2.example",
        auth="LDAP",
        username="explicit-reader",
        credentials=SecretRef.env("HIVE_PASSWORD"),
    )

    credentials = resolve_credentials(connection)

    assert credentials.username == "explicit-reader"
    assert credentials.password == "worker-secret"


def test_execution_open_rejects_raw_sql_schema_drift_and_cleans_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(_schema())
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    spec = _spec(
        expected_hive_schema=None,
        arrow_schema=pa.schema([("id", pa.string())]),
    )

    with pytest.raises(HiveSchemaError, match="explicit Arrow schema"):
        _ExecutionStream.open(spec, lambda handle: None)

    assert rpc.events[-3:] == ["close_operation", "close_session", "close_transport"]


def test_execution_stream_uses_complete_empty_fetch_as_eof() -> None:
    terminal_rpc = FakeRPC(
        _schema(),
        rowsets=deque([HiveRowSet(0, (HiveColumnVector((), b""),), False)]),
    )
    terminal = _stream(terminal_rpc)
    assert terminal.next_block() is None
    terminal.close(cancel=False)

    server_false_rpc = FakeRPC(
        _schema(),
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), False),
                HiveRowSet(0, (HiveColumnVector((), b""),), False),
            ]
        ),
    )
    server_false = _stream(server_false_rpc)
    assert server_false.next_block().column(0).to_pylist() == [1]
    assert server_false.next_block() is None
    server_false.close(cancel=False)


@pytest.mark.parametrize(
    ("rowset", "message"),
    [
        (HiveRowSet(0, (), False), "column count"),
        (
            HiveRowSet(0, (HiveColumnVector((), b""),), True),
            "reporting more rows",
        ),
    ],
)
def test_execution_stream_rejects_malformed_empty_fetches(
    rowset: HiveRowSet,
    message: str,
) -> None:
    rpc = FakeRPC(_schema(), rowsets=deque([rowset]))
    stream = _stream(rpc)

    with pytest.raises(HiveProtocolError, match=message):
        stream.next_block()
    stream.close(cancel=True)


def test_execution_stream_rejects_mismatched_vector_lengths() -> None:
    schema = HiveResultSchema(
        (
            HiveColumnSchema("id", HiveType(HiveTypeKind.INT), position=1),
            HiveColumnSchema("value", HiveType(HiveTypeKind.INT), position=2),
        )
    )
    mismatched_rpc = FakeRPC(
        schema,
        rowsets=deque(
            [
                HiveRowSet(
                    0,
                    (HiveColumnVector((), b""), HiveColumnVector((1,), b"\x00")),
                    False,
                )
            ]
        ),
    )
    mismatched = _stream(mismatched_rpc)
    with pytest.raises(HiveProtocolError, match="different lengths"):
        mismatched.next_block()
    mismatched.close(cancel=True)


def test_execution_stream_cleanup_attempts_every_resource() -> None:
    events: list[str] = []

    class FailingOperation:
        def cancel(self) -> None:
            events.append("cancel")
            raise OSError("cancel")

        def close(self) -> None:
            events.append("operation-close")
            raise OSError("operation close")

    class FailingSession:
        def close(self) -> None:
            events.append("session-close")
            raise OSError("session close")

    stream = _ExecutionStream(
        _spec(),
        FailingSession(),
        FailingOperation(),
        _schema(),
        result_schema_to_arrow(_schema()),
    )

    with pytest.raises(HiveCleanupError, match="worker resource cleanup"):
        stream.close(cancel=True)
    stream.close(cancel=True)

    assert events == ["cancel", "operation-close", "session-close"]


def test_iter_rejects_invalid_open_result_and_invalid_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _ExecutionStream,
        "open",
        staticmethod(lambda spec, publish_abort: object()),
    )
    with pytest.raises(HiveTransportError, match="initialize"):
        list(iter_hive_blocks(_spec()))

    rpc = FakeRPC(_schema())
    stream = _stream(rpc)
    stream._pending = iter(["not-an-arrow-block"])
    monkeypatch.setattr(
        _ExecutionStream,
        "open",
        staticmethod(lambda spec, publish_abort: stream),
    )
    with pytest.raises(HiveTransportError, match="invalid block"):
        list(iter_hive_blocks(_spec()))


def test_iter_surfaces_cleanup_failure_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CloseFailureRPC(FakeRPC):
        def close_session(self, session: SessionHandle) -> None:
            super().close_session(session)
            raise OSError("close failed")

    rpc = CloseFailureRPC(_schema())
    stream = _stream(rpc)
    stream._source_exhausted = True
    monkeypatch.setattr(
        _ExecutionStream,
        "open",
        staticmethod(lambda spec, publish_abort: stream),
    )

    with pytest.raises(HiveCleanupError, match="worker resource cleanup"):
        list(iter_hive_blocks(_spec()))
