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

import threading
import time
from collections import deque

import pytest

from ray_hive import (
    HiveConnectionOptions,
    HiveProtocolError,
    HiveTransportError,
    SecretRef,
)
from ray_hive._hs2.protocol import (
    HiveColumnSchema,
    HiveColumnVector,
    HiveResultSchema,
    HiveRowSet,
    HiveType,
    HiveTypeKind,
)
from ray_hive._models import HiveConsistency, UnsupportedTypeMode
from ray_hive._schema import result_schema_to_arrow
from ray_hive._task_spec import HiveTaskSpec
from ray_hive._worker import iter_hive_blocks
from tests._fakes import FakeFactory, FakeRPC


def _schema() -> HiveResultSchema:
    return HiveResultSchema((HiveColumnSchema("id", HiveType(HiveTypeKind.INT), position=1),))


def _spec(connection: HiveConnectionOptions, *, target_bytes: int = 1024) -> HiveTaskSpec:
    hive_schema = _schema()
    return HiveTaskSpec(
        connection=connection,
        statement="SELECT id FROM db.events",
        expected_arrow_schema=result_schema_to_arrow(hive_schema),
        expected_hive_schema=hive_schema,
        fetch_rows=2,
        target_batch_bytes=target_bytes,
        query_timeout=5,
        consistency=HiveConsistency.STRICT,
        unsupported_type_mode=UnsupportedTypeMode.ERROR,
    )


def test_worker_uses_one_io_thread_and_streams_multiple_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(
        _schema(),
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector((1, 2), b"\x00"),), True),
                HiveRowSet(0, (HiveColumnVector((3,), b"\x00"),), False),
                HiveRowSet(0, (HiveColumnVector((), b""),), False),
            ]
        ),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    control_thread = threading.get_ident()

    blocks = list(iter_hive_blocks(_spec(HiveConnectionOptions(host="hs2.example"))))

    assert [row for block in blocks for row in block.column(0).to_pylist()] == [1, 2, 3]
    assert len(blocks) == 2
    assert len(set(rpc.thread_ids)) == 1
    assert rpc.thread_ids[0] != control_thread
    assert rpc.events[-3:] == ["close_operation", "close_session", "close_transport"]
    assert not any(thread.name.startswith("ray-hive-io") for thread in threading.enumerate())


def test_worker_resolves_secret_on_io_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HIVE_PASSWORD", "worker-only-secret")
    rpc = FakeRPC(
        _schema(),
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), False),
                HiveRowSet(0, (HiveColumnVector((), b""),), False),
            ]
        ),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    connection = HiveConnectionOptions(
        host="hs2.example", auth="LDAP", credentials=SecretRef.env("HIVE_PASSWORD")
    )

    list(iter_hive_blocks(_spec(connection)))

    assert rpc.opened_credentials is not None
    assert rpc.opened_credentials.password == "worker-only-secret"


def test_worker_does_not_resolve_secret_for_nosasl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(
        _schema(),
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), False),
                HiveRowSet(0, (HiveColumnVector((), b""),), False),
            ]
        ),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    connection = HiveConnectionOptions(
        host="hs2.example", credentials=SecretRef.env("MISSING_HIVE_PASSWORD")
    )

    list(iter_hive_blocks(_spec(connection)))

    assert rpc.opened_credentials is not None
    assert rpc.opened_credentials.password is None


def test_midstream_transport_failure_is_not_replayed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(
        _schema(),
        rowsets=deque([HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), True)]),
        fetch_error=OSError("connection reset"),
        fetch_error_after=1,
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    stream = iter_hive_blocks(_spec(HiveConnectionOptions(host="hs2.example")))

    assert next(stream).column(0).to_pylist() == [1]
    with pytest.raises(HiveTransportError) as caught:
        next(stream)

    assert caught.value.partial_output is True
    assert caught.value.retryable is False
    assert rpc.events.count("execute_statement") == 1


def test_midstream_protocol_failure_is_marked_as_partial_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(
        _schema(),
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), False),
                HiveRowSet(
                    0,
                    (HiveColumnVector((2,), b"\x00"),),
                    False,
                    binary_columns=b"payload",
                    binary_column_count=1,
                ),
            ]
        ),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    stream = iter_hive_blocks(_spec(HiveConnectionOptions(host="hs2.example")))

    assert next(stream).column(0).to_pylist() == [1]
    with pytest.raises(HiveProtocolError) as caught:
        next(stream)

    assert caught.value.partial_output is True
    assert caught.value.retryable is False


def test_generator_close_cancels_before_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(
        _schema(),
        rowsets=deque([HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), True)]),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    stream = iter_hive_blocks(_spec(HiveConnectionOptions(host="hs2.example")))
    next(stream)

    stream.close()

    assert rpc.abort.calls == 0
    cleanup = rpc.events[rpc.events.index("cancel_operation") :]
    assert cleanup == [
        "cancel_operation",
        "close_operation",
        "close_session",
        "close_transport",
    ]


def test_fetched_batch_is_fully_sliced_before_the_eof_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = tuple("x" * 100 for _ in range(10))
    schema = HiveResultSchema(
        (HiveColumnSchema("value", HiveType(HiveTypeKind.STRING), position=1),)
    )
    rpc = FakeRPC(
        schema,
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector(values, b"\x00\x00"),), False),
                HiveRowSet(0, (HiveColumnVector((), b""),), False),
            ]
        ),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    spec = HiveTaskSpec(
        connection=HiveConnectionOptions(host="hs2.example"),
        statement="SELECT value FROM db.events",
        expected_arrow_schema=result_schema_to_arrow(schema),
        expected_hive_schema=schema,
        fetch_rows=10,
        target_batch_bytes=200,
        query_timeout=5,
        consistency=HiveConsistency.STRICT,
        unsupported_type_mode=UnsupportedTypeMode.ERROR,
    )

    stream = iter_hive_blocks(spec)
    first = next(stream)
    second = next(stream)

    assert rpc.fetch_calls == 1
    blocks = [first, second, *stream]
    assert len(blocks) > 1
    assert rpc.fetch_calls == 2
    assert sum(block.num_rows for block in blocks) == 10


def test_downstream_consumption_time_does_not_consume_query_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRPC(
        _schema(),
        rowsets=deque(
            [
                HiveRowSet(0, (HiveColumnVector((1,), b"\x00"),), False),
                HiveRowSet(0, (HiveColumnVector((), b""),), False),
            ]
        ),
    )
    monkeypatch.setattr(
        "ray_hive._worker.resolve_connection_factory", lambda driver: FakeFactory(rpc)
    )
    spec = _spec(HiveConnectionOptions(host="hs2.example"))
    spec = HiveTaskSpec(
        connection=spec.connection,
        statement=spec.statement,
        expected_arrow_schema=spec.expected_arrow_schema,
        expected_hive_schema=spec.expected_hive_schema,
        fetch_rows=spec.fetch_rows,
        target_batch_bytes=spec.target_batch_bytes,
        query_timeout=0.05,
        consistency=spec.consistency,
        unsupported_type_mode=spec.unsupported_type_mode,
    )
    stream = iter_hive_blocks(spec)

    assert next(stream).column(0).to_pylist() == [1]
    time.sleep(0.1)

    with pytest.raises(StopIteration):
        next(stream)
    assert rpc.abort.calls == 0
