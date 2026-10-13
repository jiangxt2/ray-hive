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

from collections.abc import Callable, Iterator
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import cluster_read_support as support
import pyarrow as pa
import pytest
from ray.actor import ActorHandle

from ray_hive._task_spec import HiveTaskSpec


@pytest.fixture
def observed_read(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Callable[[bool], Iterator[pa.Table]], Mock, list[int], list[bool]]:
    events = Mock()
    fetched: list[int] = []
    closed: list[bool] = []

    def blocks() -> Iterator[pa.Table]:
        try:
            for value in range(1, 5):
                fetched.append(value)
                yield pa.table({"id": [value]})
        finally:
            closed.append(True)

    native_stream = blocks()
    monkeypatch.setattr(support, "iter_hive_blocks", lambda spec: native_stream)
    monkeypatch.setattr(support.ray, "get", lambda reference: reference)

    def read(fail_after_first: bool) -> Iterator[pa.Table]:
        return support._read_observed(
            cast(HiveTaskSpec, object()), cast(ActorHandle, events), fail_after_first
        )

    return read, events, fetched, closed


def test_early_close_gate_prevents_eof_and_closes_stream_on_cancellation(observed_read) -> None:
    read, events, fetched, closed = observed_read
    events.is_released.remote.side_effect = KeyboardInterrupt
    stream = read(False)
    assert next(stream).num_rows == 1

    with pytest.raises(KeyboardInterrupt):
        next(stream)

    assert fetched == [1]
    assert closed == [True]
    assert events.record.remote.call_args_list[-1].args == ("blocks",)


def test_early_close_gate_timeout_closes_stream(
    observed_read, monkeypatch: pytest.MonkeyPatch
) -> None:
    read, events, fetched, closed = observed_read
    events.is_released.remote.return_value = False
    clock = SimpleNamespace(monotonic=Mock(side_effect=[0, 31]), sleep=Mock())
    monkeypatch.setattr(support, "time", clock)
    stream = read(False)
    next(stream)

    with pytest.raises(RuntimeError, match="Timed out waiting for the partial-output consumer"):
        next(stream)

    assert fetched == [1]
    assert closed == [True]
    clock.sleep.assert_not_called()


def test_partial_output_fault_is_released_without_consuming_a_second_block(observed_read) -> None:
    read, events, fetched, closed = observed_read
    events.is_released.remote.return_value = True
    stream = read(True)
    next(stream)

    with pytest.raises(RuntimeError, match="Injected HS2 fault after partial output"):
        next(stream)

    assert fetched == [1]
    assert closed == [True]
