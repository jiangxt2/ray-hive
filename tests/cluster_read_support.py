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

"""Observe real HS2 resources from a Ray read task without recording query data."""

from __future__ import annotations

import time
from collections.abc import Iterator
from functools import partial
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import ray
from ray.actor import ActorHandle
from ray.data import ReadTask

from ray_hive._compat import create_read_task
from ray_hive._hs2.client import NativeConnectionFactory
from ray_hive._models import HiveConnectionOptions
from ray_hive._task_spec import HiveTaskSpec
from ray_hive._worker import iter_hive_blocks
from ray_hive.datasource import HiveServer2Datasource


@ray.remote(num_cpus=0)
class ReadEvents:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.released = False

    def record(self, event: str) -> None:
        self.counts[event] = self.counts.get(event, 0) + 1

    def release(self) -> None:
        self.released = True

    def is_released(self) -> bool:
        return self.released

    def snapshot(self) -> dict[str, int]:
        return dict(self.counts)


class _ObservedRPC:
    def __init__(self, rpc: Any, events: ActorHandle) -> None:
        self.rpc = rpc
        self.events = events

    def __getattr__(self, name: str) -> Any:
        return getattr(self.rpc, name)

    def execute_statement(self, *args: Any, **kwargs: Any) -> Any:
        ray.get(self.events.record.remote("queries"))
        return self.rpc.execute_statement(*args, **kwargs)

    def cancel_operation(self, *args: Any, **kwargs: Any) -> None:
        self.rpc.cancel_operation(*args, **kwargs)
        ray.get(self.events.record.remote("cancel_operation"))

    def close_operation(self, *args: Any, **kwargs: Any) -> None:
        self.rpc.close_operation(*args, **kwargs)
        ray.get(self.events.record.remote("close_operation"))

    def close_session(self, *args: Any, **kwargs: Any) -> None:
        self.rpc.close_session(*args, **kwargs)
        ray.get(self.events.record.remote("close_session"))

    def close_transport(self) -> None:
        self.rpc.close_transport()
        ray.get(self.events.record.remote("close_transport"))


class _ObservedFactory:
    def __init__(self, events: ActorHandle) -> None:
        self.events = events

    def connect(self, connection: HiveConnectionOptions) -> _ObservedRPC:
        return _ObservedRPC(NativeConnectionFactory().connect(connection), self.events)


def _read_observed(
    spec: HiveTaskSpec, events: ActorHandle, fail_after_first: bool
) -> Iterator[pa.Table]:
    ray.get(events.record.remote("attempts"))
    with patch(
        "ray_hive._worker.resolve_connection_factory", return_value=_ObservedFactory(events)
    ):
        stream = iter_hive_blocks(spec)
        try:
            for block in stream:
                ray.get(events.record.remote("blocks"))
                yield block
                # Keep the operation live until the consumer releases the fault or
                # Ray cancels the read after a downstream limit.
                deadline = time.monotonic() + 30
                while not ray.get(events.is_released.remote()):
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Timed out waiting for the partial-output consumer")
                    time.sleep(0.05)
                if fail_after_first:
                    raise RuntimeError("Injected HS2 fault after partial output")
            ray.get(events.record.remote("eof"))
        finally:
            stream.close()


class ObservedHiveDatasource(HiveServer2Datasource):
    def __init__(
        self, task_specs: tuple[HiveTaskSpec, ...], events: ActorHandle, *, fail_after_first: bool
    ) -> None:
        super().__init__(task_specs)
        self.events = events
        self.fail_after_first = fail_after_first

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        del parallelism, data_context
        return [
            create_read_task(
                partial(_read_observed, spec, self.events, self.fail_after_first),
                spec.expected_arrow_schema,
                per_task_row_limit=per_task_row_limit,
            )
            for spec in self.task_specs
        ]
