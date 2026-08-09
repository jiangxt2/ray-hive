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
from collections import deque
from dataclasses import dataclass, field

from ray_hive._capabilities import CapabilityState, HiveCapabilities
from ray_hive._credentials import HiveCredentials
from ray_hive._hs2.protocol import (
    HiveResultSchema,
    HiveRowSet,
    OperationHandle,
    OperationState,
    OperationStatus,
    SessionHandle,
)
from ray_hive._models import HiveConnectionOptions


@dataclass(slots=True)
class FakeAbortHandle:
    event: threading.Event = field(default_factory=threading.Event)
    calls: int = 0

    def abort(self) -> None:
        self.calls += 1
        self.event.set()


@dataclass(slots=True)
class FakeRPC:
    schema: HiveResultSchema
    rowsets: deque[HiveRowSet] = field(default_factory=deque)
    statuses: deque[OperationStatus] = field(
        default_factory=lambda: deque([OperationStatus(OperationState.FINISHED)])
    )
    events: list[str] = field(default_factory=list)
    thread_ids: list[int] = field(default_factory=list)
    abort: FakeAbortHandle = field(default_factory=FakeAbortHandle)
    fetch_error: Exception | None = None
    fetch_error_after: int = 0
    fetch_calls: int = 0
    opened_credentials: HiveCredentials | None = None

    def _record(self, event: str) -> None:
        self.events.append(event)
        self.thread_ids.append(threading.get_ident())

    @property
    def abort_handle(self) -> FakeAbortHandle:
        return self.abort

    def open_session(
        self, connection: HiveConnectionOptions, credentials: HiveCredentials
    ) -> tuple[SessionHandle, HiveCapabilities]:
        self._record("open_session")
        self.opened_credentials = credentials
        return SessionHandle(object()), HiveCapabilities(
            protocol_version=11,
            columnar_rowsets=CapabilityState.SUPPORTED,
            binary_columns=CapabilityState.UNSUPPORTED,
            cancel_operation=CapabilityState.SUPPORTED,
            operation_log=CapabilityState.UNKNOWN,
        )

    def execute_statement(
        self,
        session: SessionHandle,
        statement: str,
        *,
        timeout_seconds: int,
    ) -> OperationHandle:
        self._record("execute_statement")
        return OperationHandle(object(), public_id="fake-operation")

    def get_operation_status(self, operation: OperationHandle) -> OperationStatus:
        self._record("get_operation_status")
        if len(self.statuses) > 1:
            return self.statuses.popleft()
        return self.statuses[0]

    def get_result_schema(self, operation: OperationHandle) -> HiveResultSchema:
        self._record("get_result_schema")
        return self.schema

    def fetch_results(self, operation: OperationHandle, *, max_rows: int) -> HiveRowSet:
        self._record("fetch_results")
        self.fetch_calls += 1
        if self.fetch_error is not None and self.fetch_calls > self.fetch_error_after:
            raise self.fetch_error
        if self.rowsets:
            return self.rowsets.popleft()
        return HiveRowSet(0, (), False)

    def cancel_operation(self, operation: OperationHandle) -> None:
        self._record("cancel_operation")

    def close_operation(self, operation: OperationHandle) -> None:
        self._record("close_operation")

    def close_session(self, session: SessionHandle) -> None:
        self._record("close_session")

    def close_transport(self) -> None:
        self._record("close_transport")


@dataclass(frozen=True, slots=True)
class FakeFactory:
    rpc: FakeRPC

    def connect(self, connection: HiveConnectionOptions) -> FakeRPC:
        self.rpc.events.append("connect")
        return self.rpc
