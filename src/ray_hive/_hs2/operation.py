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

"""Hive operation state machine with idempotent cancellation and cleanup."""

from __future__ import annotations

import time
from dataclasses import dataclass

from ray_hive._errors import (
    HiveCancellationError,
    HiveCleanupError,
    HiveError,
    HivePlanningError,
    HiveQueryError,
)
from ray_hive._hs2.protocol import (
    HiveResultSchema,
    HiveRowSet,
    HiveRPC,
    OperationHandle,
    OperationState,
    OperationStatus,
)


def _query_failure(status: OperationStatus) -> HiveError:
    details = []
    if status.sql_state:
        details.append(f"SQLState={status.sql_state}")
    if status.error_code is not None:
        details.append(f"server_error_code={status.error_code}")
    suffix = f" ({', '.join(details)})" if details else ""
    server_detail = f": {status.safe_message}" if status.safe_message else ""
    if status.sql_state and status.sql_state.startswith("42"):
        return HivePlanningError(
            f"Hive rejected the query during compilation{suffix}{server_detail}",
            operation_state=status.state.value,
        )
    return HiveQueryError(
        f"Hive query execution failed{suffix}{server_detail}",
        operation_state=status.state.value,
    )


@dataclass(slots=True)
class HiveOperation:
    """One session-owned operation; every method runs on its owner's I/O thread."""

    rpc: HiveRPC
    handle: OperationHandle
    _closed: bool = False
    _cancel_requested: bool = False
    _last_state: OperationState = OperationState.INITIALIZED

    def wait_until_finished(self, *, deadline: float, poll_interval: float = 0.05) -> None:
        """Poll until results are available or a terminal failure is observed."""

        while True:
            if time.monotonic() >= deadline:
                raise HiveCancellationError(
                    "Hive query deadline expired", operation_state=self._last_state.value
                )
            status = self.rpc.get_operation_status(self.handle)
            self._last_state = status.state
            if status.state is OperationState.FINISHED:
                return
            if status.state is OperationState.CANCELLED:
                raise HiveCancellationError(
                    "Hive query was cancelled", operation_state=status.state.value
                )
            if status.state is OperationState.TIMED_OUT:
                raise HiveQueryError("Hive query timed out", operation_state=status.state.value)
            if status.state in {OperationState.ERROR, OperationState.CLOSED}:
                raise _query_failure(status)
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))

    def get_schema(self) -> HiveResultSchema:
        """Fetch complete metadata only after the operation is ready."""

        return self.rpc.get_result_schema(self.handle)

    def fetch(self, *, max_rows: int) -> HiveRowSet:
        """Fetch the next columnar rowset."""

        if self._closed:
            raise HiveQueryError("Cannot fetch from a closed Hive operation")
        return self.rpc.fetch_results(self.handle, max_rows=max_rows)

    def cancel(self) -> None:
        """Request cancellation at most once; closing remains independently possible."""

        if self._cancel_requested or self._closed:
            return
        self._cancel_requested = True
        self.rpc.cancel_operation(self.handle)

    def close(self) -> None:
        """Close the operation exactly once."""

        if self._closed:
            return
        try:
            self.rpc.close_operation(self.handle)
        except Exception as exc:
            raise HiveCleanupError("Failed to close the Hive operation") from exc
        finally:
            self._closed = True
