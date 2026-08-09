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

"""Ray worker execution with one task-owned, serial I/O thread."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from ray_hive._errors import (
    HiveCancellationError,
    HiveCleanupError,
    HiveError,
    HiveProtocolError,
    HiveSchemaError,
    HiveTransportError,
)
from ray_hive._hs2.client import resolve_connection_factory
from ray_hive._hs2.decoder import decode_rowset, split_batch
from ray_hive._hs2.operation import HiveOperation
from ray_hive._hs2.protocol import AbortHandle, HiveResultSchema
from ray_hive._hs2.session import HiveSession
from ray_hive._schema import require_same_schema, result_schema_to_arrow
from ray_hive._task_spec import HiveTaskSpec


@dataclass(slots=True)
class _AbortState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    handle: AbortHandle | None = None
    requested: bool = False

    def publish(self, handle: AbortHandle) -> None:
        with self.lock:
            self.handle = handle
            requested = self.requested
        if requested:
            handle.abort()

    def abort(self) -> None:
        with self.lock:
            self.requested = True
            handle = self.handle
        if handle is not None:
            handle.abort()


@dataclass(slots=True)
class _ExecutionStream:
    spec: HiveTaskSpec
    session: HiveSession
    operation: HiveOperation
    actual_hive_schema: HiveResultSchema
    output_schema: pa.Schema
    _pending: Iterator[pa.RecordBatch] = field(default_factory=lambda: iter(()))
    _source_exhausted: bool = False
    _closed: bool = False

    @classmethod
    def open(
        cls,
        spec: HiveTaskSpec,
        publish_abort: Callable[[AbortHandle], None],
    ) -> _ExecutionStream:
        factory = resolve_connection_factory(str(spec.connection.driver))
        rpc = factory.connect(spec.connection)
        publish_abort(rpc.abort_handle)
        session: HiveSession | None = None
        operation: HiveOperation | None = None
        try:
            session = HiveSession.open(rpc, spec.connection)
            operation = session.execute(
                spec.statement,
                timeout_seconds=math.ceil(spec.query_timeout),
            )
            operation.wait_until_finished(deadline=time.monotonic() + spec.query_timeout)
            actual_hive_schema = operation.get_schema()
            actual_arrow_schema = result_schema_to_arrow(
                actual_hive_schema,
                unsupported_type_mode=spec.unsupported_type_mode,
            )
            if spec.expected_hive_schema is not None:
                require_same_schema(spec.expected_hive_schema, actual_hive_schema)
                output_schema = spec.expected_arrow_schema
            else:
                if not actual_arrow_schema.equals(spec.expected_arrow_schema, check_metadata=False):
                    raise HiveSchemaError(
                        "Hive raw SQL schema does not match the explicit Arrow schema"
                    )
                output_schema = actual_arrow_schema
            return cls(spec, session, operation, actual_hive_schema, output_schema)
        except Exception as primary_error:
            if operation is not None:
                if isinstance(primary_error, HiveCancellationError):
                    try:
                        operation.cancel()
                    except Exception as cleanup_error:
                        primary_error.add_note(
                            "Operation cancellation also failed during worker initialization: "
                            f"{type(cleanup_error).__name__}"
                        )
                try:
                    operation.close()
                except Exception as cleanup_error:
                    primary_error.add_note(
                        "Operation cleanup also failed during worker initialization: "
                        f"{type(cleanup_error).__name__}"
                    )
            if session is not None:
                try:
                    session.close()
                except Exception as cleanup_error:
                    primary_error.add_note(
                        "Session cleanup also failed during worker initialization: "
                        f"{type(cleanup_error).__name__}"
                    )
            raise

    def next_block(self) -> pa.RecordBatch | None:
        """Return one block while keeping at most one fetched rowset materialized."""

        try:
            return next(self._pending)
        except StopIteration:
            pass
        if self._source_exhausted:
            return None

        try:
            rowset = self.operation.fetch(max_rows=self.spec.fetch_rows)
        except HiveError:
            raise
        except Exception as exc:
            raise HiveTransportError("Hive FetchResults transport failed") from exc
        if rowset.binary_columns is not None:
            raise HiveProtocolError("Hive binary column rowsets are not supported")
        if len(rowset.columns) != len(self.actual_hive_schema.columns):
            raise HiveProtocolError("Hive rowset column count does not match result metadata")
        row_counts = {len(column.values) for column in rowset.columns}
        if len(row_counts) > 1:
            raise HiveProtocolError("Hive rowset column vectors have different lengths")
        row_count = next(iter(row_counts), 0)
        if row_count == 0:
            if rowset.server_has_more_rows:
                raise HiveProtocolError("Hive returned an empty rowset while reporting more rows")
            self._source_exhausted = True
            return None

        batch = decode_rowset(rowset, self.actual_hive_schema, self.output_schema)
        self._pending = iter(split_batch(batch, self.spec.target_batch_bytes))
        return next(self._pending)

    def close(self, *, cancel: bool) -> None:
        """Cancel if needed, then close operation, session, and transport in order."""

        if self._closed:
            return
        errors: list[Exception] = []
        if cancel and not self._source_exhausted:
            try:
                self.operation.cancel()
            except Exception as exc:
                errors.append(exc)
        try:
            self.operation.close()
        except Exception as exc:
            errors.append(exc)
        try:
            self.session.close()
        except Exception as exc:
            errors.append(exc)
        self._closed = True
        if errors:
            raise HiveCleanupError("Hive worker resource cleanup failed") from errors[0]


def _close_abandoned_stream(future: Future[_ExecutionStream]) -> None:
    """Close a stream that finished opening after its caller stopped waiting."""

    try:
        stream = future.result()
    except BaseException:
        return
    if not isinstance(stream, _ExecutionStream):
        return
    try:
        stream.close(cancel=True)
    except Exception:
        return


def _future_result[T](
    future: Future[T],
    *,
    timeout: float,
    abort_state: _AbortState,
    timeout_message: str,
) -> T:
    try:
        return future.result(timeout=max(0.001, timeout))
    except FutureTimeout as exc:
        abort_state.abort()
        raise HiveCancellationError(timeout_message) from exc


def iter_hive_blocks(spec: HiveTaskSpec) -> Iterator[pa.Table]:
    """Execute one query attempt and stream multiple blocks without replay."""

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ray-hive-io")
    abort_state = _AbortState()
    stream: _ExecutionStream | None = None
    opened: Future[_ExecutionStream] | None = None
    active_future: Future[Any] | None = None
    submitted_futures: list[Future[Any]] = []
    completed = False
    published_blocks = 0
    primary_error: BaseException | None = None
    try:
        opened = executor.submit(_ExecutionStream.open, spec, abort_state.publish)
        active_future = opened
        submitted_futures.append(opened)
        open_timeout = (
            spec.connection.connect_timeout + spec.query_timeout + 4 * spec.connection.rpc_timeout
        )
        opened_stream = _future_result(
            opened,
            timeout=open_timeout,
            abort_state=abort_state,
            timeout_message="Hive worker initialization deadline expired",
        )
        active_future = None
        if not isinstance(opened_stream, _ExecutionStream):
            raise HiveTransportError("Hive worker failed to initialize its result stream")
        stream = opened_stream

        while True:
            future = executor.submit(stream.next_block)
            active_future = future
            submitted_futures.append(future)
            block = _future_result(
                future,
                timeout=spec.connection.rpc_timeout,
                abort_state=abort_state,
                timeout_message="Hive FetchResults RPC deadline expired",
            )
            active_future = None
            if block is None:
                completed = True
                return
            if not isinstance(block, pa.RecordBatch):
                raise HiveTransportError("Hive worker produced an invalid block type")
            published_blocks += 1
            yield pa.Table.from_batches([block])
    except (GeneratorExit, KeyboardInterrupt) as exc:
        primary_error = exc
        if active_future is not None and not active_future.done():
            abort_state.abort()
        raise
    except HiveError as exc:
        if published_blocks:
            exc.mark_partial_output()
        primary_error = exc
        raise
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: Exception | None = None
        if stream is None and opened is not None:
            if opened.done():
                try:
                    abandoned_stream = opened.result()
                except BaseException:
                    abandoned_stream = None
                if isinstance(abandoned_stream, _ExecutionStream):
                    stream = abandoned_stream
            else:
                opened.add_done_callback(_close_abandoned_stream)
        if stream is not None:
            close_future = executor.submit(stream.close, cancel=not completed)
            submitted_futures.append(close_future)
            try:
                close_future.result(timeout=max(1.0, spec.connection.rpc_timeout * 3))
            except FutureTimeout as exc:
                abort_state.abort()
                cleanup_error = HiveCleanupError("Hive worker cleanup deadline expired")
                cleanup_error.__cause__ = exc
            except Exception as exc:
                abort_state.abort()
                if isinstance(exc, HiveError):
                    cleanup_error = exc
                else:
                    cleanup_error = HiveCleanupError("Hive worker cleanup failed")
                    cleanup_error.__cause__ = exc
        wait_for_executor = all(future.done() for future in submitted_futures)
        executor.shutdown(wait=wait_for_executor, cancel_futures=False)
        if cleanup_error is not None:
            if published_blocks and isinstance(cleanup_error, HiveError):
                cleanup_error.mark_partial_output()
            if primary_error is not None:
                primary_error.add_note(f"Cleanup also failed: {type(cleanup_error).__name__}")
            else:
                raise cleanup_error
