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

"""Driver-side HS2-only logical and physical read planning."""

from __future__ import annotations

import math
import time
from collections.abc import Callable

import pyarrow as pa

from ray_hive._errors import HiveCancellationError, HiveCleanupError, HivePlanningError
from ray_hive._hs2.protocol import HiveConnectionFactory, HiveResultSchema
from ray_hive._hs2.session import HiveSession
from ray_hive._models import (
    HiveConnectionOptions,
    HiveConsistency,
    HiveReadOptions,
    HiveTableIdentifier,
)
from ray_hive._schema import result_schema_to_arrow
from ray_hive._sql import Predicate, quote_identifier
from ray_hive._task_spec import HiveTaskSpec


def render_table_query(
    read: HiveReadOptions,
    *,
    split_predicate: Predicate | None = None,
    schema_only: bool = False,
) -> str:
    """Compile a validated table read into one Hive statement."""

    if not isinstance(read.table, HiveTableIdentifier):
        raise HivePlanningError("Table query rendering requires a table identifier")
    projection = "*" if read.columns is None else ", ".join(map(quote_identifier, read.columns))
    table = f"{quote_identifier(read.table.database)}.{quote_identifier(read.table.table)}"
    predicates = []
    if read.filter is not None:
        predicates.append(read.filter.to_sql())
    if split_predicate is not None:
        predicates.append(split_predicate.to_sql())
    if read.unsafe_where_sql is not None:
        predicates.append(f"({read.unsafe_where_sql})")
    statement = f"SELECT {projection} FROM {table}"
    if predicates:
        statement += " WHERE " + " AND ".join(f"({item})" for item in predicates)
    limit = 0 if schema_only else read.limit
    if limit is not None:
        statement += f" LIMIT {limit}"
    return statement


class HivePlanner:
    """Create immutable task specs using HS2 only, then close all driver resources."""

    def __init__(self, factory_resolver: Callable[[str], HiveConnectionFactory]) -> None:
        self._factory_resolver = factory_resolver

    def _plan_table_schema(
        self,
        connection: HiveConnectionOptions,
        read: HiveReadOptions,
    ) -> tuple[HiveResultSchema, pa.Schema]:
        factory = self._factory_resolver(str(connection.driver))
        rpc = factory.connect(connection)
        session: HiveSession | None = None
        operation = None
        primary_error: BaseException | None = None
        try:
            session = HiveSession.open(rpc, connection)
            operation = session.execute(
                render_table_query(read, schema_only=True),
                timeout_seconds=math.ceil(read.query_timeout),
            )
            operation.wait_until_finished(deadline=time.monotonic() + read.query_timeout)
            hive_schema = operation.get_schema()
            arrow_schema = result_schema_to_arrow(
                hive_schema,
                unsupported_type_mode=read.normalized_unsupported_type_mode,
            )
            return hive_schema, arrow_schema
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            cleanup_errors: list[Exception] = []
            if operation is not None:
                if isinstance(primary_error, HiveCancellationError):
                    try:
                        operation.cancel()
                    except Exception as exc:
                        cleanup_errors.append(exc)
                try:
                    operation.close()
                except Exception as exc:
                    cleanup_errors.append(exc)
            if session is not None:
                try:
                    session.close()
                except Exception as exc:
                    cleanup_errors.append(exc)
            if cleanup_errors:
                if primary_error is not None:
                    primary_error.add_note(
                        f"Planning cleanup also failed: {type(cleanup_errors[0]).__name__}"
                    )
                else:
                    raise HiveCleanupError("Hive planning resource cleanup failed") from (
                        cleanup_errors[0]
                    )

    def plan(
        self,
        connection: HiveConnectionOptions,
        read: HiveReadOptions,
    ) -> tuple[HiveTaskSpec, ...]:
        """Return one strict task or explicit experimental independent task specs."""

        if read.sql is not None:
            if read.schema is None:
                raise HivePlanningError(
                    "Raw SQL requires an explicit Arrow schema in the initial release"
                )
            return (
                HiveTaskSpec(
                    connection=connection,
                    statement=read.sql,
                    expected_arrow_schema=read.schema,
                    expected_hive_schema=None,
                    fetch_rows=read.fetch_rows,
                    target_batch_bytes=read.target_batch_bytes,
                    query_timeout=read.query_timeout,
                    consistency=read.normalized_consistency,
                    unsupported_type_mode=read.normalized_unsupported_type_mode,
                ),
            )

        hive_schema, arrow_schema = self._plan_table_schema(connection, read)
        predicates: tuple[Predicate | None, ...]
        if read.normalized_consistency is HiveConsistency.STRICT:
            predicates = (None,)
        else:
            predicates = tuple(read.split_predicates)
        return tuple(
            HiveTaskSpec(
                connection=connection,
                statement=render_table_query(read, split_predicate=predicate),
                expected_arrow_schema=arrow_schema,
                expected_hive_schema=hive_schema,
                fetch_rows=read.fetch_rows,
                target_batch_bytes=read.target_batch_bytes,
                query_timeout=read.query_timeout,
                consistency=read.normalized_consistency,
                unsupported_type_mode=read.normalized_unsupported_type_mode,
                split_index=index if predicate is not None else None,
            )
            for index, predicate in enumerate(predicates)
        )
