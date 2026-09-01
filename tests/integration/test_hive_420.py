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

import os
from decimal import Decimal

import pyarrow as pa
import pytest
import ray

from ray_hive import (
    HiveConfigurationError,
    HiveConnectionOptions,
    HiveReadOptions,
    HiveSchemaError,
    HiveTableIdentifier,
    read_hive,
)
from ray_hive._hs2.client import resolve_connection_factory
from ray_hive._planner import HivePlanner
from ray_hive._worker import iter_hive_blocks
from ray_hive.sql import col

pytestmark = pytest.mark.integration


def _connection() -> HiveConnectionOptions:
    return HiveConnectionOptions(
        host=os.environ.get("RAY_HIVE_HS2_HOST", "127.0.0.1"),
        port=int(os.environ.get("RAY_HIVE_HS2_PORT", "10090")),
        database="ray_hive_it",
        auth="NOSASL",
        session_options={
            "hive.local.time.zone": "UTC",
            "hive.resultset.use.unique.column.names": "false",
        },
        rpc_timeout=30,
    )


def test_native_multifetch_schema_and_primitive_values() -> None:
    read = HiveReadOptions(
        table=HiveTableIdentifier("ray_hive_it", "primitive_values"),
        columns=(
            "id",
            "enabled",
            "label",
            "amount",
            "event_date",
            "event_time",
            "payload",
        ),
        fetch_rows=2,
    )
    spec = HivePlanner(resolve_connection_factory).plan(_connection(), read)[0]

    blocks = list(iter_hive_blocks(spec))
    table = pa.concat_tables(blocks).sort_by([("id", "ascending")])

    assert len(blocks) >= 3
    assert table["id"].to_pylist() == [1, 2, 3, 4, 5]
    assert table["amount"].to_pylist()[0] == Decimal("12.3000")
    assert table["label"].to_pylist()[2] is None
    assert table["event_time"].cast(pa.int64()).to_pylist()[0] == 1_767_229_323_123_456_789
    assert table["payload"].to_pylist()[4] == b"xyz"


def test_external_table_is_read_only_through_hs2() -> None:
    read = HiveReadOptions(
        table=HiveTableIdentifier("ray_hive_it", "external_values"), fetch_rows=1
    )
    spec = HivePlanner(resolve_connection_factory).plan(_connection(), read)[0]

    rows = [row for block in iter_hive_blocks(spec) for row in block.to_pylist()]

    assert rows == [
        {"id": 10, "label": "server-only"},
        {"id": 20, "label": "location"},
    ]
    serialized_plan = repr(spec)
    assert "/opt/hive/data" not in serialized_plan
    assert "LOCATION" not in serialized_plan


def test_table_projection_typed_predicate_and_source_limit() -> None:
    read = HiveReadOptions(
        table=HiveTableIdentifier("ray_hive_it", "primitive_values"),
        columns=("id", "label"),
        filter=(
            col("id").ge(2)
            & col("label").is_not_null()
            & col("label").ne("")
            & col("label").ne("O'Reilly")
            & col("label").ne(r"C:\data")
        ),
        limit=2,
        fetch_rows=1,
    )
    spec = HivePlanner(resolve_connection_factory).plan(_connection(), read)[0]

    rows = [row for block in iter_hive_blocks(spec) for row in block.to_pylist()]

    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 2, "label": "beta"},
        {"id": 5, "label": "omega"},
    ]


def test_char_padding_is_preserved_with_length_metadata() -> None:
    read = HiveReadOptions(
        table=HiveTableIdentifier("ray_hive_it", "char_values"),
        columns=("fixed_label",),
    )
    spec = HivePlanner(resolve_connection_factory).plan(_connection(), read)[0]

    table = pa.concat_tables(list(iter_hive_blocks(spec)))

    assert table["fixed_label"].to_pylist() == ["xy   "]
    assert table.schema.field("fixed_label").metadata[b"hive.length"] == b"5"


def test_local_timezone_fails_closed_or_preserves_explicit_string_fallback() -> None:
    statement = (
        "SELECT CAST(CAST('2026-01-01 01:02:03' AS STRING) "
        "AS TIMESTAMP WITH LOCAL TIME ZONE) AS local_event_time"
    )
    strict_read = HiveReadOptions(
        sql=statement,
        schema=pa.schema([("local_event_time", pa.string())]),
    )
    strict_spec = HivePlanner(resolve_connection_factory).plan(_connection(), strict_read)[0]
    with pytest.raises(HiveSchemaError, match="no verified native decoder"):
        list(iter_hive_blocks(strict_spec))

    fallback_read = HiveReadOptions(
        sql=statement,
        schema=pa.schema([("local_event_time", pa.string())]),
        unsupported_type_mode="string",
    )
    spec = HivePlanner(resolve_connection_factory).plan(_connection(), fallback_read)[0]
    table = pa.concat_tables(list(iter_hive_blocks(spec)))

    value = table["local_event_time"].to_pylist()[0]
    assert isinstance(value, str)
    assert table.schema.field("local_event_time").metadata[b"hive.fallback"] == b"string"


def test_strict_ray_facade_reads_view_without_query_replay() -> None:
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    try:
        dataset = read_hive(
            connection=_connection(),
            read=HiveReadOptions(
                table=HiveTableIdentifier("ray_hive_it", "primitive_view"),
                fetch_rows=1,
            ),
        )
        rows = sorted(dataset.take_all(), key=lambda row: row["id"])
    finally:
        ray.shutdown()

    assert [row["id"] for row in rows] == [2, 3, 4, 5]


def test_query_only_validation_prevents_side_effects_before_raw_select() -> None:
    with pytest.raises(HiveConfigurationError, match="top-level SELECT"):
        HiveReadOptions(sql="DROP TABLE ray_hive_it.primitive_values")
    with pytest.raises(HiveConfigurationError, match="outer parenthesis"):
        HiveReadOptions(
            table="ray_hive_it.primitive_values",
            unsafe_where_sql=") ; DROP TABLE ray_hive_it.primitive_values; --",
        )

    read = HiveReadOptions(
        sql=(
            "SELECT COUNT(*) AS row_count, '; -- /* quoted data' AS marker "
            "FROM ray_hive_it.primitive_values"
        ),
        schema=pa.schema([("row_count", pa.int64()), ("marker", pa.string())]),
    )
    spec = HivePlanner(resolve_connection_factory).plan(_connection(), read)[0]
    table = pa.concat_tables(list(iter_hive_blocks(spec)))

    assert table.to_pylist() == [{"row_count": 5, "marker": "; -- /* quoted data"}]
