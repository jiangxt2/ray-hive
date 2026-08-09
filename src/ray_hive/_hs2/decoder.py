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

"""Schema-aware V6 columnar result decoding."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa

from ray_hive._errors import HiveProtocolError, HiveSchemaError
from ray_hive._hs2.protocol import HiveResultSchema, HiveRowSet, HiveTypeKind


def decode_null_bitmap(nulls: bytes, row_count: int) -> list[bool]:
    """Decode Hive's little-endian-per-byte NULL bitmap into an Arrow mask."""

    if row_count < 0:
        raise HiveProtocolError("Hive row count cannot be negative")
    return [
        index // 8 < len(nulls) and bool(nulls[index // 8] & (1 << (index % 8)))
        for index in range(row_count)
    ]


def _decode_values(
    values: tuple[object, ...],
    *,
    hive_kind: HiveTypeKind,
    arrow_type: pa.DataType,
    mask: list[bool],
) -> pa.Array[Any]:
    converted: list[object]
    if hive_kind is HiveTypeKind.DECIMAL:
        try:
            converted = [
                Decimal(str(value)) if not mask[index] else None
                for index, value in enumerate(values)
            ]
        except InvalidOperation as exc:
            raise HiveSchemaError("Hive returned an invalid DECIMAL value") from exc
    else:
        converted = [None if mask[index] else value for index, value in enumerate(values)]

    try:
        if hive_kind in {HiveTypeKind.TIMESTAMP, HiveTypeKind.TIMESTAMP_LOCAL_TZ}:
            timestamp_values = [
                None if mask[index] else str(value) for index, value in enumerate(values)
            ]
            return pa.array(timestamp_values, type=pa.string()).cast(arrow_type)
        if hive_kind is HiveTypeKind.DATE:
            date_values = [
                None if mask[index] else str(value) for index, value in enumerate(values)
            ]
            return pa.array(date_values, type=pa.string()).cast(arrow_type)
        return pa.array(converted, type=arrow_type)
    except (pa.ArrowException, OverflowError, TypeError, ValueError) as exc:
        raise HiveSchemaError(f"Hive value cannot be represented as {arrow_type}") from exc


def decode_rowset(
    rowset: HiveRowSet,
    hive_schema: HiveResultSchema,
    arrow_schema: pa.Schema,
) -> pa.RecordBatch:
    """Decode one V6 TRowSet into one Arrow RecordBatch without row materialization."""

    if rowset.binary_columns is not None:
        raise HiveProtocolError("V9 binaryColumns requires the separately negotiated decoder")
    if len(hive_schema.columns) != len(arrow_schema) or len(rowset.columns) != len(arrow_schema):
        raise HiveProtocolError("Hive rowset column count does not match result metadata")
    row_counts = {len(column.values) for column in rowset.columns}
    if len(row_counts) > 1:
        raise HiveProtocolError("Hive rowset column vectors have different lengths")
    row_count = row_counts.pop() if row_counts else 0

    arrays = []
    for index, (vector, hive_column, arrow_field) in enumerate(
        zip(rowset.columns, hive_schema.columns, arrow_schema, strict=True)
    ):
        if hive_column.name != arrow_field.name:
            raise HiveSchemaError(f"Hive column name changed at ordinal {index + 1}")
        mask = decode_null_bitmap(vector.nulls, row_count)
        arrays.append(
            _decode_values(
                vector.values,
                hive_kind=hive_column.type.kind,
                arrow_type=arrow_field.type,
                mask=mask,
            )
        )
    return pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)


def split_batch(batch: pa.RecordBatch, target_bytes: int) -> tuple[pa.RecordBatch, ...]:
    """Slice one fetched batch to a soft Arrow byte target without copying buffers."""

    if target_bytes <= 0:
        raise HiveProtocolError("Arrow target batch bytes must be positive")
    if batch.num_rows <= 1 or batch.nbytes <= target_bytes:
        return (batch,)
    rows_per_slice = max(1, batch.num_rows * target_bytes // batch.nbytes)
    return tuple(
        batch.slice(offset, min(rows_per_slice, batch.num_rows - offset))
        for offset in range(0, batch.num_rows, rows_per_slice)
    )
