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

from decimal import Decimal

import pyarrow as pa
import pytest

from ray_hive import HiveProtocolError, HiveSchemaError
from ray_hive._hs2.decoder import decode_null_bitmap, decode_rowset, split_batch
from ray_hive._hs2.protocol import (
    HiveColumnSchema,
    HiveColumnVector,
    HiveResultSchema,
    HiveRowSet,
    HiveType,
    HiveTypeKind,
)
from ray_hive._models import UnsupportedTypeMode
from ray_hive._schema import (
    hive_type_to_arrow,
    require_same_schema,
    result_schema_to_arrow,
    schema_fingerprint,
)


def _schema(*types: tuple[str, HiveType]) -> HiveResultSchema:
    return HiveResultSchema(
        tuple(
            HiveColumnSchema(name, hive_type, position=index)
            for index, (name, hive_type) in enumerate(types, start=1)
        )
    )


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (HiveTypeKind.BOOLEAN, pa.bool_()),
        (HiveTypeKind.TINYINT, pa.int8()),
        (HiveTypeKind.SMALLINT, pa.int16()),
        (HiveTypeKind.INT, pa.int32()),
        (HiveTypeKind.BIGINT, pa.int64()),
        (HiveTypeKind.FLOAT, pa.float32()),
        (HiveTypeKind.DOUBLE, pa.float64()),
        (HiveTypeKind.STRING, pa.string()),
        (HiveTypeKind.BINARY, pa.binary()),
        (HiveTypeKind.DATE, pa.date32()),
        (HiveTypeKind.TIMESTAMP, pa.timestamp("ns")),
    ],
)
def test_primitive_type_mapping(kind: HiveTypeKind, expected: pa.DataType) -> None:
    actual, metadata = hive_type_to_arrow(HiveType(kind))
    assert actual == expected
    assert metadata[b"hive.type"]


def test_qualified_types_preserve_metadata() -> None:
    decimal, decimal_metadata = hive_type_to_arrow(
        HiveType(HiveTypeKind.DECIMAL, precision=18, scale=4)
    )
    char, char_metadata = hive_type_to_arrow(HiveType(HiveTypeKind.CHAR, length=12))

    assert decimal == pa.decimal128(18, 4)
    assert decimal_metadata[b"hive.type"] == b"DECIMAL(18,4)"
    assert char == pa.string()
    assert char_metadata[b"hive.length"] == b"12"


@pytest.mark.parametrize(
    "hive_type",
    [
        HiveType(HiveTypeKind.DECIMAL),
        HiveType(HiveTypeKind.DECIMAL, precision=39, scale=0),
        HiveType(HiveTypeKind.CHAR),
        HiveType(HiveTypeKind.ARRAY, children=(HiveType(HiveTypeKind.INT),)),
        HiveType(HiveTypeKind.TIMESTAMP_LOCAL_TZ),
    ],
)
def test_unverified_or_invalid_types_fail_closed(hive_type: HiveType) -> None:
    with pytest.raises(HiveSchemaError):
        hive_type_to_arrow(hive_type)


def test_explicit_string_fallback_is_marked() -> None:
    actual, metadata = hive_type_to_arrow(
        HiveType(HiveTypeKind.ARRAY, children=(HiveType(HiveTypeKind.INT),)),
        unsupported_type_mode=UnsupportedTypeMode.STRING,
    )
    assert actual == pa.string()
    assert metadata[b"hive.fallback"] == b"string"
    assert metadata[b"hive.type"] == b"ARRAY"

    local_tz, local_tz_metadata = hive_type_to_arrow(
        HiveType(HiveTypeKind.TIMESTAMP_LOCAL_TZ),
        unsupported_type_mode=UnsupportedTypeMode.STRING,
    )
    assert local_tz == pa.string()
    assert local_tz_metadata[b"hive.fallback"] == b"string"


def test_result_schema_requires_contiguous_positions() -> None:
    schema = HiveResultSchema((HiveColumnSchema("id", HiveType(HiveTypeKind.INT), position=0),))
    with pytest.raises(HiveSchemaError, match="one-based"):
        result_schema_to_arrow(schema)


def test_schema_fingerprint_covers_type_qualifiers_and_nested_names() -> None:
    first = _schema(("value", HiveType(HiveTypeKind.DECIMAL, precision=18, scale=2)))
    second = _schema(("value", HiveType(HiveTypeKind.DECIMAL, precision=18, scale=3)))

    assert schema_fingerprint(first) != schema_fingerprint(second)
    with pytest.raises(HiveSchemaError, match="changed"):
        require_same_schema(first, second)


def test_null_bitmap_uses_hive_little_endian_bit_order() -> None:
    mask = decode_null_bitmap(bytes([0b10000001, 0b00000001]), 10)
    assert mask == [True, False, False, False, False, False, False, True, True, False]


def test_short_null_bitmap_implies_non_null_high_rows() -> None:
    assert decode_null_bitmap(b"\x00", 20) == [False] * 20


def test_v6_rowset_decodes_primitive_and_qualified_values() -> None:
    hive_schema = _schema(
        ("id", HiveType(HiveTypeKind.INT)),
        ("amount", HiveType(HiveTypeKind.DECIMAL, precision=10, scale=2)),
        ("day", HiveType(HiveTypeKind.DATE)),
        ("created_at", HiveType(HiveTypeKind.TIMESTAMP)),
    )
    arrow_schema = result_schema_to_arrow(hive_schema)
    rowset = HiveRowSet(
        start_row_offset=0,
        columns=(
            HiveColumnVector((1, 0), b"\x02"),
            HiveColumnVector(("12.30", "0"), b"\x02"),
            HiveColumnVector(("2026-08-09", ""), b"\x02"),
            HiveColumnVector(("2026-08-09 01:02:03.123456789", ""), b"\x02"),
        ),
        server_has_more_rows=False,
    )

    batch = decode_rowset(rowset, hive_schema, arrow_schema)

    assert batch.num_rows == 2
    assert batch.column(0).to_pylist() == [1, None]
    assert batch.column(1).to_pylist() == [Decimal("12.30"), None]
    assert batch.column(2).to_pylist()[0].isoformat() == "2026-08-09"
    assert batch.column(3).cast(pa.int64()).to_pylist()[0] == 1_786_237_323_123_456_789


def test_rowset_rejects_column_mismatch_and_binary_encoding() -> None:
    hive_schema = _schema(("id", HiveType(HiveTypeKind.INT)))
    arrow_schema = result_schema_to_arrow(hive_schema)
    with pytest.raises(HiveProtocolError, match="column count"):
        decode_rowset(HiveRowSet(0, (), False), hive_schema, arrow_schema)
    with pytest.raises(HiveProtocolError, match="binaryColumns"):
        decode_rowset(
            HiveRowSet(0, (), False, binary_columns=b"payload", binary_column_count=1),
            hive_schema,
            arrow_schema,
        )


def test_batch_splitting_preserves_all_rows() -> None:
    batch = pa.RecordBatch.from_pydict({"value": ["x" * 100] * 10})
    slices = split_batch(batch, target_bytes=200)
    assert len(slices) > 1
    assert sum(item.num_rows for item in slices) == 10
    assert pa.Table.from_batches(slices).column("value").to_pylist() == ["x" * 100] * 10
