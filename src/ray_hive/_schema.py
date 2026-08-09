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

"""TTypeDesc-derived Arrow schema mapping and drift detection."""

from __future__ import annotations

import hashlib

import pyarrow as pa

from ray_hive._errors import HiveSchemaError
from ray_hive._hs2.protocol import HiveResultSchema, HiveType, HiveTypeKind
from ray_hive._models import UnsupportedTypeMode

_PRIMITIVE_ARROW_TYPES: dict[HiveTypeKind, pa.DataType] = {
    HiveTypeKind.BOOLEAN: pa.bool_(),
    HiveTypeKind.TINYINT: pa.int8(),
    HiveTypeKind.SMALLINT: pa.int16(),
    HiveTypeKind.INT: pa.int32(),
    HiveTypeKind.BIGINT: pa.int64(),
    HiveTypeKind.FLOAT: pa.float32(),
    HiveTypeKind.DOUBLE: pa.float64(),
    HiveTypeKind.STRING: pa.string(),
    HiveTypeKind.BINARY: pa.binary(),
    HiveTypeKind.DATE: pa.date32(),
    HiveTypeKind.TIMESTAMP: pa.timestamp("ns"),
    HiveTypeKind.NULL: pa.null(),
}


def _type_label(hive_type: HiveType) -> str:
    if hive_type.kind is HiveTypeKind.DECIMAL:
        return f"DECIMAL({hive_type.precision},{hive_type.scale})"
    if hive_type.kind in {HiveTypeKind.CHAR, HiveTypeKind.VARCHAR}:
        return f"{hive_type.kind.value}({hive_type.length})"
    return hive_type.kind.value


def hive_type_to_arrow(
    hive_type: HiveType,
    *,
    unsupported_type_mode: UnsupportedTypeMode = UnsupportedTypeMode.ERROR,
) -> tuple[pa.DataType, dict[bytes, bytes]]:
    """Map one complete Hive type to Arrow without silent loss."""

    metadata = {b"hive.type": _type_label(hive_type).encode()}
    if hive_type.kind in _PRIMITIVE_ARROW_TYPES:
        return _PRIMITIVE_ARROW_TYPES[hive_type.kind], metadata
    if hive_type.kind is HiveTypeKind.DECIMAL:
        if hive_type.precision is None or hive_type.scale is None:
            raise HiveSchemaError("Hive DECIMAL metadata is missing precision or scale")
        if not 1 <= hive_type.precision <= 38:
            raise HiveSchemaError("Hive DECIMAL precision must be between 1 and 38")
        if not 0 <= hive_type.scale <= hive_type.precision:
            raise HiveSchemaError("Hive DECIMAL scale is outside its precision")
        return pa.decimal128(hive_type.precision, hive_type.scale), metadata
    if hive_type.kind in {HiveTypeKind.CHAR, HiveTypeKind.VARCHAR}:
        if hive_type.length is None or hive_type.length <= 0:
            raise HiveSchemaError(f"Hive {hive_type.kind.value} is missing a positive length")
        metadata[b"hive.length"] = str(hive_type.length).encode()
        return pa.string(), metadata
    if unsupported_type_mode is UnsupportedTypeMode.STRING:
        metadata[b"hive.fallback"] = b"string"
        return pa.string(), metadata
    raise HiveSchemaError(f"Hive type {_type_label(hive_type)!r} has no verified native decoder")


def result_schema_to_arrow(
    schema: HiveResultSchema,
    *,
    unsupported_type_mode: UnsupportedTypeMode = UnsupportedTypeMode.ERROR,
) -> pa.Schema:
    """Convert ordered HS2 result metadata into a canonical Arrow schema."""

    if [column.position for column in schema.columns] != list(range(1, len(schema.columns) + 1)):
        raise HiveSchemaError("Hive result metadata positions are not contiguous and one-based")
    fields = []
    for column in schema.columns:
        arrow_type, metadata = hive_type_to_arrow(
            column.type, unsupported_type_mode=unsupported_type_mode
        )
        if column.comment is not None:
            metadata[b"hive.comment"] = column.comment.encode()
        fields.append(pa.field(column.name, arrow_type, nullable=True, metadata=metadata))
    return pa.schema(fields, metadata={b"ray_hive.schema": b"hs2-ttypedesc-v1"})


def schema_fingerprint(schema: HiveResultSchema) -> str:
    """Fingerprint every drift-relevant field of a normalized Hive schema."""

    def encode_type(hive_type: HiveType) -> str:
        children = ",".join(encode_type(child) for child in hive_type.children)
        names = ",".join(hive_type.field_names)
        return (
            f"{hive_type.kind.value}:{hive_type.precision}:{hive_type.scale}:"
            f"{hive_type.length}:[{children}]:[{names}]:{hive_type.user_defined_name}"
        )

    canonical = "\n".join(
        f"{column.position}\0{column.name}\0{encode_type(column.type)}" for column in schema.columns
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def require_same_schema(expected: HiveResultSchema, actual: HiveResultSchema) -> None:
    """Fail before output when data-operation metadata drifted after planning."""

    if schema_fingerprint(expected) != schema_fingerprint(actual):
        raise HiveSchemaError("Hive result schema changed between planning and execution")
