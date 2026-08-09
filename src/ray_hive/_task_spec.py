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

"""Immutable, serializable boundary between driver planning and Ray workers."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from ray_hive._hs2.protocol import HiveResultSchema
from ray_hive._models import HiveConnectionOptions, HiveConsistency, UnsupportedTypeMode


@dataclass(frozen=True, slots=True)
class HiveTaskSpec:
    """A cleaned physical query plan containing no live handles or plaintext secrets."""

    connection: HiveConnectionOptions
    statement: str = field(repr=False)
    expected_arrow_schema: pa.Schema
    expected_hive_schema: HiveResultSchema | None
    fetch_rows: int
    target_batch_bytes: int
    query_timeout: float
    consistency: HiveConsistency
    unsupported_type_mode: UnsupportedTypeMode
    split_index: int | None = None

    def __repr__(self) -> str:
        return (
            f"HiveTaskSpec(connection={self.connection!r}, statement=<redacted>, "
            f"columns={self.expected_arrow_schema.names!r}, "
            f"consistency={self.consistency.value!r}, "
            f"split_index={self.split_index!r})"
        )
