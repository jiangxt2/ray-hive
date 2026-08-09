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

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ray_hive import HivePlanningError
from ray_hive._sql import quote_identifier, render_literal
from ray_hive.sql import col


def test_identifier_quoting_escapes_backticks() -> None:
    assert quote_identifier("a`b") == "`a``b`"
    with pytest.raises(HivePlanningError):
        quote_identifier("")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        (42, "42"),
        (1.25, "1.25"),
        (Decimal("12.30"), "DECIMAL '12.30'"),
        ("O'Reilly", "'O''Reilly'"),
        (b"\x00\xff", "X'00FF'"),
        (date(2026, 8, 9), "DATE '2026-08-09'"),
        (
            datetime(2026, 8, 9, 1, 2, 3, 4000),
            "TIMESTAMP '2026-08-09 01:02:03.004000'",
        ),
    ],
)
def test_literal_rendering(value: object, expected: str) -> None:
    assert render_literal(value) == expected


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), Decimal("NaN"), 2**63, datetime.now(UTC)],
)
def test_literal_rendering_rejects_ambiguous_values(value: object) -> None:
    with pytest.raises(HivePlanningError):
        render_literal(value)


def test_complete_predicate_tree_renders_without_stringifying_unknown_objects() -> None:
    predicate = (
        col("score").ge(Decimal("0.5"))
        & col("kind").isin(["open", "close"])
        & ~col("deleted_at").is_not_null()
    )

    assert predicate.to_sql() == (
        "((`score` >= DECIMAL '0.5') AND (`kind` IN ('open', 'close'))) "
        "AND (NOT (`deleted_at` IS NOT NULL))"
    )


def test_null_and_empty_in_require_explicit_semantics() -> None:
    with pytest.raises(HivePlanningError, match="NULL comparisons"):
        col("value").eq(None)
    with pytest.raises(HivePlanningError, match="at least one"):
        col("value").isin([])
    with pytest.raises(HivePlanningError, match="cannot contain NULL"):
        col("value").isin([1, None])


@pytest.mark.parametrize("method", ["__and__", "__or__"])
def test_logical_composition_rejects_non_predicate_operands(method: str) -> None:
    predicate = col("value").eq(1)

    with pytest.raises(HivePlanningError, match="Predicate"):
        getattr(predicate, method)("not-a-predicate")
