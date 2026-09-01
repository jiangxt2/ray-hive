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

import ray_hive.sql as sql_api
from ray_hive import HivePlanningError
from ray_hive._sql import (
    quote_identifier,
    render_literal,
    validate_raw_select,
    validate_unsafe_where,
)
from ray_hive.sql import col


def test_structured_predicate_builder_has_a_stable_public_surface() -> None:
    assert sql_api.__all__ == ["Column", "Predicate", "col"]
    assert "Stable built-in" in (sql_api.__doc__ or "")


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
        ("O'Reilly", "'O\\'Reilly'"),
        (r"C:\data\file", r"'C:\\data\\file'"),
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


def test_identifier_and_literal_reject_preparse_variable_substitution() -> None:
    with pytest.raises(HivePlanningError, match="variable substitution"):
        quote_identifier("${env:TABLE}")
    with pytest.raises(HivePlanningError, match="variable substitution"):
        render_literal("${hiveconf:FILTER}")
    with pytest.raises(HivePlanningError, match="NUL"):
        render_literal("value\x00suffix")
    with pytest.raises(HivePlanningError, match="variable substitution"):
        col("value").eq("${env:FILTER}")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "  SeLeCt (1 + (2))  ",
        "SELECT '; DROP TABLE hidden' AS value",
        'SELECT "-- not a comment" AS value',
        "SELECT `semi;--/*` FROM `db`.`table`",
        r"SELECT 'escaped\' quote; -- still literal'",
        "SELECT 'adjacent''; DROP remains quoted'",
    ],
)
def test_raw_select_validator_accepts_one_conservative_select(sql: str) -> None:
    assert validate_raw_select(sql) == sql.strip()


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "WITH source AS (SELECT 1) SELECT * FROM source",
        "EXPLAIN SELECT 1",
        "SHOW TABLES",
        "INSERT INTO target SELECT 1",
        "SELECTED 1",
        "SELECTé 1",
        "SELECT 1;",
        "SELECT 1; DROP TABLE target",
        "SELECT /* hidden */ 1",
        "SELECT -- hidden\n1",
        "SELECT */ 1",
        "SELECT (1",
        "SELECT 1)",
        "SELECT 'unterminated",
        'SELECT "unterminated',
        "SELECT `unterminated",
        "SELECT 'incomplete\\",
        "SELECT '\x00'",
        "SELECT '${env:QUERY}'",
    ],
)
def test_raw_select_validator_rejects_ambiguous_or_multi_statement_sql(sql: str) -> None:
    with pytest.raises(HivePlanningError):
        validate_raw_select(sql)


@pytest.mark.parametrize(
    "fragment",
    [
        "id = 1",
        "(id = 1 OR id = 2)",
        "label = '; -- /* quoted'",
        r"label = 'escaped\' quote; -- quoted'",
        "`odd);--name` = 1",
    ],
)
def test_unsafe_where_validator_confines_one_predicate(fragment: str) -> None:
    assert validate_unsafe_where(fragment) == fragment.strip()


@pytest.mark.parametrize(
    "fragment",
    [
        "",
        "id = 1; DROP TABLE target",
        "id = 1 -- bypass",
        "id = 1 /* bypass */",
        ") OR TRUE OR (",
        "(id = 1",
        "id = '${env:FILTER}'",
        "id = '\x00'",
    ],
)
def test_unsafe_where_validator_rejects_renderer_escape(fragment: str) -> None:
    with pytest.raises(HivePlanningError):
        validate_unsafe_where(fragment)


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
