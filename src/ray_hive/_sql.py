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

"""Safe Hive identifier, literal, and predicate rendering."""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from ray_hive._errors import HivePlanningError

type HiveLiteral = None | bool | int | float | Decimal | str | bytes | date | datetime


def quote_identifier(identifier: str) -> str:
    """Quote exactly one Hive identifier segment."""

    if not identifier or "\x00" in identifier:
        raise HivePlanningError("Hive identifiers must be non-empty and contain no NUL")
    return f"`{identifier.replace('`', '``')}`"


def render_literal(value: HiveLiteral) -> str:
    """Render a supported Python value as a Hive SQL literal."""

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise HivePlanningError("Integer literal is outside Hive BIGINT range")
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HivePlanningError("Non-finite floating-point literals are unsupported")
        return repr(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise HivePlanningError("Non-finite decimal literals are unsupported")
        return f"DECIMAL '{value}'"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise HivePlanningError(
                "Timezone-aware datetime literals require an explicit SQL policy"
            )
        return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, bytes):
        return f"X'{value.hex().upper()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise HivePlanningError(f"Unsupported Hive literal type: {type(value).__name__}")


class Predicate(ABC):
    """A complete predicate tree that can be rendered without a residual."""

    @abstractmethod
    def to_sql(self) -> str:
        """Render this complete predicate as safe Hive SQL."""

    def __and__(self, other: Predicate) -> Predicate:
        if not isinstance(other, Predicate):
            raise HivePlanningError("AND requires another ray-hive Predicate")
        return LogicalPredicate("AND", (self, other))

    def __or__(self, other: Predicate) -> Predicate:
        if not isinstance(other, Predicate):
            raise HivePlanningError("OR requires another ray-hive Predicate")
        return LogicalPredicate("OR", (self, other))

    def __invert__(self) -> Predicate:
        return NotPredicate(self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(<redacted>)"


@dataclass(frozen=True, slots=True)
class Column:
    """A Hive column reference used to build structured predicates."""

    name: str

    def __post_init__(self) -> None:
        quote_identifier(self.name)

    def _compare(self, operator: str, value: HiveLiteral) -> Predicate:
        return ComparisonPredicate(self, operator, value)

    def eq(self, value: HiveLiteral) -> Predicate:
        return self._compare("=", value)

    def ne(self, value: HiveLiteral) -> Predicate:
        return self._compare("<>", value)

    def lt(self, value: HiveLiteral) -> Predicate:
        return self._compare("<", value)

    def le(self, value: HiveLiteral) -> Predicate:
        return self._compare("<=", value)

    def gt(self, value: HiveLiteral) -> Predicate:
        return self._compare(">", value)

    def ge(self, value: HiveLiteral) -> Predicate:
        return self._compare(">=", value)

    def is_null(self) -> Predicate:
        return NullPredicate(self, negated=False)

    def is_not_null(self) -> Predicate:
        return NullPredicate(self, negated=True)

    def isin(self, values: tuple[HiveLiteral, ...] | list[HiveLiteral]) -> Predicate:
        return InPredicate(self, tuple(values), negated=False)

    def notin(self, values: tuple[HiveLiteral, ...] | list[HiveLiteral]) -> Predicate:
        return InPredicate(self, tuple(values), negated=True)

    def to_sql(self) -> str:
        return quote_identifier(self.name)


@dataclass(frozen=True, slots=True, repr=False)
class ComparisonPredicate(Predicate):
    column: Column
    operator: str
    value: HiveLiteral

    def __post_init__(self) -> None:
        if self.operator not in {"=", "<>", "<", "<=", ">", ">="}:
            raise HivePlanningError(f"Unsupported comparison operator: {self.operator!r}")
        if self.value is None:
            raise HivePlanningError("NULL comparisons must use is_null() or is_not_null()")

    def to_sql(self) -> str:
        return f"{self.column.to_sql()} {self.operator} {render_literal(self.value)}"


@dataclass(frozen=True, slots=True, repr=False)
class NullPredicate(Predicate):
    column: Column
    negated: bool

    def to_sql(self) -> str:
        operator = "IS NOT NULL" if self.negated else "IS NULL"
        return f"{self.column.to_sql()} {operator}"


@dataclass(frozen=True, slots=True, repr=False)
class InPredicate(Predicate):
    column: Column
    values: tuple[HiveLiteral, ...]
    negated: bool

    def __post_init__(self) -> None:
        if not self.values:
            raise HivePlanningError("IN predicates require at least one value")
        if any(value is None for value in self.values):
            raise HivePlanningError("IN predicates cannot contain NULL")

    def to_sql(self) -> str:
        operator = "NOT IN" if self.negated else "IN"
        values = ", ".join(render_literal(value) for value in self.values)
        return f"{self.column.to_sql()} {operator} ({values})"


@dataclass(frozen=True, slots=True, repr=False)
class LogicalPredicate(Predicate):
    operator: str
    operands: tuple[Predicate, ...]

    def __post_init__(self) -> None:
        if self.operator not in {"AND", "OR"}:
            raise HivePlanningError(f"Unsupported logical operator: {self.operator!r}")
        if len(self.operands) < 2:
            raise HivePlanningError("Logical predicates require at least two operands")
        if any(not isinstance(operand, Predicate) for operand in self.operands):
            raise HivePlanningError("Logical operands must be ray-hive Predicates")

    def to_sql(self) -> str:
        return "(" + f") {self.operator} (".join(item.to_sql() for item in self.operands) + ")"


@dataclass(frozen=True, slots=True, repr=False)
class NotPredicate(Predicate):
    operand: Predicate

    def __post_init__(self) -> None:
        if not isinstance(self.operand, Predicate):
            raise HivePlanningError("NOT requires a ray-hive Predicate")

    def to_sql(self) -> str:
        return f"NOT ({self.operand.to_sql()})"


def col(name: str) -> Column:
    """Create a column reference for the structured predicate API."""

    return Column(name)
