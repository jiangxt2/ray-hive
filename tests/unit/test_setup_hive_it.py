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

from types import SimpleNamespace
from typing import Any

import pytest

from ray_hive import HivePlanningError, HiveProtocolError
from ray_hive._hs2.generated.TCLIService import ttypes
from ray_hive._hs2.protocol import SessionHandle
from scripts.setup_hive_it import execute_fixture, split_sql_script


def _status(code: int = ttypes.TStatusCode.SUCCESS_STATUS, state: str | None = None) -> Any:
    return SimpleNamespace(statusCode=code, sqlState=state)


class _FixtureClient:
    def __init__(self, *, handle: object | None = object(), state: str | None = None) -> None:
        self.handle = handle
        self.state = state
        self.statements: list[str] = []
        self.closed: list[object] = []

    def ExecuteStatement(self, request: Any) -> Any:
        self.statements.append(request.statement)
        return SimpleNamespace(
            status=_status(
                ttypes.TStatusCode.ERROR_STATUS
                if self.state
                else ttypes.TStatusCode.SUCCESS_STATUS,
                self.state,
            ),
            operationHandle=self.handle,
        )

    def CloseOperation(self, request: Any) -> Any:
        self.closed.append(request.operationHandle)
        return SimpleNamespace(status=_status())


def test_split_sql_script_preserves_semicolons_and_escaped_quotes() -> None:
    source = "INSERT INTO t VALUES ('a;''b');\nSELECT * FROM t;\n"

    assert split_sql_script(source) == (
        "INSERT INTO t VALUES ('a;''b')",
        "SELECT * FROM t",
    )


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("SELECT 'unterminated;", "unterminated string literal"),
        ("SELECT 1", "terminate every statement"),
    ],
)
def test_split_sql_script_rejects_incomplete_input(source: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        split_sql_script(source)


def test_execute_fixture_closes_every_successful_operation() -> None:
    client = _FixtureClient()

    execute_fixture(client, SessionHandle("session"), ("CREATE TABLE t (id INT)", "DROP TABLE t"))

    assert client.statements == ["CREATE TABLE t (id INT)", "DROP TABLE t"]
    assert client.closed == [client.handle, client.handle]


def test_execute_fixture_rejects_missing_handle_and_planning_failure() -> None:
    with pytest.raises(HiveProtocolError, match="omitted its handle"):
        execute_fixture(_FixtureClient(handle=None), SessionHandle("session"), ("SELECT 1",))
    with pytest.raises(HivePlanningError):
        execute_fixture(_FixtureClient(state="42000"), SessionHandle("session"), ("BROKEN",))
