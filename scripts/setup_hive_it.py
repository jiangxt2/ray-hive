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

"""Load the deterministic integration fixture through native HiveServer2 RPC."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from ray_hive import HiveConnectionOptions
from ray_hive._credentials import HiveCredentials
from ray_hive._errors import HiveProtocolError
from ray_hive._hs2.client import NativeHiveRPC, _require_success, ttypes
from ray_hive._hs2.protocol import SessionHandle


def split_sql_script(source: str) -> tuple[str, ...]:
    """Split the controlled fixture script on unquoted statement terminators."""

    statements: list[str] = []
    buffer: list[str] = []
    in_string = False
    index = 0
    while index < len(source):
        character = source[index]
        buffer.append(character)
        if character == "'":
            if in_string and index + 1 < len(source) and source[index + 1] == "'":
                buffer.append(source[index + 1])
                index += 1
            else:
                in_string = not in_string
        elif character == ";" and not in_string:
            statement = "".join(buffer[:-1]).strip()
            if statement:
                statements.append(statement)
            buffer.clear()
        index += 1

    if in_string:
        raise ValueError("Hive fixture SQL contains an unterminated string literal")
    trailing = "".join(buffer).strip()
    if trailing:
        raise ValueError("Hive fixture SQL must terminate every statement with a semicolon")
    return tuple(statements)


def execute_fixture(client: Any, session: SessionHandle, statements: tuple[str, ...]) -> None:
    """Execute fixture statements synchronously and close every operation."""

    for statement in statements:
        response = client.ExecuteStatement(
            ttypes.TExecuteStatementReq(
                sessionHandle=session.value,
                statement=statement,
                runAsync=False,
                queryTimeout=120,
            )
        )
        _require_success(response.status, action="fixture ExecuteStatement")
        if response.operationHandle is None:
            raise HiveProtocolError("Hive fixture operation omitted its handle")
        close_response = client.CloseOperation(
            ttypes.TCloseOperationReq(operationHandle=response.operationHandle)
        )
        _require_success(close_response.status, action="fixture CloseOperation")


def load_fixture(path: Path, *, host: str, port: int) -> None:
    """Open one setup session, load the fixture, and release all RPC resources."""

    connection = HiveConnectionOptions(host=host, port=port, rpc_timeout=180.0)
    rpc = NativeHiveRPC.connect(connection)
    session: SessionHandle | None = None
    try:
        session, _ = rpc.open_session(connection, HiveCredentials(username="hive"))
        statements = split_sql_script(path.read_text(encoding="utf-8"))
        execute_fixture(rpc.client, session, statements)
    finally:
        try:
            if session is not None:
                rpc.close_session(session)
        finally:
            rpc.close_transport()


def main() -> None:
    """Parse integration-only endpoint arguments and load the SQL fixture."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("script", type=Path)
    parser.add_argument("--host", default=os.environ.get("RAY_HIVE_HS2_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("RAY_HIVE_HS2_PORT", "10090")),
    )
    arguments = parser.parse_args()
    load_fixture(arguments.script, host=arguments.host, port=arguments.port)


if __name__ == "__main__":
    main()
