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

"""Hive session ownership and deterministic cleanup."""

from __future__ import annotations

from dataclasses import dataclass, replace

from ray_hive._capabilities import HiveCapabilities
from ray_hive._credentials import HiveCredentials
from ray_hive._errors import HiveCleanupError
from ray_hive._hs2.operation import HiveOperation
from ray_hive._hs2.protocol import HiveRPC, SessionHandle
from ray_hive._models import HiveConnectionOptions


def resolve_credentials(connection: HiveConnectionOptions) -> HiveCredentials:
    """Resolve credentials on the trusted node that is opening the HS2 session."""

    if connection.auth == "NOSASL":
        return HiveCredentials(username=connection.username)
    if connection.credentials is None:
        return HiveCredentials(username=connection.username)
    credentials = connection.credentials.resolve(connection.credential_context)
    if connection.username is not None:
        credentials = replace(credentials, username=connection.username)
    return credentials


@dataclass(slots=True)
class HiveSession:
    """An open session and the transport it exclusively owns."""

    rpc: HiveRPC
    handle: SessionHandle
    capabilities: HiveCapabilities
    _closed: bool = False

    @classmethod
    def open(cls, rpc: HiveRPC, connection: HiveConnectionOptions) -> HiveSession:
        """Resolve credentials, open a session, and enforce the Core capability set."""

        try:
            handle, capabilities = rpc.open_session(connection, resolve_credentials(connection))
        except Exception as exc:
            try:
                rpc.close_transport()
            except Exception as cleanup_error:
                exc.add_note(
                    "Transport cleanup also failed after OpenSession: "
                    f"{type(cleanup_error).__name__}"
                )
            raise
        try:
            capabilities.require_core()
        except Exception as exc:
            try:
                rpc.close_session(handle)
            except Exception as cleanup_error:
                exc.add_note(
                    "Session cleanup also failed after capability validation: "
                    f"{type(cleanup_error).__name__}"
                )
            try:
                rpc.close_transport()
            except Exception as cleanup_error:
                exc.add_note(
                    "Transport cleanup also failed after capability validation: "
                    f"{type(cleanup_error).__name__}"
                )
            raise
        return cls(rpc=rpc, handle=handle, capabilities=capabilities)

    def execute(self, statement: str, *, timeout_seconds: int) -> HiveOperation:
        """Submit one statement as a session-owned operation."""

        handle = self.rpc.execute_statement(self.handle, statement, timeout_seconds=timeout_seconds)
        return HiveOperation(self.rpc, handle)

    def close(self) -> None:
        """Close the session and transport exactly once, preserving both failures."""

        if self._closed:
            return
        errors: list[Exception] = []
        try:
            self.rpc.close_session(self.handle)
        except Exception as exc:
            errors.append(exc)
        try:
            self.rpc.close_transport()
        except Exception as exc:
            errors.append(exc)
        self._closed = True
        if errors:
            raise HiveCleanupError("Failed to close Hive session resources") from errors[0]
