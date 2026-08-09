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

"""Serializable secret references resolved only by trusted HS2-connecting nodes."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Protocol, runtime_checkable

from ray_hive._errors import HiveAuthenticationError, HiveConfigurationError


@dataclass(frozen=True, slots=True)
class CredentialContext:
    """Security boundary to which resolved credentials must be bound."""

    endpoint: str
    driver: str
    auth: str
    tls_server_name: str | None
    audience: str | None = None


@dataclass(frozen=True, slots=True)
class HiveCredentials:
    """Ephemeral authentication material that must never enter a task spec."""

    username: str | None = None
    password: str | None = field(default=None, repr=False)
    token: str | None = field(default=None, repr=False)


@runtime_checkable
class CredentialProvider(Protocol):
    """A serializable strategy resolved only by a trusted HS2-connecting node."""

    def bind(self, context: CredentialContext) -> CredentialProvider:
        """Return a provider restricted to the supplied connection context."""

    def resolve(self, context: CredentialContext) -> HiveCredentials:
        """Resolve ephemeral authentication material for the supplied context."""


@dataclass(frozen=True, slots=True)
class SecretRef:
    """An immutable reference to one secret value, never the value itself."""

    provider: str
    key: str
    allowed_context: CredentialContext | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.provider != "env":
            raise HiveConfigurationError(f"Unsupported secret provider: {self.provider!r}")
        if not self.key or "\x00" in self.key:
            raise HiveConfigurationError(
                "Secret reference key must be non-empty and contain no NUL"
            )

    @classmethod
    def env(cls, variable: str) -> SecretRef:
        """Create a reference resolved from the local HS2 client's environment."""

        return cls(provider="env", key=variable)

    def bind(self, context: CredentialContext) -> SecretRef:
        """Bind this reference to exactly one endpoint and authentication context."""

        if self.allowed_context is not None and self.allowed_context != context:
            raise HiveConfigurationError("Secret reference is already bound to another endpoint")
        return replace(self, allowed_context=context)

    def resolve_value(self, context: CredentialContext) -> str:
        """Resolve the referenced value after validating its endpoint binding."""

        if self.allowed_context is None or self.allowed_context != context:
            raise HiveAuthenticationError("Secret reference is not bound to this endpoint")
        try:
            return os.environ[self.key]
        except KeyError as exc:
            raise HiveAuthenticationError(
                f"Required environment credential {self.key!r} is unavailable"
            ) from exc

    def resolve(self, context: CredentialContext) -> HiveCredentials:
        """Resolve a direct secret reference as a password credential."""

        return HiveCredentials(password=self.resolve_value(context))

    def __repr__(self) -> str:
        return (
            f"SecretRef(provider={self.provider!r}, key={self.key!r}, "
            f"bound={self.allowed_context is not None})"
        )


@dataclass(frozen=True, slots=True)
class EnvironmentCredentialProvider:
    """Resolve optional username, password, and token environment references."""

    username: str | None = None
    password: SecretRef | None = None
    token: SecretRef | None = None
    allowed_context: CredentialContext | None = field(default=None, repr=False)

    def bind(self, context: CredentialContext) -> EnvironmentCredentialProvider:
        """Bind every contained reference to one connection context."""

        if self.allowed_context is not None and self.allowed_context != context:
            raise HiveConfigurationError("Credential provider is already bound to another endpoint")
        return replace(
            self,
            password=self.password.bind(context) if self.password else None,
            token=self.token.bind(context) if self.token else None,
            allowed_context=context,
        )

    def resolve(self, context: CredentialContext) -> HiveCredentials:
        """Resolve authentication material only when the binding is exact."""

        if self.allowed_context != context:
            raise HiveAuthenticationError("Credential provider is not bound to this endpoint")
        return HiveCredentials(
            username=self.username,
            password=self.password.resolve_value(context) if self.password else None,
            token=self.token.resolve_value(context) if self.token else None,
        )
