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

"""Public, stable error contracts for ray-hive."""

from __future__ import annotations

from enum import StrEnum


class HiveErrorCode(StrEnum):
    """Stable conformance error codes."""

    CONFIG = "CONFIG"
    AUTH = "AUTH"
    PERMISSION = "PERMISSION"
    QUERY_COMPILE = "QUERY_COMPILE"
    QUERY_EXECUTION = "QUERY_EXECUTION"
    SCHEMA = "SCHEMA"
    PROTOCOL = "PROTOCOL"
    TRANSPORT_BEFORE_OUTPUT = "TRANSPORT_BEFORE_OUTPUT"
    TRANSPORT_AFTER_OUTPUT = "TRANSPORT_AFTER_OUTPUT"
    CANCELLED = "CANCELLED"
    CLEANUP = "CLEANUP"


class HiveError(Exception):
    """Base exception with stable, sanitized machine-readable attributes."""

    default_error_code = HiveErrorCode.CONFIG
    default_retryable = False
    default_partial_output = False

    def __init__(
        self,
        safe_message: str,
        *,
        error_code: HiveErrorCode | None = None,
        retryable: bool | None = None,
        partial_output: bool | None = None,
        operation_state: str | None = None,
    ) -> None:
        super().__init__(safe_message)
        self.error_code = error_code or self.default_error_code
        self.retryable = self.default_retryable if retryable is None else retryable
        self.partial_output = (
            self.default_partial_output if partial_output is None else partial_output
        )
        self.operation_state = operation_state

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(error_code={self.error_code.value!r}, "
            f"retryable={self.retryable!r}, partial_output={self.partial_output!r}, "
            f"operation_state={self.operation_state!r})"
        )

    def mark_partial_output(self) -> HiveError:
        """Preserve the concrete error while recording that output was published."""

        self.partial_output = True
        self.retryable = False
        if self.error_code is HiveErrorCode.TRANSPORT_BEFORE_OUTPUT:
            self.error_code = HiveErrorCode.TRANSPORT_AFTER_OUTPUT
        return self


class HiveConfigurationError(HiveError):
    """Raised for invalid or insecure configuration."""


class HiveDependencyError(HiveConfigurationError):
    """Raised when an explicitly selected optional dependency is unavailable."""


class HiveAuthenticationError(HiveError):
    """Raised when HiveServer2 authentication fails."""

    default_error_code = HiveErrorCode.AUTH


class HivePermissionError(HiveError):
    """Raised when HiveServer2 rejects an authorized operation."""

    default_error_code = HiveErrorCode.PERMISSION


class HivePlanningError(HiveError):
    """Raised when a logical read cannot be compiled safely."""

    default_error_code = HiveErrorCode.QUERY_COMPILE


class HiveSchemaError(HiveError):
    """Raised for unsupported types or schema drift."""

    default_error_code = HiveErrorCode.SCHEMA


class HiveQueryError(HiveError):
    """Raised when an accepted Hive query fails during execution."""

    default_error_code = HiveErrorCode.QUERY_EXECUTION


class HiveProtocolError(HiveError):
    """Raised when the server violates the negotiated HS2 protocol."""

    default_error_code = HiveErrorCode.PROTOCOL


class HiveCompatibilityError(HiveProtocolError):
    """Raised when a runtime version or capability is unsupported."""


class HiveTransportError(HiveError):
    """Raised when the HS2 transport fails."""

    default_error_code = HiveErrorCode.TRANSPORT_BEFORE_OUTPUT
    default_retryable = True

    @classmethod
    def after_output(
        cls, safe_message: str, *, operation_state: str | None = None
    ) -> HiveTransportError:
        """Construct a non-retryable transport error after a block was published."""

        return cls(
            safe_message,
            error_code=HiveErrorCode.TRANSPORT_AFTER_OUTPUT,
            retryable=False,
            partial_output=True,
            operation_state=operation_state,
        )


class HiveCancellationError(HiveError):
    """Raised when a read is cancelled."""

    default_error_code = HiveErrorCode.CANCELLED


class HiveCleanupError(HiveError):
    """Raised when required resource cleanup fails."""

    default_error_code = HiveErrorCode.CLEANUP
