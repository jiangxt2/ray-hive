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

"""Fail-closed HiveServer2 capability negotiation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ray_hive._errors import HiveCompatibilityError


class CapabilityState(StrEnum):
    """Tri-state support result; unknown is never treated as supported."""

    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CapabilityEvidence:
    """The origin of one capability conclusion."""

    capability: str
    state: CapabilityState
    source: str


@dataclass(frozen=True, slots=True)
class HiveCapabilities:
    """Immutable capabilities negotiated immediately after OpenSession."""

    protocol_version: int
    columnar_rowsets: CapabilityState
    binary_columns: CapabilityState
    cancel_operation: CapabilityState
    operation_log: CapabilityState
    evidence: tuple[CapabilityEvidence, ...] = ()

    def require_core(self) -> None:
        """Reject a connection that cannot satisfy the Core read profile."""

        if self.protocol_version < 6:
            raise HiveCompatibilityError("HS2 protocol V6 or newer is required")
        if self.columnar_rowsets is not CapabilityState.SUPPORTED:
            raise HiveCompatibilityError("Native V6 columnar result sets are required")
        if self.cancel_operation is not CapabilityState.SUPPORTED:
            raise HiveCompatibilityError("CancelOperation capability is required")

    def require(self, capability: str, state: CapabilityState) -> None:
        """Require an explicitly supported capability by attribute name."""

        actual = getattr(self, capability, None)
        if actual is not state:
            raise HiveCompatibilityError(
                f"Required capability {capability!r} is not explicitly {state.value}"
            )
