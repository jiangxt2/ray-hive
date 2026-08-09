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

import pytest

from ray_hive import (
    HiveAuthenticationError,
    HiveCompatibilityError,
    HiveConfigurationError,
    HiveDependencyError,
    HiveErrorCode,
    HiveProtocolError,
    HiveSchemaError,
    HiveTransportError,
)
from ray_hive._capabilities import CapabilityState, HiveCapabilities
from ray_hive._telemetry import query_fingerprint


def _capabilities(**overrides: object) -> HiveCapabilities:
    values = {
        "protocol_version": 11,
        "columnar_rowsets": CapabilityState.SUPPORTED,
        "binary_columns": CapabilityState.UNSUPPORTED,
        "cancel_operation": CapabilityState.SUPPORTED,
        "operation_log": CapabilityState.UNKNOWN,
    }
    values.update(overrides)
    return HiveCapabilities(**values)


def test_core_capability_requires_explicit_evidence() -> None:
    _capabilities().require_core()
    with pytest.raises(HiveCompatibilityError, match="V6"):
        _capabilities(protocol_version=5).require_core()
    with pytest.raises(HiveCompatibilityError, match="columnar"):
        _capabilities(columnar_rowsets=CapabilityState.UNKNOWN).require_core()
    with pytest.raises(HiveCompatibilityError, match="CancelOperation"):
        _capabilities(cancel_operation=CapabilityState.UNSUPPORTED).require_core()


def test_public_error_codes_and_inheritance_are_stable() -> None:
    assert issubclass(HiveDependencyError, HiveConfigurationError)
    assert issubclass(HiveCompatibilityError, HiveProtocolError)
    assert HiveAuthenticationError("auth").error_code is HiveErrorCode.AUTH
    assert HiveCompatibilityError("version").error_code is HiveErrorCode.PROTOCOL


def test_transport_after_output_is_non_retryable_and_redacted_in_repr() -> None:
    error = HiveTransportError.after_output("safe summary", operation_state="running")
    assert error.error_code is HiveErrorCode.TRANSPORT_AFTER_OUTPUT
    assert error.retryable is False
    assert error.partial_output is True
    assert "safe summary" not in repr(error)
    assert "running" in repr(error)


def test_any_hive_error_can_preserve_its_class_after_partial_output() -> None:
    error = HiveSchemaError("schema changed", retryable=True)

    marked = error.mark_partial_output()

    assert marked is error
    assert isinstance(marked, HiveSchemaError)
    assert marked.error_code is HiveErrorCode.SCHEMA
    assert marked.retryable is False
    assert marked.partial_output is True


def test_named_capability_requirement_and_query_fingerprint_are_stable() -> None:
    _capabilities().require("columnar_rowsets", CapabilityState.SUPPORTED)
    with pytest.raises(HiveCompatibilityError, match="not explicitly"):
        _capabilities().require("operation_log", CapabilityState.SUPPORTED)
    assert query_fingerprint("SELECT 1") == query_fingerprint("SELECT 1")
    assert query_fingerprint("SELECT 1") != query_fingerprint("SELECT 2")
