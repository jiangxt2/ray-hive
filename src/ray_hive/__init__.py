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

"""Public API for the ray-hive HiveServer2 datasource."""

from ray_hive._api import read_hive
from ray_hive._credentials import CredentialProvider, SecretRef
from ray_hive._errors import (
    HiveAuthenticationError,
    HiveCancellationError,
    HiveCleanupError,
    HiveCompatibilityError,
    HiveConfigurationError,
    HiveDependencyError,
    HiveError,
    HiveErrorCode,
    HivePermissionError,
    HivePlanningError,
    HiveProtocolError,
    HiveQueryError,
    HiveSchemaError,
    HiveTransportError,
)
from ray_hive._models import (
    HiveConnectionOptions,
    HiveReadOptions,
    HiveTableIdentifier,
    HiveTLSOptions,
)

__all__ = [
    "CredentialProvider",
    "HiveAuthenticationError",
    "HiveCancellationError",
    "HiveCleanupError",
    "HiveCompatibilityError",
    "HiveConfigurationError",
    "HiveConnectionOptions",
    "HiveDependencyError",
    "HiveError",
    "HiveErrorCode",
    "HivePermissionError",
    "HivePlanningError",
    "HiveProtocolError",
    "HiveQueryError",
    "HiveReadOptions",
    "HiveSchemaError",
    "HiveTLSOptions",
    "HiveTableIdentifier",
    "HiveTransportError",
    "SecretRef",
    "read_hive",
]
