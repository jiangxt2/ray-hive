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

"""Stable public read facade."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ray_hive._compat import read_datasource_strict
from ray_hive._models import HiveConnectionOptions, HiveReadOptions
from ray_hive.datasource import HiveServer2Datasource


def read_hive(
    *,
    connection: HiveConnectionOptions,
    read: HiveReadOptions,
    ray_remote_args: Mapping[str, Any] | None = None,
) -> Any:
    """Plan an HS2-only read and return a lazy Ray Dataset with task retries disabled."""

    datasource = HiveServer2Datasource.from_options(connection, read)
    return read_datasource_strict(
        datasource,
        num_tasks=len(datasource.task_specs),
        ray_remote_args=ray_remote_args,
    )
