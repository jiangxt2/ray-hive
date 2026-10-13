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

"""The only module allowed to adapt Ray DeveloperAPI version differences."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable, Mapping
from importlib.metadata import version
from typing import Any

import pyarrow as pa

from ray_hive._errors import HiveCompatibilityError

SUPPORTED_RAY_RANGE = ">=2.55.1,<2.60"
SUPPORTED_PYARROW_VERSION = "19.0.1"
_MIN_RAY_VERSION = (2, 55, 1)
_MAX_RAY_VERSION = (2, 60, 0)
_FINAL_RAY_RELEASE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*)?$")
_READ_TASK_PARAMETERS = ("self", "read_fn", "metadata", "schema", "per_task_row_limit")
_GET_READ_TASKS_PARAMETERS = (
    "self",
    "parallelism",
    "per_task_row_limit",
    "data_context",
)


def validate_runtime() -> None:
    """Fail closed unless the exact tested Ray/PyArrow contracts are installed."""

    installed_ray = version("ray")
    match = _FINAL_RAY_RELEASE.fullmatch(installed_ray)
    release = tuple(int(part) for part in match.groups()) if match is not None else ()
    if not _MIN_RAY_VERSION <= release < _MAX_RAY_VERSION:
        raise HiveCompatibilityError(
            f"ray-hive requires final Ray releases {SUPPORTED_RAY_RANGE}; found {installed_ray!r}"
        )
    if pa.__version__ != SUPPORTED_PYARROW_VERSION:
        raise HiveCompatibilityError(
            f"ray-hive requires PyArrow {SUPPORTED_PYARROW_VERSION} in the initial release"
        )

    from ray.data import Datasource, ReadTask

    read_task_parameters = tuple(inspect.signature(ReadTask.__init__).parameters)
    if read_task_parameters != _READ_TASK_PARAMETERS:
        raise HiveCompatibilityError("Unsupported Ray ReadTask constructor signature")
    get_tasks_parameters = tuple(inspect.signature(Datasource.get_read_tasks).parameters)
    if get_tasks_parameters != _GET_READ_TASKS_PARAMETERS:
        raise HiveCompatibilityError("Unsupported Ray Datasource.get_read_tasks signature")


def initialize_datasource(instance: Any) -> None:
    """Invoke the exact pinned Ray Datasource initializer through the adapter boundary."""

    from ray.data import Datasource

    initializer: Any = Datasource.__init__
    initializer(instance)


def create_read_task(
    read_fn: Callable[[], Iterable[pa.Table]],
    schema: pa.Schema,
    *,
    per_task_row_limit: int | None,
) -> Any:
    """Construct the exact supported DeveloperAPI ReadTask shape."""

    validate_runtime()
    from ray.data import ReadTask
    from ray.data.block import BlockMetadata

    metadata = BlockMetadata(
        num_rows=None,
        size_bytes=None,
        exec_stats=None,
        input_files=None,
    )
    return ReadTask(
        read_fn,
        metadata,
        schema=schema,
        per_task_row_limit=per_task_row_limit,
    )


def read_datasource_strict(
    datasource: Any,
    *,
    num_tasks: int,
    ray_remote_args: Mapping[str, Any] | None,
) -> Any:
    """Call the public Ray facade with no retry or lineage replay of a query task."""

    validate_runtime()
    from ray.data import read_datasource

    parameters = inspect.signature(read_datasource).parameters
    required = {"datasource", "ray_remote_args", "concurrency", "override_num_blocks"}
    if not required.issubset(parameters):
        raise HiveCompatibilityError("Unsupported Ray read_datasource signature")
    remote_args = dict(ray_remote_args or {})
    if remote_args.get("max_retries", 0) != 0:
        raise HiveCompatibilityError("Strict Hive reads require Ray max_retries=0")
    remote_args["max_retries"] = 0
    return read_datasource(
        datasource,
        ray_remote_args=remote_args,
        concurrency=num_tasks,
        override_num_blocks=num_tasks,
    )
