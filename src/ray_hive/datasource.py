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

"""Advanced Alpha Ray Datasource API."""

from __future__ import annotations

from functools import partial
from typing import Any

from ray.data import Datasource

from ray_hive._compat import create_read_task, initialize_datasource
from ray_hive._hs2.client import resolve_connection_factory
from ray_hive._models import HiveConnectionOptions, HiveReadOptions
from ray_hive._planner import HivePlanner
from ray_hive._task_spec import HiveTaskSpec
from ray_hive._worker import iter_hive_blocks


class HiveServer2Datasource(Datasource):
    """An immutable planned datasource; direct use does not imply strict Ray task options."""

    def __init__(self, task_specs: tuple[HiveTaskSpec, ...]) -> None:
        initialize_datasource(self)
        if not task_specs:
            raise ValueError("HiveServer2Datasource requires at least one task spec")
        self._task_specs = task_specs
        self._projection_map: dict[str, str] | None = None

    @classmethod
    def from_options(
        cls,
        connection: HiveConnectionOptions,
        read: HiveReadOptions,
    ) -> HiveServer2Datasource:
        """Plan through HS2 and cache a resource-free physical plan."""

        planner = HivePlanner(resolve_connection_factory)
        return cls(planner.plan(connection, read))

    @property
    def task_specs(self) -> tuple[HiveTaskSpec, ...]:
        """Return the immutable planned task set."""

        return self._task_specs

    def estimate_inmemory_data_size(self) -> None:
        """Return unknown without issuing COUNT or sampling queries."""

        return None

    def supports_projection_pushdown(self) -> bool:
        """Remain disabled until empty projection and rename contracts are proven."""

        return False

    def supports_predicate_pushdown(self) -> bool:
        """Keep Ray Expr filters above the read; only ray-hive predicates are pushed."""

        return False

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[Any]:
        """Create one strict task or one task per explicit independent predicate."""

        del parallelism, data_context
        return [
            create_read_task(
                partial(iter_hive_blocks, spec),
                spec.expected_arrow_schema,
                per_task_row_limit=per_task_row_limit,
            )
            for spec in self._task_specs
        ]
