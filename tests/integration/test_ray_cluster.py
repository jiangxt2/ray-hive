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

import os
import time

import pytest
import ray

from ray_hive import HiveConnectionOptions, HiveReadOptions, HiveTableIdentifier, read_hive

pytestmark = [pytest.mark.integration, pytest.mark.cluster]


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required by the Ray cluster integration profile")
    return value


def test_strict_read_executes_on_a_multi_node_ray_cluster() -> None:
    address = _required_environment("RAY_HIVE_RAY_ADDRESS")
    minimum_nodes = int(_required_environment("RAY_HIVE_MIN_NODES"))
    ray.init(address=address, namespace="ray-hive-cluster-it")
    try:
        deadline = time.monotonic() + 60
        alive_nodes = []
        while time.monotonic() < deadline:
            alive_nodes = [node for node in ray.nodes() if node["Alive"]]
            if len(alive_nodes) >= minimum_nodes:
                break
            time.sleep(1)
        assert len(alive_nodes) >= minimum_nodes

        dataset = read_hive(
            connection=HiveConnectionOptions(
                host=_required_environment("RAY_HIVE_HS2_HOST"),
                port=int(_required_environment("RAY_HIVE_HS2_PORT")),
                database="ray_hive_it",
                auth="NOSASL",
                session_options={
                    "hive.local.time.zone": "UTC",
                    "hive.resultset.use.unique.column.names": "false",
                },
                rpc_timeout=30,
            ),
            read=HiveReadOptions(
                table=HiveTableIdentifier("ray_hive_it", "primitive_view"),
                fetch_rows=1,
            ),
        )
        rows = sorted(dataset.take_all(), key=lambda row: row["id"])
    finally:
        ray.shutdown()

    assert [row["id"] for row in rows] == [2, 3, 4, 5]
