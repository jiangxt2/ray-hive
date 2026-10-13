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
from cluster_read_support import ObservedHiveDatasource, ReadEvents

from ray_hive import HiveConnectionOptions, HiveReadOptions, HiveTableIdentifier, read_hive
from ray_hive._compat import read_datasource_strict
from ray_hive.datasource import HiveServer2Datasource

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


@pytest.mark.parametrize("fail_after_first", [False, True])
def test_strict_partial_output_does_not_replay_and_closes_hs2(
    fail_after_first: bool,
) -> None:
    ray.init(address=_required_environment("RAY_HIVE_RAY_ADDRESS"), namespace="ray-hive-fault-it")
    events = ReadEvents.remote()
    try:
        source = HiveServer2Datasource.from_options(
            HiveConnectionOptions(
                host=_required_environment("RAY_HIVE_HS2_HOST"),
                port=int(_required_environment("RAY_HIVE_HS2_PORT")),
                database="ray_hive_it",
                auth="NOSASL",
                rpc_timeout=30,
            ),
            HiveReadOptions(
                table=HiveTableIdentifier("ray_hive_it", "primitive_view"), fetch_rows=1
            ),
        )
        dataset = read_datasource_strict(
            ObservedHiveDatasource(source.task_specs, events, fail_after_first=fail_after_first),
            num_tasks=1,
            ray_remote_args={"num_cpus": 1},
        )
        dataset.context.target_max_block_size = 1
        if fail_after_first:
            batches = iter(
                dataset.iter_batches(batch_size=1, batch_format="pyarrow", prefetch_batches=0)
            )
            try:
                assert next(batches).num_rows == 1
                ray.get(events.release.remote())
                with pytest.raises(Exception, match="Injected HS2 fault after partial output"):
                    list(batches)
            finally:
                ray.get(events.release.remote())
                batches.close()
        else:
            assert len(dataset.limit(1).take_all()) == 1

        deadline = time.monotonic() + 15
        while True:
            counts = ray.get(events.snapshot.remote())
            if counts.get("close_transport") == 1:
                break
            assert time.monotonic() < deadline, counts
            time.sleep(0.05)
        assert counts["attempts"] == counts["queries"] == 1
        assert counts["cancel_operation"] == 1
        assert counts["close_operation"] == counts["close_session"] == 1
        assert counts.get("eof", 0) == 0
        assert counts["blocks"] == 1
    finally:
        ray.get(events.release.remote())
        ray.kill(events)
        ray.shutdown()
