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

from typing import Any

import pyarrow as pa
import pytest
import ray.data

from ray_hive import HiveCompatibilityError, HiveConnectionOptions
from ray_hive import _api as api_module
from ray_hive import _compat as compat_module
from ray_hive._compat import create_read_task, read_datasource_strict, validate_runtime
from ray_hive._hs2.protocol import HiveColumnSchema, HiveResultSchema, HiveType, HiveTypeKind
from ray_hive._models import HiveConsistency, UnsupportedTypeMode
from ray_hive._task_spec import HiveTaskSpec
from ray_hive.datasource import HiveServer2Datasource


def _spec() -> HiveTaskSpec:
    hive_schema = HiveResultSchema(
        (HiveColumnSchema("id", HiveType(HiveTypeKind.INT), position=1),)
    )
    return HiveTaskSpec(
        connection=HiveConnectionOptions(host="hs2.example"),
        statement="SELECT id FROM db.events",
        expected_arrow_schema=pa.schema([("id", pa.int32())]),
        expected_hive_schema=hive_schema,
        fetch_rows=100,
        target_batch_bytes=1024,
        query_timeout=5,
        consistency=HiveConsistency.STRICT,
        unsupported_type_mode=UnsupportedTypeMode.ERROR,
    )


def test_exact_runtime_contract_is_supported() -> None:
    validate_runtime()


def test_read_task_yields_multiple_blocks_and_applies_ray_side_limit() -> None:
    schema = pa.schema([("id", pa.int32())])
    task = create_read_task(
        lambda: iter(
            [
                pa.Table.from_pydict({"id": [1, 2]}, schema=schema),
                pa.Table.from_pydict({"id": [3, 4]}, schema=schema),
            ]
        ),
        schema,
        per_task_row_limit=3,
    )

    blocks = list(task())
    assert [row for block in blocks for row in block.column(0).to_pylist()] == [1, 2, 3]


def test_strict_facade_forces_zero_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_read_datasource(
        datasource: Any,
        *,
        ray_remote_args: dict[str, Any],
        concurrency: int,
        override_num_blocks: int,
    ) -> str:
        captured.update(
            datasource=datasource,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )
        return "dataset"

    monkeypatch.setattr(ray.data, "read_datasource", fake_read_datasource)
    datasource = HiveServer2Datasource((_spec(),))

    result = read_datasource_strict(
        datasource,
        num_tasks=1,
        ray_remote_args={"num_cpus": 0.5},
    )

    assert result == "dataset"
    assert captured["ray_remote_args"] == {"num_cpus": 0.5, "max_retries": 0}
    assert captured["concurrency"] == 1
    assert captured["override_num_blocks"] == 1


def test_strict_facade_rejects_retry_override() -> None:
    with pytest.raises(HiveCompatibilityError, match="max_retries=0"):
        read_datasource_strict(
            HiveServer2Datasource((_spec(),)),
            num_tasks=1,
            ray_remote_args={"max_retries": 1},
        )


def test_datasource_declines_unsafe_optimizer_pushdown() -> None:
    datasource = HiveServer2Datasource((_spec(),))
    assert datasource.supports_projection_pushdown() is False
    assert datasource.supports_predicate_pushdown() is False
    assert datasource.estimate_inmemory_data_size() is None
    assert len(datasource.get_read_tasks(parallelism=32)) == 1
    assert datasource.task_specs == (_spec(),)
    with pytest.raises(ValueError, match="at least one"):
        HiveServer2Datasource(())


def test_public_read_facade_plans_then_applies_strict_ray_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    datasource = HiveServer2Datasource((_spec(),))
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        api_module.HiveServer2Datasource,
        "from_options",
        lambda connection, read: datasource,
    )

    def strict(source: Any, **kwargs: Any) -> str:
        captured.update(source=source, **kwargs)
        return "dataset"

    monkeypatch.setattr(api_module, "read_datasource_strict", strict)

    result = api_module.read_hive(
        connection=HiveConnectionOptions(host="hs2.example"),
        read=object(),
        ray_remote_args={"num_cpus": 1},
    )

    assert result == "dataset"
    assert captured == {
        "source": datasource,
        "num_tasks": 1,
        "ray_remote_args": {"num_cpus": 1},
    }


def test_runtime_and_read_signature_mismatches_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(compat_module, "version", lambda package: "0")
    with pytest.raises(HiveCompatibilityError, match="requires Ray"):
        compat_module.validate_runtime()

    monkeypatch.setattr(compat_module, "validate_runtime", lambda: None)
    monkeypatch.setattr(
        compat_module.inspect,
        "signature",
        lambda value: __import__("inspect").Signature(),
    )
    with pytest.raises(HiveCompatibilityError, match="read_datasource signature"):
        compat_module.read_datasource_strict(object(), num_tasks=1, ray_remote_args=None)
