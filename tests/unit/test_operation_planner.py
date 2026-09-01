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

from collections import deque

import pyarrow as pa
import pytest

from ray_hive import (
    HiveCancellationError,
    HiveConfigurationError,
    HiveConnectionOptions,
    HivePlanningError,
    HiveQueryError,
    HiveReadOptions,
    HiveTableIdentifier,
    HiveTransportError,
    SecretRef,
)
from ray_hive._capabilities import CapabilityState, HiveCapabilities
from ray_hive._credentials import HiveCredentials
from ray_hive._hs2.operation import HiveOperation
from ray_hive._hs2.protocol import (
    HiveColumnSchema,
    HiveResultSchema,
    HiveType,
    HiveTypeKind,
    OperationHandle,
    OperationState,
    OperationStatus,
    SessionHandle,
)
from ray_hive._hs2.session import HiveSession
from ray_hive._planner import HivePlanner, render_table_query
from ray_hive.sql import col
from tests._fakes import FakeFactory, FakeRPC


def _schema() -> HiveResultSchema:
    return HiveResultSchema((HiveColumnSchema("id", HiveType(HiveTypeKind.BIGINT), position=1),))


def test_operation_polls_then_closes_idempotently() -> None:
    rpc = FakeRPC(
        _schema(),
        statuses=deque(
            [
                OperationStatus(OperationState.PENDING),
                OperationStatus(OperationState.RUNNING),
                OperationStatus(OperationState.FINISHED),
            ]
        ),
    )
    operation = HiveOperation(rpc, OperationHandle(object()))

    operation.wait_until_finished(deadline=10**20, poll_interval=0)
    operation.cancel()
    operation.cancel()
    operation.close()
    operation.close()

    assert rpc.events.count("cancel_operation") == 1
    assert rpc.events.count("close_operation") == 1


@pytest.mark.parametrize(
    ("status", "exception"),
    [
        (OperationStatus(OperationState.CANCELLED), HiveCancellationError),
        (OperationStatus(OperationState.TIMED_OUT), HiveQueryError),
        (
            OperationStatus(OperationState.ERROR, sql_state="42000", error_code=40000),
            HivePlanningError,
        ),
        (
            OperationStatus(OperationState.ERROR, sql_state="HY000", error_code=1),
            HiveQueryError,
        ),
    ],
)
def test_operation_terminal_failures_are_classified(
    status: OperationStatus, exception: type[Exception]
) -> None:
    rpc = FakeRPC(_schema(), statuses=deque([status]))
    operation = HiveOperation(rpc, OperationHandle(object()))
    with pytest.raises(exception):
        operation.wait_until_finished(deadline=10**20, poll_interval=0)


def test_operation_failure_includes_sanitized_server_detail() -> None:
    status = OperationStatus(
        OperationState.ERROR,
        sql_state="42S02",
        safe_message="Table not found",
    )
    operation = HiveOperation(
        FakeRPC(_schema(), statuses=deque([status])),
        OperationHandle(object()),
    )

    with pytest.raises(HivePlanningError, match="Table not found"):
        operation.wait_until_finished(deadline=10**20, poll_interval=0)


def test_operation_deadline_does_not_queue_cancel_behind_status_polling() -> None:
    rpc = FakeRPC(_schema())
    operation = HiveOperation(rpc, OperationHandle(object()))

    with pytest.raises(HiveCancellationError, match="deadline"):
        operation.wait_until_finished(deadline=0, poll_interval=0)

    assert "cancel_operation" not in rpc.events


def test_table_query_is_fully_qualified_and_schema_query_is_limit_zero() -> None:
    read = HiveReadOptions(
        table=HiveTableIdentifier("analytics", "event`log"),
        columns=["event_id", "kind"],
        filter=col("event_id").ge(10),
        unsafe_where_sql="tenant_id = current_user()",
        limit=50,
    )

    assert render_table_query(read) == (
        "SELECT `event_id`, `kind` FROM `analytics`.`event``log` "
        "WHERE (`event_id` >= 10) AND ((tenant_id = current_user())) LIMIT 50"
    )
    assert render_table_query(read, schema_only=True).endswith("LIMIT 0")


def test_table_planning_uses_hs2_only_and_closes_all_resources() -> None:
    rpc = FakeRPC(_schema())
    planner = HivePlanner(lambda driver: FakeFactory(rpc))
    connection = HiveConnectionOptions(host="hs2.example")
    read = HiveReadOptions(table=HiveTableIdentifier("db", "events"), columns=["id"])

    specs = planner.plan(connection, read)

    assert len(specs) == 1
    assert specs[0].statement == "SELECT `id` FROM `db`.`events`"
    assert specs[0].expected_arrow_schema.field("id").type == pa.int64()
    assert rpc.events == [
        "connect",
        "open_session",
        "execute_statement",
        "get_operation_status",
        "get_result_schema",
        "close_operation",
        "close_session",
        "close_transport",
    ]


def test_authenticated_table_planning_resolves_credentials_on_the_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HIVE_PASSWORD", "driver-and-worker-secret")
    rpc = FakeRPC(_schema())
    planner = HivePlanner(lambda driver: FakeFactory(rpc))
    connection = HiveConnectionOptions(
        host="hs2.example",
        auth="LDAP",
        credentials=SecretRef.env("HIVE_PASSWORD"),
    )

    planner.plan(connection, HiveReadOptions(table="db.events"))

    assert rpc.opened_credentials is not None
    assert rpc.opened_credentials.password == "driver-and-worker-secret"


def test_planner_preserves_deadline_error_when_cancel_rpc_fails() -> None:
    class CancelFailureRPC(FakeRPC):
        def cancel_operation(self, operation: OperationHandle) -> None:
            super().cancel_operation(operation)
            raise HiveTransportError("cancel transport failed")

    rpc = CancelFailureRPC(
        _schema(),
        statuses=deque([OperationStatus(OperationState.RUNNING)]),
    )
    planner = HivePlanner(lambda driver: FakeFactory(rpc))
    read = HiveReadOptions(table="db.events", query_timeout=0.0001)

    with pytest.raises(HiveCancellationError, match="deadline") as caught:
        planner.plan(HiveConnectionOptions(host="hs2.example"), read)

    assert any("cleanup" in note.lower() for note in caught.value.__notes__)
    assert "cancel_operation" in rpc.events


def test_raw_sql_requires_explicit_schema_and_never_plans_a_driver_query() -> None:
    resolver_calls = 0

    def resolver(driver: str) -> FakeFactory:
        nonlocal resolver_calls
        resolver_calls += 1
        return FakeFactory(FakeRPC(_schema()))

    planner = HivePlanner(resolver)
    connection = HiveConnectionOptions(host="hs2.example")
    with pytest.raises(HivePlanningError, match="explicit Arrow schema"):
        planner.plan(connection, HiveReadOptions(sql="SELECT 1"))

    specs = planner.plan(
        connection,
        HiveReadOptions(sql="SELECT 1 AS id", schema=pa.schema([("id", pa.int32())])),
    )
    assert len(specs) == 1
    assert resolver_calls == 0
    assert "SELECT 1" not in repr(specs[0])


def test_rejected_sql_cannot_reach_planner_connection_resolution() -> None:
    resolver_calls = 0

    def resolver(driver: str) -> FakeFactory:
        nonlocal resolver_calls
        resolver_calls += 1
        return FakeFactory(FakeRPC(_schema()))

    planner = HivePlanner(resolver)

    def construct_and_plan() -> None:
        invalid = HiveReadOptions(sql="DROP TABLE db.events")
        planner.plan(HiveConnectionOptions(host="hs2.example"), invalid)

    with pytest.raises(HiveConfigurationError, match="top-level SELECT"):
        construct_and_plan()

    assert resolver_calls == 0


def test_independent_queries_create_one_query_per_explicit_predicate() -> None:
    rpc = FakeRPC(_schema())
    planner = HivePlanner(lambda driver: FakeFactory(rpc))
    read = HiveReadOptions(
        table=HiveTableIdentifier("db", "events"),
        consistency="independent_queries",
        split_predicates=[col("id").lt(10), col("id").ge(10)],
    )

    specs = planner.plan(HiveConnectionOptions(host="hs2.example"), read)

    assert len(specs) == 2
    assert specs[0].split_index == 0
    assert "`id` < 10" in specs[0].statement
    assert "`id` >= 10" in specs[1].statement


def test_session_closes_handle_and_transport_when_core_capability_is_missing() -> None:
    capabilities = HiveCapabilities(
        protocol_version=5,
        columnar_rowsets=CapabilityState.UNSUPPORTED,
        binary_columns=CapabilityState.UNSUPPORTED,
        cancel_operation=CapabilityState.SUPPORTED,
        operation_log=CapabilityState.UNKNOWN,
    )

    class UnsupportedRPC(FakeRPC):
        def open_session(
            self,
            connection: HiveConnectionOptions,
            credentials: HiveCredentials,
        ) -> tuple[SessionHandle, HiveCapabilities]:
            handle, _ = super().open_session(connection, credentials)
            return handle, capabilities

    rpc = UnsupportedRPC(_schema())

    with pytest.raises(Exception, match="protocol V6"):
        HiveSession.open(rpc, HiveConnectionOptions(host="hs2.example"))

    assert rpc.events[-2:] == ["close_session", "close_transport"]
