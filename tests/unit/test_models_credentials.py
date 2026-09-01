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

import math
import pickle
from dataclasses import FrozenInstanceError

import pyarrow as pa
import pytest

from ray_hive import (
    HiveAuthenticationError,
    HiveConfigurationError,
    HiveConnectionOptions,
    HiveReadOptions,
    HiveTableIdentifier,
    HiveTLSOptions,
    SecretRef,
)
from ray_hive._credentials import CredentialContext, EnvironmentCredentialProvider
from ray_hive._models import HiveConsistency, HiveDriver, HiveTransport
from ray_hive.sql import Column, Predicate, col


def test_connection_normalizes_and_binds_configuration() -> None:
    connection = HiveConnectionOptions(
        host="hs2.example",
        driver="native",
        transport="binary",
        auth="nosasl",
        credentials=SecretRef.env("HIVE_PASSWORD"),
        session_options={"hive.local.time.zone": "UTC"},
    )

    assert connection.driver is HiveDriver.NATIVE
    assert connection.transport is HiveTransport.BINARY
    assert connection.auth == "NOSASL"
    assert connection.normalized_session_options == (("hive.local.time.zone", "UTC"),)
    assert connection.credentials is not None
    assert connection.credentials.allowed_context == connection.credential_context


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"host": ""}, "host"),
        ({"host": 1}, "host"),
        ({"host": "hs2", "port": 0}, "port"),
        ({"host": "hs2", "username": "bad\x00name"}, "username"),
        ({"host": "hs2", "auth": "unknown"}, "authentication"),
        ({"host": "hs2", "auth": 1}, "authentication mode"),
        ({"host": "hs2", "port": True}, "port"),
        ({"host": "hs2", "tls": object()}, "HiveTLSOptions"),
        ({"host": "hs2", "connect_timeout": math.nan}, "timeouts"),
        (
            {
                "host": "hs2",
                "session_options": {"hive.exec.scratchdir": "unapproved"},
            },
            "session option",
        ),
        (
            {
                "host": "hs2",
                "session_options": {"hive.server2.thrift.resultset.serialize.in.tasks": "true"},
            },
            "binaryColumns",
        ),
        (
            {"host": "hs2", "transport": "http", "auth": "ldap"},
            "requires TLS",
        ),
        ({"host": "hs2", "credentials": object()}, "CredentialProvider"),
    ],
)
def test_connection_rejects_invalid_configuration(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(HiveConfigurationError, match=message):
        HiveConnectionOptions(**kwargs)


def test_tls_configuration_is_fail_closed() -> None:
    with pytest.raises(HiveConfigurationError, match="CA file"):
        HiveTLSOptions(enabled=True, verify=False, ca_file="ca.pem")
    with pytest.raises(HiveConfigurationError, match="requires TLS"):
        HiveTLSOptions(server_name="hs2.example")
    with pytest.raises(HiveConfigurationError, match="non-empty string"):
        HiveTLSOptions(enabled=True, ca_file="")


def test_secret_is_resolved_only_for_bound_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HIVE_PASSWORD", "top-secret")
    connection = HiveConnectionOptions(
        host="hs2.example", credentials=SecretRef.env("HIVE_PASSWORD")
    )
    assert connection.credentials is not None

    credentials = connection.credentials.resolve(connection.credential_context)
    assert credentials.password == "top-secret"
    wrong_context = CredentialContext("other:10000", "native", "NOSASL", None)
    with pytest.raises(HiveAuthenticationError, match="not bound"):
        connection.credentials.resolve(wrong_context)


def test_environment_provider_resolves_multiple_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HIVE_PASSWORD", "password-value")
    monkeypatch.setenv("HIVE_TOKEN", "token-value")
    provider = EnvironmentCredentialProvider(
        username="reader",
        password=SecretRef.env("HIVE_PASSWORD"),
        token=SecretRef.env("HIVE_TOKEN"),
    )
    connection = HiveConnectionOptions(host="hs2.example", credentials=provider)
    assert connection.credentials is not None

    credentials = connection.credentials.resolve(connection.credential_context)
    assert credentials.username == "reader"
    assert credentials.password == "password-value"
    assert credentials.token == "token-value"


def test_serialized_configuration_and_repr_do_not_contain_secret_or_literal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "credential-material-that-must-not-leak"
    literal = "customer-private-value"
    monkeypatch.setenv("HIVE_PASSWORD", secret)
    connection = HiveConnectionOptions(
        host="hs2.example", credentials=SecretRef.env("HIVE_PASSWORD")
    )
    read = HiveReadOptions(
        table=HiveTableIdentifier("db", "events"), filter=col("kind").eq(literal)
    )

    serialized = pickle.dumps((connection, read))
    assert secret.encode() not in serialized
    assert secret not in repr(connection)
    assert literal not in repr(read)
    assert literal not in repr(read.filter)


def test_models_are_immutable() -> None:
    connection = HiveConnectionOptions(host="hs2.example")
    with pytest.raises(FrozenInstanceError):
        connection.__setattr__("host", "other")


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"table": HiveTableIdentifier("db", "t"), "sql": "SELECT 1"},
        {"sql": "SELECT 1", "columns": ["a"]},
        {"sql": "SELECT 1", "limit": 1},
        {
            "sql": "SELECT 1",
            "schema": pa.schema([("value", pa.int32())]),
            "consistency": "independent_queries",
            "split_predicates": [col("value").ge(0)],
        },
        {"table": HiveTableIdentifier("db", "t"), "limit": 0},
        {"table": HiveTableIdentifier("db", "t"), "columns": []},
        {"table": HiveTableIdentifier("db", "t"), "filter": object()},
        {"table": HiveTableIdentifier("db", "t"), "schema": pa.schema([])},
        {"table": HiveTableIdentifier("db", "t"), "query_timeout": math.nan},
        {
            "table": HiveTableIdentifier("db", "t"),
            "consistency": "independent_queries",
        },
        {
            "table": HiveTableIdentifier("db", "t"),
            "consistency": "strict",
            "split_predicates": [col("id").eq(1)],
        },
        {
            "table": HiveTableIdentifier("db", "t"),
            "consistency": "independent_queries",
            "split_predicates": [object()],
        },
    ],
)
def test_read_options_reject_ambiguous_or_unsafe_combinations(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(HiveConfigurationError):
        HiveReadOptions(**kwargs)


def test_read_options_normalize_table_and_experimental_splits() -> None:
    read = HiveReadOptions(
        table="db.events",
        columns=["id", "name"],
        consistency="independent_queries",
        split_predicates=[col("id").lt(10), col("id").ge(10)],
    )

    assert read.table == HiveTableIdentifier("db", "events")
    assert read.columns == ("id", "name")
    assert read.normalized_consistency is HiveConsistency.INDEPENDENT_QUERIES


def test_read_options_validate_and_normalize_sql_before_planning() -> None:
    raw = HiveReadOptions(sql="  SELECT 1  ")
    predicate = HiveReadOptions(table="db.events", unsafe_where_sql="  id = 1  ")

    assert raw.sql == "SELECT 1"
    assert predicate.unsafe_where_sql == "id = 1"

    with pytest.raises(HiveConfigurationError, match="top-level SELECT"):
        HiveReadOptions(sql="DROP TABLE db.events")
    with pytest.raises(HiveConfigurationError, match="outer parenthesis"):
        HiveReadOptions(table="db.events", unsafe_where_sql=") OR TRUE OR (")
    with pytest.raises(HiveConfigurationError, match="variable substitution"):
        HiveReadOptions(table=HiveTableIdentifier("db", "${env:TABLE}"))
    with pytest.raises(HiveConfigurationError, match="variable substitution"):
        HiveReadOptions(table="db.events", columns=["${env:COLUMN}"])

    class CustomPredicate(Predicate):
        def to_sql(self) -> str:
            return "TRUE); DROP TABLE db.events; --"

    with pytest.raises(HiveConfigurationError, match="Custom Predicate"):
        HiveReadOptions(table="db.events", filter=CustomPredicate())


@pytest.mark.parametrize(
    "options",
    [
        {"filter": "comparison"},
        {
            "consistency": "independent_queries",
            "split_predicates": "membership",
        },
    ],
)
def test_read_options_reject_custom_columns_in_all_predicate_paths(
    options: dict[str, object],
) -> None:
    class CustomColumn(Column):
        def to_sql(self) -> str:
            return "coalesce(`id`, 0)"

    custom = CustomColumn("id")
    normalized = dict(options)
    if normalized.get("filter") == "comparison":
        normalized["filter"] = custom.eq(1)
    if normalized.get("split_predicates") == "membership":
        normalized["split_predicates"] = [custom.isin([1, 2])]

    with pytest.raises(HiveConfigurationError, match="Custom Column"):
        HiveReadOptions(table="db.events", **normalized)
