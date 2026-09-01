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

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

from scripts import release_evidence
from scripts.release_evidence import (
    EvidenceSelection,
    EvidenceValidationError,
    GitHubActionsClient,
    artifact_name,
    build_manifest,
    load_manifest,
    main,
    select_unique_evidence,
    validate_manifest,
    write_manifest,
)

SHA = "a" * 40
WORKFLOWS = {
    "ci": ".github/workflows/ci.yml",
    "hive-420": ".github/workflows/hive-integration.yml",
    "ray-cluster": ".github/workflows/ray-cluster-integration.yml",
}


def _manifest(profile: str = "ci") -> dict[str, Any]:
    return build_manifest(
        commit_sha=SHA,
        profile=profile,
        run_id=11,
        run_attempt=2,
        event="push",
        identity_source="workflow-matrix" if profile == "ci" else "measured-process",
        runtime={
            "python": ("3.12",),
            "ray": ("2.56.1",),
            "pyarrow": ("19.0.1",),
            "thrift": ("0.22.0",),
            "hive": () if profile == "ci" else ("4.2.0",),
        },
        images=() if profile == "ci" else ("example@sha256:" + "b" * 64,),
        suite="unit-contract" if profile == "ci" else profile,
        generated_at="2026-09-01T00:00:00Z",
    )


def test_manifest_round_trip_and_expected_identity(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    manifest = _manifest()
    write_manifest(path, manifest)

    loaded = load_manifest(path)
    validate_manifest(
        loaded,
        expected_commit=SHA,
        expected_profile="ci",
        expected_run_id=11,
    )
    assert json.loads(path.read_text(encoding="utf-8")) == manifest


def test_write_and_validate_cli_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "hive-420.json"
    runtime = tmp_path / "runtime.json"
    assert (
        main(
            [
                "capture-runtime",
                "--hive-version",
                "4.2.0",
                "--output",
                str(runtime),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "write",
                "--profile",
                "hive-420",
                "--commit",
                SHA,
                "--run-id",
                "7",
                "--run-attempt",
                "1",
                "--event",
                "push",
                "--identity-source",
                "measured-process",
                "--runtime-file",
                str(runtime),
                "--image",
                "apache/hive@sha256:" + "b" * 64,
                "--suite",
                "tests/integration/test_hive_420.py",
                "--generated-at",
                "2026-09-01T00:00:00Z",
                "--output",
                str(path),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "validate",
                str(path),
                "--expected-commit",
                SHA,
                "--expected-profile",
                "hive-420",
                "--expected-run-id",
                "7",
            ]
        )
        == 0
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", 2, "schema version"),
        ("commit_sha", "short", "commit SHA"),
        ("workflow_path", ".github/workflows/other.yml", "workflow path"),
        ("run_id", True, "run_id"),
        ("result", "failure", "not success"),
        ("generated_at", "2026-09-01", "must use UTC"),
    ],
)
def test_manifest_rejects_invalid_fields(field: str, value: object, message: str) -> None:
    manifest = _manifest()
    manifest[field] = value

    with pytest.raises(EvidenceValidationError, match=message):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="ci")


def test_manifest_rejects_duplicate_runtime_and_unknown_fields() -> None:
    manifest = _manifest()
    manifest["runtime"]["python"] = ["3.12", "3.12"]
    with pytest.raises(EvidenceValidationError, match=r"duplicate runtime\.python"):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="ci")

    manifest = _manifest()
    manifest["extra"] = "unexpected"
    with pytest.raises(EvidenceValidationError, match="fields do not match"):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="ci")

    with pytest.raises(EvidenceValidationError, match="run ID does not match"):
        validate_manifest(
            _manifest(),
            expected_commit=SHA,
            expected_profile="ci",
            expected_run_id=99,
        )


def test_manifest_requires_profile_runtime_and_digest_identity() -> None:
    manifest = _manifest()
    manifest["runtime"]["ray"] = []
    with pytest.raises(EvidenceValidationError, match=r"runtime\.ray must not be empty"):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="ci")

    manifest = _manifest("hive-420")
    manifest["runtime"]["hive"] = []
    with pytest.raises(EvidenceValidationError, match="requires a Hive version"):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="hive-420")

    manifest = _manifest("ray-cluster")
    manifest["images"] = ["rayproject/ray:latest"]
    with pytest.raises(EvidenceValidationError, match="invalid image identity"):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="ray-cluster")

    manifest = _manifest("hive-420")
    manifest["identity_source"] = "workflow-matrix"
    with pytest.raises(EvidenceValidationError, match="measured-process"):
        validate_manifest(manifest, expected_commit=SHA, expected_profile="hive-420")


def test_capture_runtime_reads_versions_from_current_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    versions = {
        "ray": "2.56.1",
        "pyarrow": "19.0.1",
        "thrift": "0.16.0",
    }
    monkeypatch.setattr(release_evidence.platform, "python_version", lambda: "3.12.12")
    monkeypatch.setattr(release_evidence.importlib.metadata, "version", versions.__getitem__)

    assert release_evidence.capture_runtime(hive_version="4.2.0") == {
        "python": ["3.12.12"],
        "ray": ["2.56.1"],
        "pyarrow": ["19.0.1"],
        "thrift": ["0.16.0"],
        "hive": ["4.2.0"],
    }

    def missing(name: str) -> str:
        raise release_evidence.importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(release_evidence.importlib.metadata, "version", missing)
    with pytest.raises(EvidenceValidationError, match="metadata is unavailable"):
        release_evidence.capture_runtime(hive_version="4.2.0")


class FakeActionsApi:
    def __init__(self) -> None:
        self.runs: list[Mapping[str, Any]] = []
        self.details: dict[int, Mapping[str, Any]] = {}
        self.artifacts: dict[tuple[int, str], list[Mapping[str, Any]]] = {}

    def add(
        self,
        profile: str,
        run_id: int,
        artifact_id: int,
        *,
        sha: str = SHA,
        conclusion: str = "success",
        expired: bool = False,
        path: str | None = None,
    ) -> None:
        self.runs.append({"id": run_id})
        self.details[run_id] = {
            "id": run_id,
            "head_sha": sha,
            "conclusion": conclusion,
            "path": path or WORKFLOWS[profile],
        }
        name = artifact_name(profile, SHA)
        self.artifacts[(run_id, name)] = [{"id": artifact_id, "name": name, "expired": expired}]

    def list_successful_runs(self, head_sha: str) -> Iterable[Mapping[str, Any]]:
        del head_sha
        return self.runs

    def get_run(self, run_id: int) -> Mapping[str, Any]:
        return self.details[run_id]

    def list_artifacts(self, run_id: int, name: str) -> Iterable[Mapping[str, Any]]:
        return self.artifacts.get((run_id, name), ())


def _complete_api() -> FakeActionsApi:
    api = FakeActionsApi()
    for index, profile in enumerate(WORKFLOWS, start=1):
        api.add(profile, index, index * 10)
    return api


def test_selector_returns_one_artifact_per_direct_workflow() -> None:
    assert select_unique_evidence(_complete_api(), SHA) == (
        EvidenceSelection("ci", 1, 10),
        EvidenceSelection("hive-420", 2, 20),
        EvidenceSelection("ray-cluster", 3, 30),
    )


def test_selector_ignores_reusable_and_wrong_sha_runs() -> None:
    api = _complete_api()
    api.add(
        "hive-420",
        4,
        40,
        path=".github/workflows/slow-integration.yml@refs/heads/master",
    )
    api.add("ci", 5, 50, sha="b" * 40)
    api.add("ray-cluster", 6, 60, conclusion="failure")
    api.add("hive-420", 7, 70, conclusion="cancelled")

    assert select_unique_evidence(api, SHA)[1] == EvidenceSelection("hive-420", 2, 20)


def test_client_requires_token_and_https_origin() -> None:
    with pytest.raises(EvidenceValidationError, match="GH_TOKEN"):
        GitHubActionsClient(
            api_url="https://api.github.com",
            repository="owner/repo",
            token="",
        )
    with pytest.raises(EvidenceValidationError, match="HTTPS origin"):
        GitHubActionsClient(
            api_url="http://api.github.com",
            repository="owner/repo",
            token="secret",
        )


@pytest.mark.parametrize("profile", tuple(WORKFLOWS))
def test_selector_rejects_missing_or_expired_profile(profile: str) -> None:
    api = _complete_api()
    target = next(item for item in api.runs if item["id"] == list(WORKFLOWS).index(profile) + 1)
    api.runs.remove(target)
    with pytest.raises(EvidenceValidationError, match=f"no successful {profile}"):
        select_unique_evidence(api, SHA)

    api = _complete_api()
    run_id = list(WORKFLOWS).index(profile) + 1
    name = artifact_name(profile, SHA)
    api.artifacts[(run_id, name)][0] = {
        "id": run_id * 10,
        "name": name,
        "expired": True,
    }
    with pytest.raises(EvidenceValidationError, match=f"no successful {profile}"):
        select_unique_evidence(api, SHA)


def test_selector_rejects_duplicate_runs_and_artifacts() -> None:
    api = _complete_api()
    api.add("ci", 9, 90)
    with pytest.raises(EvidenceValidationError, match="found 2"):
        select_unique_evidence(api, SHA)

    api = _complete_api()
    name = artifact_name("ci", SHA)
    api.artifacts[(1, name)].append({"id": 99, "name": name, "expired": False})
    with pytest.raises(EvidenceValidationError, match="multiple active ci"):
        select_unique_evidence(api, SHA)


def test_selector_rejects_invalid_sha_and_api_ids() -> None:
    with pytest.raises(EvidenceValidationError, match="full lowercase"):
        select_unique_evidence(_complete_api(), "short")

    api = _complete_api()
    api.runs[0] = {"id": True}
    with pytest.raises(EvidenceValidationError, match="invalid id"):
        select_unique_evidence(api, SHA)


def test_client_paginates_and_rejects_malformed_collections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubActionsClient(
        api_url="https://api.example.invalid",
        repository="owner/repo",
        token="secret",
    )
    calls: list[str] = []

    def pages(path: str, **parameters: str) -> Mapping[str, Any]:
        del path
        calls.append(parameters["page"])
        if parameters["page"] == "1":
            return {"workflow_runs": [{"id": index + 1} for index in range(100)]}
        return {"workflow_runs": [{"id": 101}]}

    monkeypatch.setattr(client, "_get", pages)
    assert len(tuple(client.list_successful_runs(SHA))) == 101
    assert calls == ["1", "2"]

    monkeypatch.setattr(client, "_get", lambda *args, **kwargs: {"workflow_runs": "bad"})
    with pytest.raises(EvidenceValidationError, match="invalid collection"):
        tuple(client.list_successful_runs(SHA))


def test_select_cli_writes_stable_github_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ClientFactory:
        def __init__(self, **kwargs: str) -> None:
            assert kwargs["token"] == "secret"

        def list_successful_runs(self, head_sha: str) -> Iterable[Mapping[str, Any]]:
            return _complete_api().list_successful_runs(head_sha)

        def get_run(self, run_id: int) -> Mapping[str, Any]:
            return _complete_api().get_run(run_id)

        def list_artifacts(self, run_id: int, name: str) -> Iterable[Mapping[str, Any]]:
            return _complete_api().list_artifacts(run_id, name)

    monkeypatch.setattr(release_evidence, "GitHubActionsClient", ClientFactory)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GH_TOKEN", "secret")
    output = tmp_path / "github-output"

    assert main(["select", "--candidate-sha", SHA, "--github-output", str(output)]) == 0
    assert output.read_text(encoding="utf-8").splitlines() == [
        "ci_run_id=1",
        "ci_artifact_id=10",
        "hive_420_run_id=2",
        "hive_420_artifact_id=20",
        "ray_cluster_run_id=3",
        "ray_cluster_artifact_id=30",
    ]
