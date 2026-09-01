#!/usr/bin/env python3
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

"""Write, validate, and select exact-SHA GitHub Actions release evidence."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

_SCHEMA_VERSION = 1
_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_IMAGE_ID = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")
_PAGE_SIZE = 100
_PROFILES = {
    "ci": ".github/workflows/ci.yml",
    "hive-420": ".github/workflows/hive-integration.yml",
    "ray-cluster": ".github/workflows/ray-cluster-integration.yml",
}
_RUNTIME_KEYS = ("python", "ray", "pyarrow", "thrift", "hive")
_IDENTITY_SOURCES = ("workflow-matrix", "measured-process")
_MANIFEST_KEYS = {
    "schema_version",
    "commit_sha",
    "profile",
    "workflow_path",
    "run_id",
    "run_attempt",
    "event",
    "result",
    "identity_source",
    "runtime",
    "images",
    "suite",
    "generated_at",
}


class EvidenceValidationError(RuntimeError):
    """Release evidence is missing, ambiguous, expired, or malformed."""


def _full_sha(value: Any, name: str = "commit SHA") -> str:
    if not isinstance(value, str) or _COMMIT_SHA.fullmatch(value) is None:
        raise EvidenceValidationError(f"{name} must be a full lowercase commit SHA")
    return value


def _positive_integer(data: Mapping[str, Any], name: str) -> int:
    value = data.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise EvidenceValidationError(f"evidence API returned an invalid {name}")
    return value


def _nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceValidationError(f"evidence manifest has an invalid {name}")
    return value


def _string_list(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise EvidenceValidationError(f"evidence manifest has an invalid {name}")
    if len(set(value)) != len(value):
        raise EvidenceValidationError(f"evidence manifest has duplicate {name}")
    return tuple(value)


def _generated_at(value: Any) -> str:
    timestamp = _nonempty_string(value, "generated_at")
    if not timestamp.endswith("Z"):
        raise EvidenceValidationError("evidence manifest generated_at must use UTC")
    try:
        parsed = datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00")
    except ValueError:
        raise EvidenceValidationError("evidence manifest has an invalid generated_at") from None
    if parsed.tzinfo != UTC:
        raise EvidenceValidationError("evidence manifest generated_at must use UTC")
    return timestamp


def artifact_name(profile: str, commit_sha: str) -> str:
    """Return the immutable artifact name for a profile and commit."""

    if profile not in _PROFILES:
        raise EvidenceValidationError(f"unsupported evidence profile: {profile!r}")
    return f"ray-hive-evidence-{profile}-{_full_sha(commit_sha)}"


def build_manifest(
    *,
    commit_sha: str,
    profile: str,
    run_id: int,
    run_attempt: int,
    event: str,
    identity_source: str,
    runtime: Mapping[str, Sequence[str]],
    images: Sequence[str],
    suite: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build and validate one successful workflow evidence manifest."""

    if profile not in _PROFILES:
        raise EvidenceValidationError(f"unsupported evidence profile: {profile!r}")
    manifest: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "commit_sha": _full_sha(commit_sha),
        "profile": profile,
        "workflow_path": _PROFILES[profile],
        "run_id": run_id,
        "run_attempt": run_attempt,
        "event": event,
        "result": "success",
        "identity_source": identity_source,
        "runtime": {key: list(runtime.get(key, ())) for key in _RUNTIME_KEYS},
        "images": list(images),
        "suite": suite,
        "generated_at": generated_at
        or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    validate_manifest(manifest, expected_commit=commit_sha, expected_profile=profile)
    return manifest


def validate_manifest(
    manifest: Mapping[str, Any],
    *,
    expected_commit: str,
    expected_profile: str,
    expected_run_id: int | None = None,
) -> None:
    """Validate the complete manifest identity and schema."""

    if set(manifest) != _MANIFEST_KEYS:
        raise EvidenceValidationError("evidence manifest fields do not match schema version 1")
    if manifest.get("schema_version") != _SCHEMA_VERSION:
        raise EvidenceValidationError("evidence manifest has an unsupported schema version")
    commit_sha = _full_sha(manifest.get("commit_sha"), "manifest commit SHA")
    if commit_sha != _full_sha(expected_commit, "expected commit SHA"):
        raise EvidenceValidationError("evidence manifest commit does not match candidate")
    profile = _nonempty_string(manifest.get("profile"), "profile")
    if profile != expected_profile or profile not in _PROFILES:
        raise EvidenceValidationError("evidence manifest profile does not match")
    if manifest.get("workflow_path") != _PROFILES[profile]:
        raise EvidenceValidationError("evidence manifest workflow path does not match profile")
    run_id = _positive_integer(manifest, "run_id")
    _positive_integer(manifest, "run_attempt")
    if expected_run_id is not None and run_id != expected_run_id:
        raise EvidenceValidationError("evidence manifest run ID does not match selected run")
    _nonempty_string(manifest.get("event"), "event")
    if manifest.get("result") != "success":
        raise EvidenceValidationError("evidence manifest result is not success")
    identity_source = _nonempty_string(manifest.get("identity_source"), "identity_source")
    expected_source = "workflow-matrix" if profile == "ci" else "measured-process"
    if identity_source != expected_source:
        raise EvidenceValidationError(
            f"{profile} evidence requires identity_source={expected_source!r}"
        )
    runtime = manifest.get("runtime")
    if not isinstance(runtime, Mapping) or set(runtime) != set(_RUNTIME_KEYS):
        raise EvidenceValidationError("evidence manifest runtime fields are invalid")
    versions = {key: _string_list(runtime[key], f"runtime.{key}") for key in _RUNTIME_KEYS}
    for key in ("python", "ray", "pyarrow", "thrift"):
        if not versions[key]:
            raise EvidenceValidationError(f"evidence manifest runtime.{key} must not be empty")
    if profile != "ci" and not versions["hive"]:
        raise EvidenceValidationError("real-infrastructure evidence requires a Hive version")
    images = _string_list(manifest.get("images"), "images")
    if profile != "ci" and not images:
        raise EvidenceValidationError("real-infrastructure evidence requires image identities")
    if any(_IMAGE_ID.fullmatch(image) is None for image in images):
        raise EvidenceValidationError("evidence manifest has an invalid image identity")
    _nonempty_string(manifest.get("suite"), "suite")
    _generated_at(manifest.get("generated_at"))


def write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_manifest(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise EvidenceValidationError("evidence manifest could not be read") from None
    if not isinstance(payload, Mapping):
        raise EvidenceValidationError("evidence manifest root must be an object")
    return payload


def capture_runtime(*, hive_version: str) -> dict[str, list[str]]:
    """Capture client runtime versions from the current Python process."""

    hive = _nonempty_string(hive_version, "hive version")
    try:
        packages = {name: importlib.metadata.version(name) for name in ("ray", "pyarrow", "thrift")}
    except importlib.metadata.PackageNotFoundError as exc:
        raise EvidenceValidationError("required runtime package metadata is unavailable") from exc
    return {
        "python": [platform.python_version()],
        "ray": [packages["ray"]],
        "pyarrow": [packages["pyarrow"]],
        "thrift": [packages["thrift"]],
        "hive": [hive],
    }


def load_runtime_facts(path: Path) -> Mapping[str, Sequence[str]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise EvidenceValidationError("runtime facts could not be read") from None
    if not isinstance(payload, Mapping) or set(payload) != set(_RUNTIME_KEYS):
        raise EvidenceValidationError("runtime facts fields are invalid")
    return {key: _string_list(payload[key], f"runtime.{key}") for key in _RUNTIME_KEYS}


def write_runtime_facts(path: str, runtime: Mapping[str, Sequence[str]]) -> None:
    content = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
    if path == "-":
        print(content, end="")
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")


class ActionsApi(Protocol):
    """Subset of the GitHub Actions API needed by the selector."""

    def list_successful_runs(self, head_sha: str) -> Iterable[Mapping[str, Any]]: ...

    def get_run(self, run_id: int) -> Mapping[str, Any]: ...

    def list_artifacts(self, run_id: int, name: str) -> Iterable[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class EvidenceSelection:
    """Identity of one selected profile artifact."""

    profile: str
    run_id: int
    artifact_id: int


def _workflow_path(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.split("@", 1)[0]


def select_unique_evidence(
    api: ActionsApi,
    candidate_sha: str,
) -> tuple[EvidenceSelection, ...]:
    """Select exactly one active artifact for each required workflow profile."""

    candidate_sha = _full_sha(candidate_sha)
    matches: dict[str, list[EvidenceSelection]] = {profile: [] for profile in _PROFILES}
    for summary in api.list_successful_runs(candidate_sha):
        run_id = _positive_integer(summary, "id")
        run = api.get_run(run_id)
        if run.get("head_sha") != candidate_sha or run.get("conclusion") != "success":
            continue
        workflow_path = _workflow_path(run.get("path"))
        profile = next(
            (item for item, path in _PROFILES.items() if workflow_path == path),
            None,
        )
        if profile is None:
            continue
        name = artifact_name(profile, candidate_sha)
        artifacts = [
            artifact
            for artifact in api.list_artifacts(run_id, name)
            if artifact.get("name") == name and artifact.get("expired") is False
        ]
        if len(artifacts) > 1:
            raise EvidenceValidationError(
                f"multiple active {profile} evidence artifacts exist for run {run_id}"
            )
        if len(artifacts) == 1:
            matches[profile].append(
                EvidenceSelection(
                    profile=profile,
                    run_id=run_id,
                    artifact_id=_positive_integer(artifacts[0], "id"),
                )
            )

    selections: list[EvidenceSelection] = []
    for profile in _PROFILES:
        profile_matches = matches[profile]
        if not profile_matches:
            raise EvidenceValidationError(
                f"no successful {profile} evidence exists for {candidate_sha}"
            )
        if len(profile_matches) != 1:
            raise EvidenceValidationError(
                f"expected one successful {profile} evidence result for {candidate_sha}, "
                f"found {len(profile_matches)}"
            )
        selections.append(profile_matches[0])
    return tuple(selections)


class GitHubActionsClient:
    """Small fail-closed GitHub Actions REST client."""

    def __init__(self, *, api_url: str, repository: str, token: str) -> None:
        if not repository or not token:
            raise EvidenceValidationError("GITHUB_REPOSITORY and GH_TOKEN are required")
        parsed = urllib.parse.urlparse(api_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise EvidenceValidationError("GITHUB_API_URL must be an HTTPS origin")
        self._base_url = api_url.rstrip("/")
        self._repository = repository
        self._token = token

    def _get(self, path: str, **parameters: str) -> Mapping[str, Any]:
        query = urllib.parse.urlencode(parameters)
        url = f"{self._base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(  # noqa: S310 -- API origin is HTTPS-only.
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(  # noqa: S310 -- API origin is HTTPS-only.
                request,
                timeout=30,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, urllib.error.URLError):
            raise EvidenceValidationError("GitHub Actions API request failed") from None
        if not isinstance(payload, Mapping):
            raise EvidenceValidationError("GitHub Actions API returned an invalid response")
        return payload

    def _list(self, path: str, key: str, **parameters: str) -> Iterator[Mapping[str, Any]]:
        page = 1
        while True:
            payload = self._get(
                path,
                **parameters,
                page=str(page),
                per_page=str(_PAGE_SIZE),
            )
            values = payload.get(key)
            if not isinstance(values, list):
                raise EvidenceValidationError("GitHub Actions API returned an invalid collection")
            for value in values:
                if not isinstance(value, Mapping):
                    raise EvidenceValidationError(
                        "GitHub Actions API returned an invalid collection item"
                    )
                yield value
            if len(values) < _PAGE_SIZE:
                return
            page += 1

    def list_successful_runs(self, head_sha: str) -> Iterator[Mapping[str, Any]]:
        return self._list(
            f"/repos/{self._repository}/actions/runs",
            "workflow_runs",
            head_sha=head_sha,
            status="success",
        )

    def get_run(self, run_id: int) -> Mapping[str, Any]:
        return self._get(f"/repos/{self._repository}/actions/runs/{run_id}")

    def list_artifacts(self, run_id: int, name: str) -> Iterator[Mapping[str, Any]]:
        return self._list(
            f"/repos/{self._repository}/actions/runs/{run_id}/artifacts",
            "artifacts",
            name=name,
        )


def _append_github_output(path: str, values: Mapping[str, int]) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


def _runtime_from_options(options: argparse.Namespace) -> dict[str, Sequence[str]]:
    declared = {
        "python": options.python_version,
        "ray": options.ray_version,
        "pyarrow": options.pyarrow_version,
        "thrift": options.thrift_version,
        "hive": options.hive_version,
    }
    has_declared_versions = any(declared.values())
    if options.runtime_file is not None:
        if options.identity_source != "measured-process" or has_declared_versions:
            raise EvidenceValidationError(
                "runtime files require measured-process identity without declared versions"
            )
        return dict(load_runtime_facts(options.runtime_file))
    if options.identity_source != "workflow-matrix":
        raise EvidenceValidationError("measured-process identity requires a runtime facts file")
    return declared


def _add_write_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("write", help="write one successful evidence manifest")
    parser.add_argument("--profile", choices=tuple(_PROFILES), required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    parser.add_argument("--event", required=True)
    parser.add_argument("--identity-source", choices=_IDENTITY_SOURCES, required=True)
    parser.add_argument("--runtime-file", type=Path)
    parser.add_argument("--python-version", action="append", default=[])
    parser.add_argument("--ray-version", action="append", default=[])
    parser.add_argument("--pyarrow-version", action="append", default=[])
    parser.add_argument("--thrift-version", action="append", default=[])
    parser.add_argument("--hive-version", action="append", default=[])
    parser.add_argument("--image", action="append", default=[])
    parser.add_argument("--suite", required=True)
    parser.add_argument("--generated-at", default="")
    parser.add_argument("--output", type=Path, required=True)


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_write_parser(subparsers)

    capture = subparsers.add_parser(
        "capture-runtime",
        help="capture runtime versions from the current Python process",
    )
    capture.add_argument("--hive-version", required=True)
    capture.add_argument("--output", required=True)

    validate = subparsers.add_parser("validate", help="validate a downloaded manifest")
    validate.add_argument("manifest", type=Path)
    validate.add_argument("--expected-commit", required=True)
    validate.add_argument("--expected-profile", choices=tuple(_PROFILES), required=True)
    validate.add_argument("--expected-run-id", type=int, required=True)

    select = subparsers.add_parser("select", help="select exact-SHA release evidence")
    select.add_argument("--candidate-sha", required=True)
    select.add_argument("--github-output", default="")

    options = parser.parse_args(arguments)
    try:
        if options.command == "capture-runtime":
            runtime = capture_runtime(hive_version=options.hive_version)
            write_runtime_facts(options.output, runtime)
            return 0
        if options.command == "write":
            manifest = build_manifest(
                commit_sha=options.commit,
                profile=options.profile,
                run_id=options.run_id,
                run_attempt=options.run_attempt,
                event=options.event,
                identity_source=options.identity_source,
                runtime=_runtime_from_options(options),
                images=options.image,
                suite=options.suite,
                generated_at=options.generated_at or None,
            )
            write_manifest(options.output, manifest)
            print(f"wrote {options.profile} evidence for {options.commit}")
            return 0
        if options.command == "validate":
            manifest = load_manifest(options.manifest)
            validate_manifest(
                manifest,
                expected_commit=options.expected_commit,
                expected_profile=options.expected_profile,
                expected_run_id=options.expected_run_id,
            )
            print(f"validated {options.expected_profile} evidence")
            return 0

        client = GitHubActionsClient(
            api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
            repository=os.environ.get("GITHUB_REPOSITORY", ""),
            token=os.environ.get("GH_TOKEN", ""),
        )
        selections = select_unique_evidence(client, options.candidate_sha)
        outputs: dict[str, int] = {}
        for selection in selections:
            prefix = selection.profile.replace("-", "_")
            outputs[f"{prefix}_run_id"] = selection.run_id
            outputs[f"{prefix}_artifact_id"] = selection.artifact_id
            print(
                f"selected {selection.profile} evidence: run {selection.run_id}, "
                f"artifact {selection.artifact_id}"
            )
        _append_github_output(options.github_output, outputs)
        return 0
    except EvidenceValidationError as exc:
        raise SystemExit(str(exc)) from None


if __name__ == "__main__":
    raise SystemExit(main())
