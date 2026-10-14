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

"""Enforce fail-closed CI and release evidence orchestration invariants."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_ACTION_PIN = re.compile(r"^[0-9a-f]{40}$")
_CANONICAL_PROJECT_VERSION = re.compile(
    r"(?m)^version = \"(?P<version>[1-9][0-9]*\.(?:0|[1-9][0-9]*)(?:\.dev0)?)\"$"
)
_RELEASE_TAG_VERSION = re.compile(
    r"(?m)^        default: \"v(?P<version>[1-9][0-9]*\.(?:0|[1-9][0-9]*))\"$"
)
_RAY_RANGE = ">=2.55.1,<2.60"
_PYTHON_VERSIONS = ("3.12", "3.13")
_RAY_VERSIONS = ("2.55.1", "2.56.1", "2.57.0", "2.58.0", "2.59.0")
_EXPECTED_UNIT_MATRIX = Counter(
    (python_version, ray_version)
    for python_version in _PYTHON_VERSIONS
    for ray_version in _RAY_VERSIONS
)
_CLUSTER_RAY_VERSION = "2.59.0"
_CLUSTER_RAY_DIGEST = "sha256:0ffca209fb4c984d648db550b8dbe7879bdba3bbb5d96ee5894843cf6ac12eb5"


def _job(workflow: str, job_id: str) -> str:
    jobs_start = workflow.find("\njobs:\n")
    if jobs_start < 0:
        return ""
    jobs = workflow[jobs_start:]
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        jobs,
    )
    return "" if match is None else match.group(1)


def _unpinned_actions(workflow: str) -> tuple[str, ...]:
    unpinned = []
    for line in workflow.splitlines():
        stripped = line.strip()
        if not stripped.startswith("uses: "):
            continue
        value = stripped.removeprefix("uses: ").split()[0]
        if value.startswith("./"):
            continue
        _, separator, revision = value.rpartition("@")
        if not separator or _ACTION_PIN.fullmatch(revision) is None:
            unpinned.append(value)
    return tuple(unpinned)


def _job_needs(job: str) -> frozenset[str]:
    match = re.search(r"(?m)^    needs: \[([^]]*)\]\s*$", job)
    if match is None:
        return frozenset()
    return frozenset(item.strip() for item in match.group(1).split(",") if item.strip())


def _top_level_block(workflow: str, key: str) -> str:
    lines = workflow.splitlines()
    for index, line in enumerate(lines):
        if line == f"{key}:" or line.startswith(f"{key}: "):
            block = [line]
            for candidate in lines[index + 1 :]:
                if candidate and not candidate[0].isspace() and not candidate.startswith("#"):
                    break
                block.append(candidate)
            return "\n".join(block)
    return ""


def _exact_dependency(source: str, package: str) -> str | None:
    match = re.search(rf'"{re.escape(package)}==([^"\s]+)"', source)
    return None if match is None else match.group(1)


def _matrix_include_pairs(job: str) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    lines = job.splitlines()
    for index, line in enumerate(lines):
        python_match = re.fullmatch(r'\s*-\s+python-version:\s*"([^"]+)"\s*', line)
        if python_match is None or index + 1 >= len(lines):
            continue
        ray_match = re.fullmatch(r'\s+ray-version:\s*"([^"]+)"\s*', lines[index + 1])
        if ray_match is not None:
            pairs.append((python_match.group(1), ray_match.group(1)))
    return tuple(pairs)


def _normalized_text(source: str) -> str:
    return " ".join(source.split())


def _project_version(project: str) -> str | None:
    """Return a canonical formal or development project version."""
    match = _CANONICAL_PROJECT_VERSION.search(project)
    return None if match is None else match.group("version")


def _formal_release_version(release: str) -> str | None:
    """Read the current release identity from the workflow recovery tag."""
    match = _RELEASE_TAG_VERSION.search(release)
    return None if match is None else match.group("version")


def workflow_policy_failures(
    *,
    ci: str,
    hive: str,
    cluster: str,
    release: str,
    precommit: str,
    project: str,
    compat: str,
    ray_dockerfile: str,
    cluster_runner: str,
    readme: str,
    support: str,
    compatibility: str,
    docs_index: str,
    changelog: str,
    release_notes: str,
) -> tuple[str, ...]:
    """Return all CI/release evidence policy violations."""

    failures: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    workflows = {"CI": ci, "Hive": hive, "Ray cluster": cluster, "Release": release}
    for name, workflow in workflows.items():
        failures.extend(
            f"{name} workflow action is not commit-pinned: {action}"
            for action in _unpinned_actions(workflow)
        )
        triggers = _top_level_block(workflow, "on")
        require(
            re.search(r"(?<![A-Za-z0-9_-])paths(?:-ignore)?\s*:", triggers) is None,
            f"{name} workflow must not use top-level paths filters",
        )

    dco = _job(ci, "dco")
    static = _job(ci, "static")
    ci_evidence = _job(ci, "evidence")
    require("workflow_call:" in ci, "CI must retain workflow_call")
    require("fetch-depth: 0" in dco, "CI DCO checkout must fetch full history")
    require("scripts/check_dco.py" in dco, "CI must use the shared DCO checker")
    require("git show -s --format=%B" not in dco, "CI must not duplicate inline DCO parsing")
    require(
        "scripts/check_release_workflows.py" in static,
        "CI static job must run the workflow policy checker",
    )
    require(bool(ci_evidence), "CI is missing the evidence aggregator job")
    ci_needs = _job_needs(ci_evidence)
    for dependency in ("dco", "static", "unit-contract", "thrift-codegen", "package"):
        require(dependency in ci_needs, f"CI evidence must depend on {dependency}")
    require("--profile ci" in ci_evidence, "CI evidence must write the ci profile")
    require(
        "--identity-source workflow-matrix" in ci_evidence,
        "CI evidence must identify its source as the successful workflow matrix",
    )
    require(
        ".artifacts/evidence/" not in ci_evidence,
        "CI evidence must not use a hidden artifact path",
    )
    require(
        "--output artifacts/evidence/ci.json" in ci_evidence
        and "path: artifacts/evidence/ci.json" in ci_evidence,
        "CI evidence must use the non-hidden evidence directory",
    )
    ci_artifact = "ray-hive-evidence-ci-${{ inputs.candidate_sha || github.sha }}"
    require(
        ci_artifact in ci_evidence,
        "CI evidence artifact must bind to candidate SHA",
    )
    unit = _job(ci, "unit-contract")
    require(
        Counter(_matrix_include_pairs(unit)) == _EXPECTED_UNIT_MATRIX,
        "CI unit matrix must contain each expected Python/Ray pair exactly once",
    )
    for python_version in _PYTHON_VERSIONS:
        require(
            f"--python-version {python_version}" in ci_evidence,
            f"CI evidence must record Python {python_version}",
        )
    for ray_version in _RAY_VERSIONS:
        require(
            f"--ray-version {ray_version}" in ci_evidence,
            f"CI evidence must record Ray {ray_version}",
        )
    project_version = _project_version(project)
    release_version = _formal_release_version(release)
    require(
        project_version is not None,
        "project version must use canonical major.minor or major.minor.dev0",
    )
    require(
        release_version is not None,
        "release workflow must default to a canonical existing release tag",
    )
    if (
        project_version is not None
        and release_version is not None
        and not project_version.endswith(".dev0")
    ):
        require(
            project_version == release_version,
            "formal project version must match the release workflow tag",
        )
    for requirement in (
        'requires-python = ">=3.12,<3.14"',
        '"pyarrow==19.0.1"',
        f'"ray[data]{_RAY_RANGE}"',
        '"thrift==0.22.0"',
    ):
        require(requirement in project, f"project dependency is missing {requirement}")
    require(
        f'SUPPORTED_RAY_RANGE = "{_RAY_RANGE}"' in compat
        and "_MAX_RAY_VERSION = (2, 60, 0)" in compat,
        "runtime compatibility constants must match the project Ray range",
    )
    for name, document in {
        "README": readme,
        "SUPPORT": support,
        "compatibility docs": compatibility,
        "docs index": docs_index,
    }.items():
        require(_RAY_RANGE in document, f"{name} must declare the supported Ray range")
    normalized_support = _normalized_text(support)
    normalized_compatibility = _normalized_text(compatibility)
    require(
        "Exact CI baselines are" in normalized_support
        and "A final patch release within an admitted minor" in normalized_support,
        "SUPPORT must distinguish the Ray window from exact evidence baselines",
    )
    require(
        "The real-infrastructure baseline is Ray 2.59.0" in normalized_compatibility,
        "compatibility docs must identify the real-infrastructure Ray baseline",
    )
    if release_version is not None:
        require(
            f"[ray-hive {release_version} release notes](release-notes/v{release_version}.md)"
            in changelog,
            f"CHANGELOG must link to the {release_version} release notes",
        )
        require(
            f"# ray-hive {release_version}" in release_notes,
            f"{release_version} release notes are missing their identity",
        )
    project_thrift = _exact_dependency(project, "thrift")
    docker_thrift = _exact_dependency(ray_dockerfile, "thrift")
    require(
        project_thrift is not None and docker_thrift == project_thrift,
        "Ray cluster Dockerfile Thrift pin must match the project dependency",
    )
    if project_thrift is not None:
        require(
            f"'thrift': '{project_thrift}'" in ray_dockerfile
            and "'thrift': version('thrift')" in ray_dockerfile,
            "Ray cluster image build must assert the installed Thrift version",
        )
        require(
            f"--thrift-version {project_thrift}" in ci_evidence,
            "CI evidence Thrift identity must match the project dependency",
        )
    require(
        f"ray-hive-ray-base:{_CLUSTER_RAY_VERSION}" in ray_dockerfile
        and f"'ray': '{_CLUSTER_RAY_VERSION}'" in ray_dockerfile,
        "Ray cluster image build must assert the current Ray version",
    )
    require(
        f'ray_base_image="ray-hive-ray-base:{_CLUSTER_RAY_VERSION}"' in cluster_runner
        and _CLUSTER_RAY_DIGEST in cluster_runner,
        "Ray cluster runner must use the pinned current Ray image",
    )
    for manifest_argument in ("--pyarrow-version 19.0.1", "--thrift-version 0.22.0"):
        require(
            manifest_argument in ci_evidence,
            f"CI evidence is missing {manifest_argument}",
        )

    hive_job = _job(hive, "hive-420")
    require("workflow_call:" in hive, "Hive workflow must retain workflow_call")
    require("--profile hive-420" in hive_job, "Hive workflow must write hive-420 evidence")
    require("capture-runtime" in hive_job, "Hive workflow must capture runtime facts")
    require(
        "--identity-source measured-process" in hive_job,
        "Hive evidence must use measured process identity",
    )
    require(
        "--runtime-file artifacts/evidence/hive-runtime.json" in hive_job,
        "Hive evidence must consume measured runtime facts",
    )
    require(
        "--ray-version" not in hive_job and "--thrift-version" not in hive_job,
        "Hive evidence must not hard-code client runtime versions",
    )
    require(
        ".artifacts/evidence/" not in hive_job,
        "Hive evidence must not use a hidden artifact path",
    )
    require(
        "--output artifacts/evidence/hive-420.json" in hive_job
        and "path: artifacts/evidence/hive-420.json" in hive_job,
        "Hive evidence must use the non-hidden evidence directory",
    )
    require(
        "ray-hive-evidence-hive-420-${{ inputs.candidate_sha || github.sha }}" in hive_job,
        "Hive evidence artifact must bind to candidate SHA",
    )
    require("if: always()" in hive_job, "Hive workflow must preserve logs on failure")

    cluster_job = _job(cluster, "ray-2590-cluster")
    require("workflow_call:" in cluster, "Ray cluster workflow must retain workflow_call")
    require(
        "--profile ray-cluster" in cluster_job,
        "Ray cluster workflow must write ray-cluster evidence",
    )
    require(
        "--identity-source measured-process" in cluster_job,
        "Ray cluster evidence must use measured process identity",
    )
    require(
        "--runtime-file .artifacts/it/ray-cluster/runtime-facts.json" in cluster_job,
        "Ray cluster evidence must consume runner runtime facts",
    )
    require(
        "--ray-version" not in cluster_job and "--thrift-version" not in cluster_job,
        "Ray cluster evidence must not hard-code client runtime versions",
    )
    require(
        ".artifacts/evidence/" not in cluster_job,
        "Ray cluster evidence must not use a hidden artifact path",
    )
    require(
        "--output artifacts/evidence/ray-cluster.json" in cluster_job
        and "path: artifacts/evidence/ray-cluster.json" in cluster_job,
        "Ray cluster evidence must use the non-hidden evidence directory",
    )
    require(
        "release_evidence.py capture-runtime" in cluster_runner
        and 'runtime_facts_file="${artifact_dir}/runtime-facts.json"' in cluster_runner,
        "Ray cluster runner must capture measured runtime facts",
    )
    require(
        "path: .artifacts/it/ray-cluster/" in cluster_job
        and "include-hidden-files: true" in cluster_job,
        "Ray cluster diagnostic upload must explicitly include its hidden directory",
    )
    require(
        'tee "${artifact_dir}/setup.log"' in cluster_runner,
        "Ray cluster fixture setup must preserve stdout and stderr",
    )
    require(
        "ray-hive-evidence-ray-cluster-${{ inputs.candidate_sha || github.sha }}" in cluster_job,
        "Ray cluster evidence artifact must bind to candidate SHA",
    )
    require(
        f"Ray {_CLUSTER_RAY_VERSION} multi-node cluster" in cluster_job
        and _CLUSTER_RAY_DIGEST in cluster_job
        and "ray-2590-cluster-${{ github.run_id }}-${{ github.run_attempt }}" in cluster_job,
        "Ray cluster workflow must use the current Ray identity",
    )
    require("if: always()" in cluster_job, "Ray cluster workflow must preserve logs on failure")

    require('tags: ["v*"]' in release, "Release must trigger on version tags")
    require("workflow_dispatch:" in release, "Release must support candidate dry runs")
    require(
        release_version is not None
        and f"for example {release_version}" in release
        and f"for example v{release_version}" in release
        and f'default: "v{release_version}"' in release,
        "Release workflow defaults must identify a formal release example",
    )
    candidate = _job(release, "candidate")
    evidence = _job(release, "evidence")
    build = _job(release, "build")
    require(
        "version: ${{ steps.validate.outputs.version }}" in candidate
        and "scripts/validate_release_candidate.py" in candidate
        and '--expected-version "${EXPECTED_VERSION}"' in candidate,
        "candidate job must expose the validated project version",
    )
    version_consumers = {
        "build": '--version "${{ needs.candidate.outputs.version }}"',
        "testpypi-publish": '--version "${{ needs.candidate.outputs.version }}"',
        "testpypi-smoke": "VERSION: ${{ needs.candidate.outputs.version }}",
        "github-release": '--notes-file "release-notes/v${{ needs.candidate.outputs.version }}.md"',
    }
    for job_id, version_reference in version_consumers.items():
        require(
            version_reference in _job(release, job_id),
            f"{job_id} must use the validated candidate version",
        )
    require("fetch-depth: 0" in candidate, "Release candidate must fetch full history")
    require("scripts/check_dco.py" in candidate, "Release candidate must use shared DCO checker")
    for workflow in ("ci.yml", "hive-integration.yml", "ray-cluster-integration.yml"):
        require(
            f"uses: ./.github/workflows/{workflow}" not in release,
            f"Release must not rerun {workflow}",
        )
    require(bool(evidence), "Release is missing the exact-SHA evidence job")
    require("actions: read" in evidence, "Release evidence job needs actions: read")
    require(
        "release_evidence.py select" in evidence,
        "Release evidence job must select exact-SHA artifacts",
    )
    for profile in ("ci", "hive_420", "ray_cluster"):
        require(
            f"steps.select.outputs.{profile}_artifact_id" in evidence,
            f"Release must download the selected {profile} artifact ID",
        )
        require(
            f"steps.select.outputs.{profile}_run_id" in evidence,
            f"Release must bind the selected {profile} run ID",
        )
    for profile in ("ci", "hive-420", "ray-cluster"):
        require(
            f"--expected-profile {profile}" in evidence,
            f"Release must revalidate the {profile} manifest",
        )
    require(
        "needs: [candidate, evidence]" in build,
        "Release build must depend directly on candidate and evidence",
    )
    require(
        "needs.evidence.result == 'success'" in build,
        "Release build must require evidence success",
    )
    for job_id in (
        "install-smoke",
        "testpypi-publish",
        "testpypi-smoke",
        "publish",
        "github-release",
    ):
        job = _job(release, job_id)
        require(bool(job), f"Release is missing the {job_id} job")
        require(
            "evidence" in _job_needs(job),
            f"{job_id} must depend on release evidence",
        )
        require(
            "needs.evidence.result == 'success'" in job,
            f"{job_id} must require evidence success",
        )
    require(
        "environment: testpypi" in _job(release, "testpypi-publish"),
        "TestPyPI publish must use its protected environment",
    )
    require(
        "id-token: write" in _job(release, "testpypi-publish"),
        "TestPyPI publish must use OIDC",
    )
    require(
        "environment: pypi" in _job(release, "publish"),
        "PyPI publish must use its protected environment",
    )
    require("id-token: write" in _job(release, "publish"), "PyPI publish must use OIDC")
    require(
        "environment: github-release" in _job(release, "github-release"),
        "GitHub Release must use its protected environment",
    )
    require(
        "id-token: write" not in _job(release, "github-release"),
        "GitHub Release must not have OIDC write",
    )
    require(
        "check_release_workflows.py" in precommit,
        "pre-commit must run the workflow policy checker",
    )
    return tuple(failures)


def main() -> int:
    release_text = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    release_version = _formal_release_version(release_text)
    if release_version is None:
        print("release workflow must default to a canonical existing release tag")
        return 1
    project_text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    failures = workflow_policy_failures(
        ci=(ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        hive=(ROOT / ".github/workflows/hive-integration.yml").read_text(encoding="utf-8"),
        cluster=(ROOT / ".github/workflows/ray-cluster-integration.yml").read_text(
            encoding="utf-8"
        ),
        release=release_text,
        precommit=(ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"),
        project=project_text,
        compat=(ROOT / "src/ray_hive/_compat.py").read_text(encoding="utf-8"),
        ray_dockerfile=(ROOT / "docker/ray/Dockerfile").read_text(encoding="utf-8"),
        cluster_runner=(ROOT / "scripts/run_ray_cluster_it.sh").read_text(encoding="utf-8"),
        readme=(ROOT / "README.md").read_text(encoding="utf-8"),
        support=(ROOT / "SUPPORT.md").read_text(encoding="utf-8"),
        compatibility=(ROOT / "docs/compatibility.md").read_text(encoding="utf-8"),
        docs_index=(ROOT / "docs/index.md").read_text(encoding="utf-8"),
        changelog=(ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
        release_notes=(ROOT / f"release-notes/v{release_version}.md").read_text(encoding="utf-8"),
    )
    for failure in failures:
        print(failure)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
