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

from pathlib import Path

import pytest

from scripts.check_release_workflows import workflow_policy_failures

ROOT = Path(__file__).parents[2]


def _workflows() -> dict[str, str]:
    return {
        "ci": (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        "hive": (ROOT / ".github/workflows/hive-integration.yml").read_text(encoding="utf-8"),
        "cluster": (ROOT / ".github/workflows/ray-cluster-integration.yml").read_text(
            encoding="utf-8"
        ),
        "release": (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"),
        "precommit": (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"),
        "project": (ROOT / "pyproject.toml").read_text(encoding="utf-8"),
        "compat": (ROOT / "src/ray_hive/_compat.py").read_text(encoding="utf-8"),
        "ray_dockerfile": (ROOT / "docker/ray/Dockerfile").read_text(encoding="utf-8"),
        "cluster_runner": (ROOT / "scripts/run_ray_cluster_it.sh").read_text(encoding="utf-8"),
        "readme": (ROOT / "README.md").read_text(encoding="utf-8"),
        "support": (ROOT / "SUPPORT.md").read_text(encoding="utf-8"),
        "compatibility": (ROOT / "docs/compatibility.md").read_text(encoding="utf-8"),
        "docs_index": (ROOT / "docs/index.md").read_text(encoding="utf-8"),
        "changelog": (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
        "release_notes": (ROOT / "release-notes/v1.2.md").read_text(encoding="utf-8"),
    }


def _failures(**changes: str) -> tuple[str, ...]:
    workflows = _workflows()
    workflows.update(changes)
    return workflow_policy_failures(**workflows)


def test_current_workflows_satisfy_release_evidence_policy() -> None:
    assert _failures() == ()


@pytest.mark.parametrize("version", ["1.2", "1.2.dev0", "1.3.dev0"])
def test_policy_accepts_formal_candidate_and_development_metadata(version: str) -> None:
    project = _workflows()["project"].replace('version = "1.2"', f'version = "{version}"', 1)

    assert _failures(project=project) == ()


def test_policy_rejects_formal_version_that_differs_from_release_tag() -> None:
    project = _workflows()["project"].replace('version = "1.2"', 'version = "1.1"', 1)

    failures = _failures(project=project)

    assert any("formal project version must match" in failure for failure in failures)


def test_policy_rejects_static_candidate_version_output() -> None:
    release = _workflows()["release"].replace(
        "version: ${{ steps.validate.outputs.version }}",
        'version: "1.2"',
        1,
    )

    failures = _failures(release=release)

    assert any("validated project version" in failure for failure in failures)


def test_policy_rejects_hardcoded_testpypi_smoke_version() -> None:
    release = _workflows()["release"].replace(
        "VERSION: ${{ needs.candidate.outputs.version }}",
        'VERSION: "1.2"',
        1,
    )

    failures = _failures(release=release)

    assert any("testpypi-smoke must use" in failure for failure in failures)


@pytest.mark.parametrize(
    ("field", "old", "new", "message"),
    [
        ("ci", "scripts/check_dco.py", "scripts/missing.py", "shared DCO"),
        ("ci", "@de0fac2e4500dabe0009e67214ff5f5447ce83dd", "@v6", "not commit-pinned"),
        ("ci", "scripts/check_release_workflows.py", "scripts/missing.py", "policy checker"),
        ("hive", "--profile hive-420", "--profile ci", "hive-420 evidence"),
        ("cluster", "--profile ray-cluster", "--profile ci", "ray-cluster evidence"),
        ("release", "actions: read", "actions: none", "actions: read"),
        (
            "release",
            "steps.select.outputs.ci_artifact_id",
            "steps.select.outputs.missing_artifact_id",
            "ci artifact ID",
        ),
        (
            "release",
            "--expected-profile ray-cluster",
            "--expected-profile ci",
            "ray-cluster manifest",
        ),
        (
            "precommit",
            "check_release_workflows.py",
            "missing_workflow_checker.py",
            "pre-commit",
        ),
    ],
)
def test_policy_detects_missing_release_invariants(
    field: str,
    old: str,
    new: str,
    message: str,
) -> None:
    workflows = _workflows()
    assert old in workflows[field]
    failures = _failures(**{field: workflows[field].replace(old, new, 1)})
    assert any(message in failure for failure in failures)


def test_policy_rejects_release_reruns_and_top_level_path_filters() -> None:
    workflows = _workflows()
    release = workflows["release"].replace(
        "jobs:\n",
        "jobs:\n  rerun:\n    uses: ./.github/workflows/ci.yml\n",
        1,
    )
    failures = _failures(release=release)
    assert any("must not rerun ci.yml" in failure for failure in failures)

    ci = workflows["ci"].replace(
        "  pull_request:\n", "  pull_request:\n    paths:\n      - src/**\n"
    )
    failures = _failures(ci=ci)
    assert any("top-level paths filters" in failure for failure in failures)

    ci = workflows["ci"].replace(
        "on:\n",
        "on: {pull_request: {paths: [src/**]}}\n",
        1,
    )
    failures = _failures(ci=ci)
    assert any("top-level paths filters" in failure for failure in failures)


def test_policy_requires_evidence_dependency_for_every_formal_job() -> None:
    workflows = _workflows()
    release = workflows["release"].replace(
        "needs: [candidate, evidence, build, publish]",
        "needs: [candidate, build, publish]",
    )
    failures = _failures(release=release)
    assert any("github-release must depend on release evidence" in failure for failure in failures)


def test_policy_parses_ci_evidence_dependencies_instead_of_matching_body_text() -> None:
    workflows = _workflows()
    ci = workflows["ci"].replace(
        "needs: [dco, static, unit-contract, thrift-codegen, package]",
        "needs: [dco, static, unit-contract, thrift-codegen]",
    )
    assert "--suite dco-static-unit-contract-codegen-package" in ci
    failures = _failures(ci=ci)
    assert any("CI evidence must depend on package" in failure for failure in failures)


def test_policy_requires_every_python_ray_matrix_pair_exactly_once() -> None:
    workflows = _workflows()
    target = '          - python-version: "3.13"\n            ray-version: "2.59.0"\n'
    duplicate = '          - python-version: "3.13"\n            ray-version: "2.57.0"\n'
    assert target in workflows["ci"]

    missing = workflows["ci"].replace(target, "", 1)
    failures = _failures(ci=missing)
    assert any("each expected Python/Ray pair exactly once" in failure for failure in failures)

    replaced_by_duplicate = workflows["ci"].replace(target, duplicate, 1)
    failures = _failures(ci=replaced_by_duplicate)
    assert any("each expected Python/Ray pair exactly once" in failure for failure in failures)

    extra_duplicate = workflows["ci"].replace(target, target + target, 1)
    failures = _failures(ci=extra_duplicate)
    assert any("each expected Python/Ray pair exactly once" in failure for failure in failures)


def test_policy_requires_matrix_manifest_and_project_versions_to_stay_aligned() -> None:
    workflows = _workflows()
    ci = workflows["ci"].replace("--ray-version 2.56.1", "--ray-version 2.57.0", 1)
    failures = _failures(ci=ci)
    assert any("CI evidence must record Ray 2.56.1" in failure for failure in failures)

    project = workflows["project"].replace("thrift==0.22.0", "thrift==0.23.0")
    failures = _failures(project=project)
    assert any("project dependency is missing" in failure for failure in failures)

    project = workflows["project"].replace("ray[data]>=2.55.1,<2.60", "ray[data]>=2.55.1,<2.61")
    failures = _failures(project=project)
    assert any("project dependency is missing" in failure for failure in failures)

    compat = workflows["compat"].replace(
        "_MAX_RAY_VERSION = (2, 60, 0)", "_MAX_RAY_VERSION = (2, 61, 0)"
    )
    failures = _failures(compat=compat)
    assert any("compatibility constants" in failure for failure in failures)

    readme = workflows["readme"].replace(">=2.55.1,<2.60", ">=2.55.1,<2.61")
    failures = _failures(readme=readme)
    assert any("README must declare" in failure for failure in failures)

    support = workflows["support"].replace(
        "A final patch release within an admitted\nminor",
        "An unspecified patch policy",
    )
    failures = _failures(support=support)
    assert any("distinguish the Ray window" in failure for failure in failures)

    compatibility = workflows["compatibility"].replace(
        "The\nreal-infrastructure baseline is Ray 2.59.0",
        "The infrastructure baseline is unspecified",
    )
    failures = _failures(compatibility=compatibility)
    assert any("real-infrastructure Ray baseline" in failure for failure in failures)

    changelog = workflows["changelog"].replace(
        "[ray-hive 1.2 release notes](release-notes/v1.2.md)",
        "missing release notes",
    )
    failures = _failures(changelog=changelog)
    assert any("CHANGELOG must link" in failure for failure in failures)

    release_notes = workflows["release_notes"].replace("# ray-hive 1.2", "# wrong version")
    failures = _failures(release_notes=release_notes)
    assert any("release notes are missing" in failure for failure in failures)

    ray_dockerfile = workflows["ray_dockerfile"].replace(
        '"thrift==0.22.0"',
        '"thrift==0.16.0"',
    )
    failures = _failures(ray_dockerfile=ray_dockerfile)
    assert any("Dockerfile Thrift pin" in failure for failure in failures)

    ray_dockerfile = workflows["ray_dockerfile"].replace(
        "'thrift': version('thrift')",
        "'thrift': 'unmeasured'",
    )
    failures = _failures(ray_dockerfile=ray_dockerfile)
    assert any("must assert the installed Thrift version" in failure for failure in failures)

    cluster_runner = workflows["cluster_runner"].replace(
        "sha256:0ffca209fb4c984d648db550b8dbe7879bdba3bbb5d96ee5894843cf6ac12eb5",
        "sha256:" + "0" * 64,
    )
    failures = _failures(cluster_runner=cluster_runner)
    assert any("pinned current Ray image" in failure for failure in failures)

    release = workflows["release"].replace('default: "v1.2"', 'default: "v1.0"')
    failures = _failures(release=release)
    assert any("defaults must identify a formal release example" in failure for failure in failures)


def test_policy_rejects_hidden_artifacts_and_declared_real_runtime() -> None:
    workflows = _workflows()
    hive = workflows["hive"].replace("artifacts/evidence/", ".artifacts/evidence/", 1)
    failures = _failures(hive=hive)
    assert any("hidden artifact path" in failure for failure in failures)

    cluster = workflows["cluster"].replace(
        "--identity-source measured-process",
        "--identity-source workflow-matrix",
    )
    failures = _failures(cluster=cluster)
    assert any("measured process identity" in failure for failure in failures)

    runner = workflows["cluster_runner"].replace("capture-runtime", "missing-capture", 1)
    failures = _failures(cluster_runner=runner)
    assert any("runner must capture" in failure for failure in failures)

    cluster = workflows["cluster"].replace("          include-hidden-files: true\n", "", 1)
    failures = _failures(cluster=cluster)
    assert any("diagnostic upload" in failure for failure in failures)

    runner = workflows["cluster_runner"].replace(
        '2>&1 | tee "${artifact_dir}/setup.log"',
        "",
        1,
    )
    failures = _failures(cluster_runner=runner)
    assert any("fixture setup must preserve" in failure for failure in failures)
