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
        "cluster_runner": (ROOT / "scripts/run_ray_cluster_it.sh").read_text(encoding="utf-8"),
    }


def _failures(**changes: str) -> tuple[str, ...]:
    workflows = _workflows()
    workflows.update(changes)
    return workflow_policy_failures(**workflows)


def test_current_workflows_satisfy_release_evidence_policy() -> None:
    assert _failures() == ()


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


def test_policy_requires_matrix_manifest_and_project_versions_to_stay_aligned() -> None:
    workflows = _workflows()
    ci = workflows["ci"].replace("--ray-version 2.56.1", "--ray-version 2.57.0", 1)
    failures = _failures(ci=ci)
    assert any("CI evidence must record Ray 2.56.1" in failure for failure in failures)

    project = workflows["project"].replace("thrift==0.22.0", "thrift==0.23.0")
    failures = _failures(project=project)
    assert any("project dependency is missing" in failure for failure in failures)


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
