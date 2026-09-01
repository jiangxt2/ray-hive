# Integration test ledger

This ledger records the exact source, test, infrastructure, dependency, image, and workflow state
behind `ray-hive` integration evidence. A result remains reusable only while every behaviorally
relevant input is unchanged. Successful, failed, superseded, skipped, and not-run states are all
retained; a workflow name or green summary cannot replace the recorded coverage boundary.

## Current evidence

| Code state | Suite/profile | Environment and coverage | Result and boundary |
| --- | --- | --- | --- |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | GitHub `CI`, run [`32814409866`](https://github.com/jiangxt2/ray-hive/actions/runs/32814409866) | Python 3.12/3.13, Ray 2.55.1/2.56.1 unit and contract matrix; static, docs, generated Thrift, package and wheel import | `Success`; current short and package evidence. The historical run predates machine-readable release manifests and is not selectable by the new release evidence policy. |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | Hive 4.2.0 required IT, run [`32814409878`](https://github.com/jiangxt2/ray-hive/actions/runs/32814409878) | Python 3.12, resolved Ray 2.56.1, PyArrow 19.0.1, Thrift 0.22.0, pinned Apache Hive 4.2.0 image; six HS2 integration cases | `Success`; current required real-Hive evidence. The historical run predates machine-readable release manifests and is not selectable by the new release evidence policy. |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | Ray 2.56.1 multi-node cluster, run [`32814409984`](https://github.com/jiangxt2/ray-hive/actions/runs/32814409984) | One Ray head, two Ray workers, Hive 4.2.0, strict read setup | `Failure` before pytest: fixture setup found Thrift 0.16.0 in the cluster runtime while the project requires 0.22.0. This is a trusted blocker, not successful cluster evidence, and remains assigned to the current-Ray release-candidate work. |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | Scheduled slow/core, run [`33303512730`](https://github.com/jiangxt2/ray-hive/actions/runs/33303512730) | Reusable Hive/core workflow plus the maturity-claim guard | `Success`; proves only the scheduled Hive/core scope. It does not execute or replace Ray cluster evidence. |
| `100a66681dc1db3e912ef8541db5758ab87c22fd` | Ray 2.56.1 multi-node cluster, run [`32636612385`](https://github.com/jiangxt2/ray-hive/actions/runs/32636612385) | Earlier Ray 2.56.1 cluster state before the Thrift 0.22.0 runtime alignment | `Success`, but superseded by the later dependency/runtime state and unable to replace the failed `7d20af6` result. |

## Release evidence foundation

| Code state | Suite/profile | Run reason | Status |
| --- | --- | --- | --- |
| Base `7d20af6` plus the expanded `release-evidence-foundation` worktree diff | DCO, manifest, selector, measured runtime-facts and workflow-policy unit tests; full short unit/contract, static, docs and package checks | Add release evidence orchestration, non-hidden manifest paths and measured Hive/cluster runtime identity without changing production source, dependencies, Dockerfile, Compose or integration tests | `Short evidence complete`: 41 focused tests passed; final full unit/contract 225 passed in 3.95s with 91.29% coverage; Ruff checked 71 files; strict mypy checked 22 source files; lock, license, spec, codespell, MkDocs strict, workflow policy, four changed-workflow actionlint, cluster-script ShellCheck, build, Twine, isolated wheel dependency check/import, pre-commit and `git diff --check` passed. |
| Base `7d20af6` plus the `release-evidence-foundation` diff | Hive 4.2.0 IT | Production runtime, Hive protocol, dependencies, test cases and Compose are unchanged | `Not required`; the current successful `7d20af6` Hive result remains the runtime evidence, but it cannot serve as a selectable machine-readable release artifact. |
| Base `7d20af6` plus the expanded `release-evidence-foundation` diff | Ray cluster IT runtime-facts diagnostic | One Ray head, two workers and Hive 4.2.0; modified script captured runner Python 3.12.12, Ray 2.56.1, PyArrow 19.0.1 and Thrift 0.16.0 before fixture setup | `Expected compatibility failure`: fixture setup rejected Thrift 0.16.0 because the project requires 0.22.0; pytest did not start. Runtime facts `.artifacts/it/ray-cluster/runtime-facts.json`, Compose log `.artifacts/it/ray-cluster/compose.log`; exact project containers/network/volume and local runtime image were removed. The facts produced and validated a local non-release diagnostic manifest. A later setup-log change justified exactly one rerun recorded below; the dependency fix remains assigned to the current-Ray release-candidate PR. |
| Same expanded worktree after hidden diagnostic/upload review fix | Ray cluster fixture-log diagnostic | Preserve fixture setup stdout/stderr in `setup.log` and explicitly include the controlled hidden diagnostic directory in GitHub artifact upload | `Expected compatibility failure / evidence complete`: runtime facts again recorded Python 3.12.12, Ray 2.56.1, PyArrow 19.0.1, Thrift 0.16.0 and Hive 4.2.0; `.artifacts/it/ray-cluster-setup-log-review/setup.log` contains the complete `HiveCompatibilityError` traceback, pytest did not start, and exact containers/network/volume/runtime image were removed. No further cluster rerun is authorized or needed. |

The repository-wide `actionlint .github/workflows/*.yml` command also reports a pre-existing
ShellCheck SC2251 informational finding in unchanged `.github/workflows/slow-integration.yml`.
All four workflows changed by this worktree pass `actionlint`; the unchanged slow-workflow finding
is not hidden or attributed to this PR and requires a separately approved scope if it is repaired.

## Planned evidence

| Future state | Required evidence | Entry condition |
| --- | --- | --- |
| `external-alpha-contract` final state | One Hive 4.2.0 run covering query-only and side-effect rejection behavior | Public SQL contract implementation is stable and its long-test matrix is approved. |
| `current-ray-release-candidate` final master SHA | Successful CI, Hive 4.2.0 and Ray current-stable multi-node manifests for the exact same commit | Ray/Thrift/runtime changes are complete; this is the final repository candidate before tag creation. |
| `v1.1` release | Exact-SHA manifest selection and validation, one immutable package build, install smoke, TestPyPI/PyPI/GitHub Release and post-publish install verification | The final master runs are unique, successful and unexpired, and every external action has separate approval. |

## Evidence rules

- A release artifact name includes the full tested commit SHA and one required profile.
- A tag release consumes existing exact-SHA CI/Hive/cluster evidence and does not rerun those suites.
- Missing, duplicate, expired, cancelled, wrong-workflow, wrong-profile, wrong-commit or malformed
  evidence blocks release.
- A later documentation-only or ledger-only change does not create new runtime evidence and cannot
  move an existing tag or manifest to another commit.
- Long-test reruns require a relevant state change or an earlier failed, interrupted, environmental
  or untrusted result; all attempts remain in this ledger.
