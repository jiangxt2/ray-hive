# Integration test ledger

This ledger records the exact source, test, infrastructure, dependency, image, and workflow state
behind `ray-hive` integration evidence. A result remains reusable only while every behaviorally
relevant input is unchanged. Successful, failed, superseded, skipped, and not-run states are all
retained; a workflow name or green summary cannot replace the recorded coverage boundary.

## Current evidence

| Code state | Suite/profile | Environment and coverage | Result and boundary |
| --- | --- | --- | --- |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | GitHub `CI`, run [`32814409866`](https://github.com/jiangxt2/ray-hive/actions/runs/32814409866) | Python 3.12/3.13, Ray 2.55.1/2.56.1 unit and contract matrix; static, docs, generated Thrift, package and wheel import | `Historical success`; the run predates machine-readable release manifests and is not selectable by the new release evidence policy. |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | Hive 4.2.0 required IT, run [`32814409878`](https://github.com/jiangxt2/ray-hive/actions/runs/32814409878) | Python 3.12, resolved Ray 2.56.1, PyArrow 19.0.1, Thrift 0.22.0, pinned Apache Hive 4.2.0 image; six HS2 integration cases | `Historical success`; the run predates machine-readable release manifests and is not selectable by the new release evidence policy. |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | Ray 2.56.1 multi-node cluster, run [`32814409984`](https://github.com/jiangxt2/ray-hive/actions/runs/32814409984) | One Ray head, two Ray workers, Hive 4.2.0, strict read setup | `Failure` before pytest: fixture setup found Thrift 0.16.0 in the cluster runtime while the project requires 0.22.0. This is a trusted blocker, not successful cluster evidence, and is assigned to `external-alpha-contract`. |
| `7d20af656a1297975e1aa9d01ed5b28f05558139` | Scheduled slow/core, run [`33303512730`](https://github.com/jiangxt2/ray-hive/actions/runs/33303512730) | Reusable Hive/core workflow plus the maturity-claim guard | `Success`; proves only the scheduled Hive/core scope. It does not execute or replace Ray cluster evidence. |
| `100a66681dc1db3e912ef8541db5758ab87c22fd` | Ray 2.56.1 multi-node cluster, run [`32636612385`](https://github.com/jiangxt2/ray-hive/actions/runs/32636612385) | Earlier Ray 2.56.1 cluster state before the Thrift 0.22.0 runtime alignment | `Success`, but superseded by the later dependency/runtime state and unable to replace the failed `7d20af6` result. |
| `0d9ea0014e245d9f85d35a54870d3b037220ebb9` | GitHub `CI`, run [`33467306596`](https://github.com/jiangxt2/ray-hive/actions/runs/33467306596), artifact `9785307242` | Python 3.12/3.13 and Ray 2.55.1/2.56.1 matrix plus static, DCO, codegen, package, and exact-SHA manifest policy | `Success`; first selectable CI evidence manifest. |
| `0d9ea0014e245d9f85d35a54870d3b037220ebb9` | Hive 4.2.0 required IT, run [`33467306542`](https://github.com/jiangxt2/ray-hive/actions/runs/33467306542), artifact `9785320630` | Measured Python, Ray, PyArrow, Thrift 0.22.0, Hive 4.2.0, and existing six-case HS2 suite | `Success`; first selectable Hive evidence manifest. It predates the query-only contract in the current worktree and cannot replace its final Hive run. |
| `0d9ea0014e245d9f85d35a54870d3b037220ebb9` | Ray 2.56.1 multi-node cluster, run [`33467306566`](https://github.com/jiangxt2/ray-hive/actions/runs/33467306566), diagnostic artifact `9785342556` | Measured Python 3.12, Ray 2.56.1, PyArrow 19.0.1, Thrift 0.16.0, Hive 4.2.0, setup and Compose logs | `Expected failure` before pytest; the artifact proves the Dockerfile mismatch and preserves the complete fixture traceback. No success manifest exists for this state. |

## Release evidence foundation

| Code state | Suite/profile | Run reason | Status |
| --- | --- | --- | --- |
| Base `7d20af6` plus the expanded `release-evidence-foundation` worktree diff | DCO, manifest, selector, measured runtime-facts and workflow-policy unit tests; full short unit/contract, static, docs and package checks | Add release evidence orchestration, non-hidden manifest paths and measured Hive/cluster runtime identity without changing production source, dependencies, Dockerfile, Compose or integration tests | `Short evidence complete`: 41 focused tests passed; final full unit/contract 225 passed in 3.95s with 91.29% coverage; Ruff checked 71 files; strict mypy checked 22 source files; lock, license, spec, codespell, MkDocs strict, workflow policy, four changed-workflow actionlint, cluster-script ShellCheck, build, Twine, isolated wheel dependency check/import, pre-commit and `git diff --check` passed. |
| Base `7d20af6` plus the `release-evidence-foundation` diff | Hive 4.2.0 IT | Production runtime, Hive protocol, dependencies, test cases and Compose are unchanged | `Not required`; the current successful `7d20af6` Hive result remains the runtime evidence, but it cannot serve as a selectable machine-readable release artifact. |
| Base `7d20af6` plus the expanded `release-evidence-foundation` diff | Ray cluster IT runtime-facts diagnostic | One Ray head, two workers and Hive 4.2.0; modified script captured runner Python 3.12.12, Ray 2.56.1, PyArrow 19.0.1 and Thrift 0.16.0 before fixture setup | `Expected compatibility failure`: fixture setup rejected Thrift 0.16.0 because the project requires 0.22.0; pytest did not start. Runtime facts `.artifacts/it/ray-cluster/runtime-facts.json`, Compose log `.artifacts/it/ray-cluster/compose.log`; exact project containers/network/volume and local runtime image were removed. The facts produced and validated a local non-release diagnostic manifest. A later setup-log change justified exactly one rerun recorded below; the dependency fix remains assigned to `external-alpha-contract`. |
| Same expanded worktree after hidden diagnostic/upload review fix | Ray cluster fixture-log diagnostic | Preserve fixture setup stdout/stderr in `setup.log` and explicitly include the controlled hidden diagnostic directory in GitHub artifact upload | `Expected compatibility failure / evidence complete`: runtime facts again recorded Python 3.12.12, Ray 2.56.1, PyArrow 19.0.1, Thrift 0.16.0 and Hive 4.2.0; `.artifacts/it/ray-cluster-setup-log-review/setup.log` contains the complete `HiveCompatibilityError` traceback, pytest did not start, and exact containers/network/volume/runtime image were removed. No further cluster rerun is authorized or needed. |

The repository-wide `actionlint .github/workflows/*.yml` command also reports a pre-existing
ShellCheck SC2251 informational finding in unchanged `.github/workflows/slow-integration.yml`.
All four workflows changed by the `release-evidence-foundation` worktree passed `actionlint`; the unchanged slow-workflow finding
is not hidden or attributed to this PR and requires a separately approved scope if it is repaired.

## External Alpha contract execution

| Code state | Suite/profile | Coverage and run reason | Status |
| --- | --- | --- | --- |
| Base `0d9ea00` plus the initial `external-alpha-contract` worktree diff before follow-up review | Full short unit/contract, static, docs, workflow policy, package, and isolated wheel checks | Thrift identity policy, conservative query/predicate lexer, variable-substitution rejection, immutable built-in predicates, pre-I/O failure, DeveloperAPI facade boundary, and support documentation | `Passed / superseded for final short state`: 270 unit/contract tests in 3.76s with 91.53% coverage; all recorded short gates passed. |
| Same pre-review implementation state | Hive 4.2.0 required IT | Existing reads plus rendered C-style literal escaping, raw query-only SELECT, rejected DDL/unsafe predicate, and sentinel row-count evidence | `Passed but superseded for final SQL-validation state`: 7 selected cases passed in 12.83s; the later exact-`Column` guard changes production predicate validation and requires one approved final Hive revalidation. Logs and exact cleanup evidence remain retained. |
| Same pre-review implementation state | Ray 2.56.1 multi-node cluster | Build-time Python/Ray/PyArrow/Thrift identity assertion, measured Thrift 0.22.0 runtime facts, fixture setup, strict-read pytest, diagnostic logs, exact cleanup, and success-manifest eligibility | `Passed / reusable for the cluster boundary`: the image asserted and the runner measured Python 3.12.12, Ray 2.56.1, PyArrow 19.0.1, Thrift 0.22.0, and Hive 4.2.0; 1 multi-node strict-read case passed in 2.55s. The later `Column` guard and API documentation do not affect the no-filter cluster case, Ray task behavior, Docker runtime, or topology, so no cluster rerun is required. `.artifacts/it/ray-cluster/` retains the logs and cleanup proof. |
| Base `0d9ea00` plus the current cumulative worktree diff after follow-up review | Full short unit/contract, static, docs, workflow policy, package, and isolated wheel checks | Exact built-in `Column` leaves for filter/split paths and an explicit stable `ray_hive.sql` builder contract, in addition to the prior scope | `Passed`: 273 unit/contract tests in 3.60s with 91.64% coverage; Ruff, strict mypy, lock, license, spec, codespell, MkDocs strict, workflow policy, actionlint, ShellCheck, pre-commit, build, Twine, isolated wheel import, and dependency check passed. |
| Same current cumulative worktree diff | Hive 4.2.0 required IT | Final built-in predicate acceptance plus the existing query-only and C-style literal cases | `Passed / final local Hive evidence`: 7 selected cases passed in 7.18s; the cluster-marked case was deselected by the suite boundary, not skipped. `tests/integration/logs/` preserves Hive, image, dangling-image, and Docker disk-state logs; the exact container, network, and volume were removed. |

## Planned evidence

| Future state | Required evidence | Entry condition |
| --- | --- | --- |
| Pushed `external-alpha-contract` exact commit | Successful CI, Hive 4.2.0, and Ray 2.56.1 cluster manifests for the pushed commit | Current short evidence and final Hive revalidation are complete; commit and push require separate approval. |
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
