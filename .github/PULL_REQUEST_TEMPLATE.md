## What changes

Describe the public behavior and the implementation boundaries changed by this pull request.

## Why

Explain the correctness, compatibility, security, or maintainability need.

## User-facing impact

Describe API, compatibility, performance, consistency, or migration impact. Write `None` only when
the change is genuinely internal.

## How tested

- [ ] Ruff format and lint
- [ ] mypy
- [ ] Unit and contract tests with coverage
- [ ] License, codespell, documentation, and conformance checks
- [ ] Real Hive 4.2.0 integration tests when required
- [ ] Ray cluster or slow infrastructure tests when required
- [ ] Package build, isolated install, and import smoke

List exact commands and infrastructure versions below.

## Checklist

- [ ] I used only public Ray APIs outside `ray_hive._compat`.
- [ ] I added no storage, filesystem, HMS, or table-location fallback.
- [ ] I documented any consistency or capability change without broadening unverified claims.
- [ ] Every commit has a valid `Signed-off-by` trailer.
