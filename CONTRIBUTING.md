# Contributing to ray-hive

Open an issue before a large public API, protocol, security, or architecture change. Source,
comments, documentation, commit messages, and public project communication are written in English.

## Local checks

Use the locked Python 3.12 or 3.13 environment:

```bash
uv sync --extra dev --extra telemetry
pre-commit install --hook-type pre-commit
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/python scripts/check_license_headers.py
.venv/bin/python scripts/check_spec.py
.venv/bin/mkdocs build --strict
.venv/bin/codespell
.venv/bin/python -m pytest tests/unit tests/contract --cov=ray_hive
.venv/bin/python -m build
.venv/bin/twine check dist/*
```

SQL, schema, decoder, transport, authentication, session/operation lifecycle, cancellation, or
credential changes must also pass the real Hive integration suite:

```bash
./scripts/run_hive_it.sh
```

Ray task configuration, retries, cancellation, object loss, worker failure, or distributed
execution changes must pass the Ray cluster suite. Do not skip or mock infrastructure gates to
produce a green result. Preserve service and network-denial logs on failure.

Docker commands must target only the `ray-hive-it` Compose project. Never run prune or broad
container, network, or volume cleanup commands.

## Compatibility and generated code

Only public Ray namespaces may be imported. All Ray DeveloperAPI adaptation belongs in
`src/ray_hive/_compat.py`. Unsupported signatures and capabilities fail closed.

Do not edit generated TCLIService files. Update the vendored IDL provenance and reproduce them with:

```bash
.venv/bin/python scripts/generate_thrift_stubs.py --build-image --check
```

Every independently verifiable behavior needs a unit or contract test. Protocol and database
behavior also needs the applicable real infrastructure test.

## Developer Certificate of Origin

Every commit must contain this trailer:

```text
Signed-off-by: jiangxt2 <jiangxt2@vip.qq.com>
```

Use `git commit -s` when the configured Git identity is the contributor who certifies the change.
