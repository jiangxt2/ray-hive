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

"""Validate release identity and reject likely project-owned credential literals."""

from __future__ import annotations

import argparse
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LITERAL = re.compile(
    r"(?i)\b(?:password|access[_-]?key|secret[_-]?key|token)\s*=\s*['\"][^'$<{][^'\"]{7,}['\"]"
)
SCANNED_SUFFIXES = {".py", ".md", ".toml", ".yml", ".yaml"}


def formal_version(value: object) -> str:
    """Require the canonical two-component identity of a formal release."""
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]*\.(?:0|[1-9][0-9]*)", value) is None:
        raise SystemExit("Formal version must use canonical major.minor components")
    return value


def _package_version() -> str:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    return formal_version(project["version"])


def _scan_credentials() -> None:
    findings = []
    roots = (ROOT / "src", ROOT / "docs", ROOT / "scripts")
    for base in roots:
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
                continue
            if "generated" in path.parts:
                continue
            for line_number, line in enumerate(path.read_text().splitlines(), start=1):
                if LITERAL.search(line):
                    findings.append(f"{path.relative_to(ROOT)}:{line_number}")
    if findings:
        raise SystemExit("Possible credential literals: " + ", ".join(findings))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    expected = f"v{_package_version()}"
    if args.tag != expected:
        raise SystemExit(f"Release tag {args.tag!r} does not match {expected!r}")
    _scan_credentials()


if __name__ == "__main__":
    main()
