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

"""Validate Signed-off-by trailers for an explicit Git commit range."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_ZERO_SHA = "0" * 40
_SIGN_OFF = re.compile(r"^.+\s<[^<>\s]+@[^<>\s]+>$")


class DcoValidationError(RuntimeError):
    """A Git range or trailer could not be validated safely."""


def _git(
    root: Path,
    *arguments: str,
    input_text: str | None = None,
) -> str:
    result = subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        ["git", *arguments],  # noqa: S607 -- executable is a fixed local command.
        cwd=root,
        input=input_text,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        command = " ".join(arguments)
        raise DcoValidationError(f"Git validation failed: git {command}")
    return result.stdout


def _resolve(root: Path, reference: str) -> str:
    resolved = _git(root, "rev-parse", "--verify", f"{reference}^{{commit}}").strip()
    if not re.fullmatch(r"[0-9a-f]{40}", resolved):
        raise DcoValidationError("Git returned an invalid commit SHA")
    return resolved


def commits_in_range(root: Path, *, base: str, head: str) -> tuple[str, ...]:
    """Return commits that require DCO validation in oldest-first order."""

    head_sha = _resolve(root, head)
    if not base or base == _ZERO_SHA:
        revision = head_sha
    else:
        base_sha = _resolve(root, base)
        ancestor = subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
            ["git", "merge-base", "--is-ancestor", base_sha, head_sha],  # noqa: S607
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if ancestor.returncode != 0:
            raise DcoValidationError("DCO base is not an ancestor of head")
        revision = f"{base_sha}..{head_sha}"

    output = _git(root, "rev-list", "--reverse", revision)
    commits = tuple(line for line in output.splitlines() if line)
    if any(re.fullmatch(r"[0-9a-f]{40}", commit) is None for commit in commits):
        raise DcoValidationError("Git returned an invalid commit range")
    return commits


def _trailers(root: Path, commit: str) -> tuple[tuple[str, str], ...]:
    message = _git(root, "show", "-s", "--format=%B", commit)
    parsed = _git(root, "interpret-trailers", "--parse", input_text=message)
    trailers: list[tuple[str, str]] = []
    for line in parsed.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            raise DcoValidationError(f"Commit {commit} has a malformed trailer block")
        trailers.append((key.strip(), value.strip()))
    return tuple(trailers)


def commit_has_valid_signoff(root: Path, commit: str) -> bool:
    """Return whether a commit has at least one valid DCO sign-off trailer."""

    return any(
        key.casefold() == "signed-off-by" and _SIGN_OFF.fullmatch(value) is not None
        for key, value in _trailers(root, commit)
    )


def dco_failures(root: Path, *, base: str, head: str) -> tuple[str, ...]:
    """Return every commit in the range that lacks a valid sign-off."""

    return tuple(
        commit
        for commit in commits_in_range(root, base=base, head=head)
        if not commit_has_valid_signoff(root, commit)
    )


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", required=True)
    parser.add_argument("--repo", type=Path, default=ROOT)
    options = parser.parse_args(arguments)

    try:
        commits = commits_in_range(options.repo, base=options.base, head=options.head)
        failures = tuple(
            commit for commit in commits if not commit_has_valid_signoff(options.repo, commit)
        )
    except DcoValidationError as exc:
        raise SystemExit(str(exc)) from None

    if failures:
        for commit in failures:
            print(f"Missing valid Signed-off-by trailer: {commit}")
        return 1
    print(f"DCO validation passed for {len(commits)} commit(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
