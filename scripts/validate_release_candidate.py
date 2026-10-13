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

"""Resolve and validate a release candidate against protected master."""

from __future__ import annotations

import argparse
import subprocess
import tomllib
from collections.abc import Sequence
from pathlib import Path

if __package__:
    from scripts.check_release import formal_version
else:
    from check_release import formal_version


def _git(*arguments: str) -> str:
    result = subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        ["git", *arguments],  # noqa: S607 -- executable is a fixed local command.
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"git validation failed: git {' '.join(arguments)}")
    return result.stdout.strip()


def _git_succeeds(*arguments: str) -> bool:
    result = subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        ["git", *arguments],  # noqa: S607 -- executable is a fixed local command.
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _resolve(reference: str) -> str:
    return _git("rev-parse", f"{reference}^{{commit}}")


def _project(candidate_sha: str) -> dict[str, object]:
    content = _git("show", f"{candidate_sha}:pyproject.toml")
    project = tomllib.loads(content).get("project")
    if not isinstance(project, dict):
        raise SystemExit("pyproject.toml has no project table")
    return project


def _tree_file_exists(candidate_sha: str, path: str) -> bool:
    result = subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        ["git", "cat-file", "-e", f"{candidate_sha}:{path}"],  # noqa: S607 -- executable is a fixed local command.
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _write_output(path: str, values: dict[str, str]) -> None:
    if not path:
        return
    output = Path(path)
    with output.open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("tag", "dry-run"), required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--expected-version", default="")
    parser.add_argument("--tag", default="")
    parser.add_argument("--master-ref", default="origin/master")
    parser.add_argument("--event-sha", default="")
    parser.add_argument("--event-created", default="false")
    parser.add_argument("--event-deleted", default="false")
    parser.add_argument("--event-forced", default="false")
    parser.add_argument("--github-output", default="")
    options = parser.parse_args(arguments)

    candidate_sha = _resolve(options.candidate_ref)
    master_sha = _resolve(options.master_ref)
    if not _git_succeeds("merge-base", "--is-ancestor", candidate_sha, master_sha):
        raise SystemExit("candidate is not reachable from protected master")

    project = _project(candidate_sha)
    name = project.get("name")
    version = project.get("version")
    if name != "ray-hive" or not isinstance(version, str):
        raise SystemExit("candidate package identity is not ray-hive")
    version = formal_version(version)
    if options.expected_version and version != options.expected_version:
        raise SystemExit(f"candidate version {version!r} does not match expected version")

    expected_tag = f"v{version}"
    tag = options.tag or expected_tag
    if tag != expected_tag:
        raise SystemExit(f"release tag {tag!r} does not match {expected_tag!r}")
    if not _tree_file_exists(candidate_sha, f"release-notes/v{version}.md"):
        raise SystemExit("candidate release notes are missing")

    if options.mode == "tag":
        if options.event_created.lower() != "true":
            raise SystemExit("formal release requires a newly created tag event")
        if options.event_deleted.lower() == "true":
            raise SystemExit("deleted tag event cannot publish")
        if options.event_forced.lower() == "true":
            raise SystemExit("forced tag event cannot publish")
        tag_sha = _resolve(f"refs/tags/{tag}")
        if tag_sha != candidate_sha:
            raise SystemExit("tag peeled commit does not match candidate")
        if options.event_sha and _resolve(options.event_sha) != candidate_sha:
            raise SystemExit("tag event SHA does not resolve to candidate commit")

    values = {
        "mode": options.mode,
        "sha": candidate_sha,
        "version": version,
        "tag": tag,
        "master_sha": master_sha,
    }
    _write_output(options.github_output, values)
    print(f"release candidate verified: {name} {version} at {candidate_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
