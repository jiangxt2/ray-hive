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

"""Verify that a remote tag still resolves to the release candidate commit."""

from __future__ import annotations

import argparse
import subprocess
from collections.abc import Sequence


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-sha", required=True)
    options = parser.parse_args(arguments)
    if not options.tag.startswith("v") or any(char.isspace() for char in options.tag):
        raise SystemExit("release tag must start with v and contain no whitespace")

    reference = f"refs/tags/{options.tag}"
    result = subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        ["git", "ls-remote", "origin", reference, f"{reference}^{{}}"],  # noqa: S607 -- executable is a fixed local command.
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit("remote release tag lookup failed")
    identities: dict[str, str] = {}
    for line in result.stdout.splitlines():
        values = line.split()
        if len(values) != 2:
            raise SystemExit("remote release tag lookup returned malformed output")
        identities[values[1]] = values[0]
    resolved = identities.get(f"{reference}^{{}}", identities.get(reference, ""))
    if resolved != options.expected_sha:
        raise SystemExit("remote release tag does not match the release candidate")
    print(f"remote release tag verified: {options.tag} -> {options.expected_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
