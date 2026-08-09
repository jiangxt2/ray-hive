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

"""Reproduce the vendored Apache Hive 4.2.0 TCLIService Python bindings."""

from __future__ import annotations

import argparse
import filecmp
import hashlib
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GENERATED_ROOT = ROOT / "src/ray_hive/_hs2/generated"
IDL = GENERATED_ROOT / "idl/TCLIService.thrift"
EXPECTED_IDL_SHA256 = "0146013b0bf6f9d794364dac40d1537662708aa62c548548191eb5cd821c391b"
IMAGE = "ray-hive-thrift:0.16.0"
PACKAGE_FILES = (
    "TCLIService-remote",
    "TCLIService.py",
    "__init__.py",
    "constants.py",
    "ttypes.py",
)


def _run(command: list[str]) -> None:
    subprocess.run(command, check=True)


def _validate_idl() -> None:
    actual = hashlib.sha256(IDL.read_bytes()).hexdigest()
    if actual != EXPECTED_IDL_SHA256:
        raise SystemExit(f"IDL checksum mismatch: expected {EXPECTED_IDL_SHA256}, got {actual}")


def _generate(output: Path) -> Path:
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-v",
            f"{IDL}:/idl/TCLIService.thrift:ro",
            "-v",
            f"{output}:/out",
            IMAGE,
            "--strict",
            "--gen",
            "py:slots,package_prefix=ray_hive._hs2.generated.",
            "-out",
            "/out",
            "/idl/TCLIService.thrift",
        ]
    )
    return output / "TCLIService"


def _compare(actual: Path, expected: Path) -> None:
    differences = [
        name
        for name in PACKAGE_FILES
        if not filecmp.cmp(actual / name, expected / name, shallow=False)
    ]
    if differences:
        raise SystemExit("Generated bindings differ: " + ", ".join(differences))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-image", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.check and args.write:
        parser.error("--check and --write are mutually exclusive")

    _validate_idl()
    if args.build_image:
        _run(
            [
                "docker",
                "buildx",
                "build",
                "--load",
                "-t",
                IMAGE,
                "-f",
                str(ROOT / "scripts/thrift-compiler.Dockerfile"),
                str(ROOT),
            ]
        )

    with tempfile.TemporaryDirectory(prefix="ray-hive-thrift-") as temporary:
        generated = _generate(Path(temporary))
        expected = GENERATED_ROOT / "TCLIService"
        if args.write:
            shutil.copytree(generated, expected, dirs_exist_ok=True)
        else:
            _compare(generated, expected)


if __name__ == "__main__":
    main()
