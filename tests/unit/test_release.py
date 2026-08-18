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

import io
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path


def _run_validator(root: Path, *arguments: str) -> None:
    subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        [sys.executable, "scripts/verify_release_artifacts.py", *arguments],
        cwd=root,
        check=True,
    )


def _write_distributions(directory: Path) -> None:
    wheel = directory / "ray_hive-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "ray_hive-1.0.dist-info/METADATA",
            "Metadata-Version: 2.4\nName: ray-hive\nVersion: 1.0\n",
        )
    sdist = directory / "ray-hive-1.0.tar.gz"
    with tarfile.open(sdist, "w:gz") as archive:
        content = b"Metadata-Version: 2.4\nName: ray-hive\nVersion: 1.0\n"
        info = tarfile.TarInfo("ray-hive-1.0/PKG-INFO")
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))


def test_release_artifact_manifest_round_trip(tmp_path: Path) -> None:
    root = Path(__file__).parents[2]
    packages = tmp_path / "packages"
    packages.mkdir()
    _write_distributions(packages)
    manifest = tmp_path / "SHA256SUMS"
    _run_validator(
        root,
        str(packages),
        "--name",
        "ray-hive",
        "--version",
        "1.0",
        "--manifest",
        str(manifest),
        "--prepare",
    )
    _run_validator(
        root,
        str(packages),
        "--name",
        "ray-hive",
        "--version",
        "1.0",
        "--manifest",
        str(manifest),
    )
