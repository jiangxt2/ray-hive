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
from unittest.mock import Mock

import pytest

from scripts import check_release, validate_release_candidate

PROJECT_VERSION = "1.1"


@pytest.mark.parametrize("version", ["1.0", "1.1", "1.10", "2.0"])
def test_formal_versions_keep_two_component_route(version: str) -> None:
    assert check_release.formal_version(version) == version


@pytest.mark.parametrize("version", ["1.2.dev0", "1.2rc1", "1.2.0", "01.2", 12])
def test_formal_versions_reject_development_and_noncanonical_identities(version: object) -> None:
    with pytest.raises(SystemExit, match=r"canonical major\.minor"):
        check_release.formal_version(version)


def test_tag_validator_rejects_development_metadata(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "1.2.dev0"\n')
    monkeypatch.setattr(check_release, "ROOT", tmp_path)
    with pytest.raises(SystemExit, match=r"canonical major\.minor"):
        check_release._package_version()


@pytest.mark.parametrize("mode", ["dry-run", "tag"])
@pytest.mark.parametrize("version", ["1.2.dev0", "1.2"])
def test_candidate_version_gate(monkeypatch, version: str, mode: str) -> None:
    monkeypatch.setattr(validate_release_candidate, "_resolve", lambda _: "a" * 40)
    monkeypatch.setattr(validate_release_candidate, "_git_succeeds", lambda *_: True)
    monkeypatch.setattr(
        validate_release_candidate, "_project", lambda _: {"name": "ray-hive", "version": version}
    )
    notes = Mock(return_value=True)
    monkeypatch.setattr(validate_release_candidate, "_tree_file_exists", notes)
    arguments = ["--mode", mode, "--candidate-ref", "a" * 40, "--event-created", "true"]
    if version.endswith(".dev0"):
        with pytest.raises(SystemExit, match=r"canonical major\.minor"):
            validate_release_candidate.main(arguments)
        notes.assert_not_called()
    else:
        assert validate_release_candidate.main(arguments) == 0
        notes.assert_called_once_with("a" * 40, "release-notes/v1.2.md")


def _run_validator(root: Path, *arguments: str) -> None:
    subprocess.run(  # noqa: S603 -- executable and arguments are fixed/local.
        [sys.executable, "scripts/verify_release_artifacts.py", *arguments],
        cwd=root,
        check=True,
    )


def _write_distributions(directory: Path) -> None:
    wheel = directory / f"ray_hive-{PROJECT_VERSION}-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            f"ray_hive-{PROJECT_VERSION}.dist-info/METADATA",
            f"Metadata-Version: 2.4\nName: ray-hive\nVersion: {PROJECT_VERSION}\n",
        )
    sdist = directory / f"ray-hive-{PROJECT_VERSION}.tar.gz"
    with tarfile.open(sdist, "w:gz") as archive:
        content = f"Metadata-Version: 2.4\nName: ray-hive\nVersion: {PROJECT_VERSION}\n".encode()
        info = tarfile.TarInfo(f"ray-hive-{PROJECT_VERSION}/PKG-INFO")
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
        PROJECT_VERSION,
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
        PROJECT_VERSION,
        "--manifest",
        str(manifest),
    )
