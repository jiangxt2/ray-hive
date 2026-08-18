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

"""Verify package metadata and a SHA-256 manifest for release artifacts."""

from __future__ import annotations

import argparse
import hashlib
import tarfile
import zipfile
from collections.abc import Sequence
from pathlib import Path


def _metadata(path: Path) -> tuple[str, str]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(names) != 1:
                raise ValueError(f"{path.name} must contain one dist-info/METADATA")
            content = archive.read(names[0]).decode("utf-8")
    elif path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            names = [member for member in archive.getmembers() if member.name.endswith("/PKG-INFO")]
            if len(names) != 1 or not names[0].isreg():
                raise ValueError(f"{path.name} must contain one PKG-INFO")
            extracted = archive.extractfile(names[0])
            if extracted is None:
                raise ValueError(f"{path.name} has unreadable PKG-INFO")
            content = extracted.read().decode("utf-8")
    else:
        raise ValueError(f"unsupported distribution: {path.name}")

    fields: dict[str, str] = {}
    for line in content.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            fields.setdefault(key, value)
    return fields.get("Name", ""), fields.get("Version", "")


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _packages(directory: Path) -> list[Path]:
    packages = sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and (path.suffix == ".whl" or path.name.endswith(".tar.gz"))
    )
    wheels = [path for path in packages if path.suffix == ".whl"]
    sdists = [path for path in packages if path.name.endswith(".tar.gz")]
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit("release directory must contain exactly one wheel and one sdist")
    return packages


def _validate(packages: list[Path], name: str, version: str) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for path in packages:
        metadata_name, metadata_version = _metadata(path)
        if metadata_name != name or metadata_version != version:
            raise SystemExit(f"{path.name} metadata does not match {name} {version}")
        manifest[path.name] = _digest(path)
    return manifest


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    options = parser.parse_args(arguments)

    packages = _packages(options.directory)
    expected = _validate(packages, options.name, options.version)
    if options.prepare:
        options.manifest.parent.mkdir(parents=True, exist_ok=True)
        content = "".join(f"{digest}  {name}\n" for name, digest in sorted(expected.items()))
        options.manifest.write_text(content, encoding="utf-8")
        print(f"prepared manifest: {options.manifest}")
        return 0

    actual: dict[str, str] = {}
    for line in options.manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, filename = line.split(maxsplit=1)
        actual[filename.strip()] = digest
    if actual != expected:
        raise SystemExit("SHA-256 manifest does not match release artifacts")
    print(f"verified artifacts: {options.name} {options.version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
