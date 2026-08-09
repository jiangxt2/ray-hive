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

"""Validate the vendored HS2/Arrow conformance specification and purity boundary."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import jsonschema

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "spec/hs2-arrow/v1"
IDL = ROOT / "src/ray_hive/_hs2/generated/idl/TCLIService.thrift"
REQUIRED_CASES = {
    "HS2-PROTO-001",
    "HS2-PROTO-002",
    "HS2-PROTO-003",
    "HS2-PROTO-004",
    "HS2-TABLE-001",
    "HS2-TABLE-002",
    "HS2-TABLE-003",
    "HS2-ACID-001",
    "HS2-ICEBERG-001",
    "HS2-TYPE-001",
    "HS2-TYPE-002",
    "HS2-TYPE-003",
    "HS2-CONSISTENCY-001",
    "HS2-FAILURE-001",
    "HS2-FAILURE-002",
    "HS2-FAILURE-003",
    "HS2-SECURITY-001",
    "HS2-PURITY-001",
    "HS2-PURITY-002",
}
REQUIRED_ERRORS = {
    "CONFIG",
    "AUTH",
    "PERMISSION",
    "QUERY_COMPILE",
    "QUERY_EXECUTION",
    "SCHEMA",
    "PROTOCOL",
    "TRANSPORT_BEFORE_OUTPUT",
    "TRANSPORT_AFTER_OUTPUT",
    "CANCELLED",
    "CLEANUP",
}
BANNED_SOURCE_TEXT = (
    "ray.data._internal",
    "import daft",
    "from daft",
    "import pyiceberg",
    "from pyiceberg",
    "import tributo",
    "from tributo",
)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise SystemExit(f"Expected a JSON object: {path.relative_to(ROOT)}")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require_unique(values: list[str], label: str) -> None:
    if len(values) != len(set(values)):
        raise SystemExit(f"Duplicate {label}")


def main() -> None:
    manifest = _load(SPEC / "manifest.json")
    errors = _load(SPEC / "errors.json")
    jsonschema.validate(manifest, _load(SPEC / "schema/manifest.schema.json"))
    jsonschema.validate(errors, _load(SPEC / "schema/errors.schema.json"))

    version = (SPEC / "SPEC_VERSION").read_text().strip()
    if version != manifest["spec_version"] or version != errors["spec_version"]:
        raise SystemExit("Conformance spec versions do not match")

    case_ids = [case["id"] for case in manifest["cases"]]
    _require_unique(case_ids, "case ID")
    if set(case_ids) != REQUIRED_CASES:
        raise SystemExit("Conformance case set differs from the required V1 set")

    error_codes = [error["code"] for error in errors["errors"]]
    _require_unique(error_codes, "error code")
    if set(error_codes) != REQUIRED_ERRORS:
        raise SystemExit("Conformance error set differs from the required V1 set")

    if _sha256(IDL) != manifest["hive_idl"]["sha256"]:
        raise SystemExit("Vendored Hive IDL checksum differs from manifest")
    for fixture in manifest["fixtures"]:
        relative = Path(fixture["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise SystemExit(f"Unsafe fixture path: {relative}")
        path = SPEC / relative
        if not path.is_file() or _sha256(path) != fixture["sha256"]:
            raise SystemExit(f"Fixture checksum mismatch: {relative}")

    capabilities = _load(SPEC / "capabilities.json")["capabilities"]
    capability_ids = [capability["id"] for capability in capabilities]
    _require_unique(capability_ids, "capability ID")

    for path in (ROOT / "src/ray_hive").rglob("*.py"):
        if "generated" in path.parts:
            continue
        source = path.read_text().lower()
        for banned in BANNED_SOURCE_TEXT:
            if banned in source:
                raise SystemExit(
                    f"Forbidden runtime dependency in {path.relative_to(ROOT)}: {banned}"
                )


if __name__ == "__main__":
    main()
