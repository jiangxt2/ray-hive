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

import subprocess
from pathlib import Path

import pytest

from scripts.check_dco import DcoValidationError, commits_in_range, dco_failures, main


def _git(root: Path, *arguments: str, input_text: str | None = None) -> str:
    result = subprocess.run(  # noqa: S603 -- test helper invokes local Git only.
        ["git", *arguments],  # noqa: S607 -- Git is the fixed test executable.
        cwd=root,
        input=input_text,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Test User")
    _git(root, "config", "user.email", "test@example.com")
    return root


def _commit(root: Path, message: str) -> str:
    tracked = root / "tracked.txt"
    previous = tracked.read_text(encoding="utf-8") if tracked.exists() else ""
    tracked.write_text(previous + message.splitlines()[0] + "\n", encoding="utf-8")
    _git(root, "add", "tracked.txt")
    _git(root, "commit", "-F", "-", input_text=message)
    return _git(root, "rev-parse", "HEAD")


def test_dco_reports_every_missing_or_malformed_signoff(tmp_path: Path) -> None:
    root = _repository(tmp_path)
    base = _commit(
        root,
        "Initial\n\nSigned-off-by: Test User <test@example.com>\n",
    )
    missing = _commit(root, "Missing signoff\n")
    malformed = _commit(root, "Malformed\n\nSigned-off-by: missing-email\n")

    assert commits_in_range(root, base=base, head=malformed) == (missing, malformed)
    assert dco_failures(root, base=base, head=malformed) == (missing, malformed)


def test_dco_accepts_case_insensitive_trailer_and_empty_range(tmp_path: Path) -> None:
    root = _repository(tmp_path)
    commit = _commit(
        root,
        "Signed\n\nsigned-off-by: Another User <another@example.com>\n",
    )

    assert dco_failures(root, base="", head=commit) == ()
    assert commits_in_range(root, base="0" * 40, head=commit) == (commit,)
    assert commits_in_range(root, base=commit, head=commit) == ()
    assert main(["--repo", str(root), "--base", commit, "--head", commit]) == 0


def test_dco_does_not_accept_signoff_text_outside_trailer_block(tmp_path: Path) -> None:
    root = _repository(tmp_path)
    commit = _commit(
        root,
        "Body only\n\nSigned-off-by: Test User <test@example.com>\n\nMore body text\n",
    )

    assert dco_failures(root, base="", head=commit) == (commit,)


def test_dco_rejects_unknown_references_and_non_ancestor_base(tmp_path: Path) -> None:
    root = _repository(tmp_path)
    base = _commit(root, "Base\n\nSigned-off-by: Test User <test@example.com>\n")
    _git(root, "checkout", "-b", "side")
    side = _commit(root, "Side\n\nSigned-off-by: Test User <test@example.com>\n")
    _git(root, "checkout", "main")
    head = _commit(root, "Head\n\nSigned-off-by: Test User <test@example.com>\n")

    with pytest.raises(DcoValidationError, match="not an ancestor"):
        commits_in_range(root, base=side, head=head)
    with pytest.raises(DcoValidationError, match="Git validation failed"):
        commits_in_range(root, base=base, head="missing")


def test_dco_main_returns_failure_after_reporting_all_commits(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _repository(tmp_path)
    first = _commit(root, "First\n")
    second = _commit(root, "Second\n")

    assert main(["--repo", str(root), "--head", second]) == 1
    output = capsys.readouterr().out
    assert first in output
    assert second in output
