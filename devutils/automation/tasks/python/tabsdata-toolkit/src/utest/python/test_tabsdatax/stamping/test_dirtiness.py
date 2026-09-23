#
# Copyright 2026 Tabsdata Inc.
#

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tabsdatax.setup._dirtiness import changed_tracked_files, repository_dirty

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX symlinks"),
]


def run_git(folder: Path, *arguments: str) -> None:
    subprocess.run(["git", "-C", str(folder), *arguments], check=True)


def init_repository(path: Path) -> None:
    path.mkdir(parents=True)
    run_git(path, "init", "-q")
    run_git(path, "config", "user.email", "test@tabsdata.com")
    run_git(path, "config", "user.name", "test")
    run_git(path, "config", "core.symlinks", "true")


def commit_all(path: Path) -> None:
    run_git(path, "add", "-A")
    run_git(path, "commit", "-q", "-m", "init")


def materialize(repository: Path, link: str) -> None:
    path = repository / link
    target = path.resolve(strict=True)
    path.unlink()
    if target.is_dir():
        shutil.copytree(target, path, symlinks=True)
    else:
        shutil.copy2(target, path)


@pytest.fixture
def overlay(tmp_path: Path) -> Path:
    tmp_path = tmp_path.resolve()
    owner = tmp_path / "owner"
    overlay = tmp_path / "overlay"
    init_repository(owner)
    (owner / "folder" / "nested").mkdir(parents=True)
    (owner / "file.txt").write_text("file")
    (owner / "folder" / "a.txt").write_text("a")
    (owner / "folder" / "nested" / "b.txt").write_text("b")
    commit_all(owner)
    init_repository(overlay)
    os.symlink("../owner/file.txt", overlay / "file.txt")
    os.symlink("../owner/folder", overlay / "folder")
    (overlay / "own.txt").write_text("own")
    commit_all(overlay)
    return overlay


def test_identical_materializations_are_clean_in_strict_mode_only(overlay: Path):
    materialize(overlay, "file.txt")
    materialize(overlay, "folder")
    (overlay / "folder" / "untracked.log").write_text("build output")
    assert changed_tracked_files(overlay, True) == []
    assert len(changed_tracked_files(overlay, False)) == 2
    assert repository_dirty("overlay", overlay, True) == "false"
    assert repository_dirty("overlay", overlay, False) == "true"


def test_modified_materializations_are_real_changes(overlay: Path):
    materialize(overlay, "file.txt")
    materialize(overlay, "folder")
    (overlay / "file.txt").write_text("changed")
    (overlay / "folder" / "nested" / "b.txt").unlink()
    changes = changed_tracked_files(overlay, True)
    assert len(changes) == 2
    assert all(change.reason for change in changes)
    with pytest.raises(RuntimeError, match="2 modified tracked file"):
        repository_dirty("overlay", overlay, True)


def test_regular_changes_are_real_changes_in_strict_mode(overlay: Path):
    (overlay / "own.txt").write_text("changed")
    (overlay / "file.txt").unlink()
    changes = changed_tracked_files(overlay, True)
    assert [change.path for change in changes] == ["file.txt", "own.txt"]
    assert changes[0].reason is not None
    assert changes[1].reason is None
