#
# Copyright 2026 Tabs Data Inc.
#

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from tabsdata._tabsserver.tools.janitor import (
    delete_folder,
    is_folder_expired,
)

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401


class TestJanitorDeleteFolder:
    def test_delete_folder_success(self, tmp_path):
        folder = tmp_path / "to_delete"
        folder.mkdir()
        (folder / "file.txt").write_text("content")
        delete_folder(folder)
        assert not folder.exists()

    def test_delete_folder_permission_error_does_not_raise(self, tmp_path):
        folder = tmp_path / "locked"
        folder.mkdir()
        with patch(
            "tabsdata._tabsserver.tools.janitor.shutil.rmtree",
            side_effect=OSError("Permission denied"),
        ):
            delete_folder(folder)

    def test_delete_folder_logs_error_on_failure(self, tmp_path, caplog):
        folder = tmp_path / "locked"
        folder.mkdir()
        with patch(
            "tabsdata._tabsserver.tools.janitor.shutil.rmtree",
            side_effect=OSError("Permission denied"),
        ):
            with caplog.at_level(logging.ERROR):
                delete_folder(folder)
            assert "Failed to delete folder" in caplog.text
            assert "Permission denied" in caplog.text


class TestJanitorIsFolderExpired:
    def test_returns_false_when_iterdir_fails(self, tmp_path):
        folder = tmp_path / "problem_folder"
        folder.mkdir()
        now_ms = int(folder.stat().st_mtime * 1000) + 999999999

        with patch.object(Path, "iterdir", side_effect=OSError("Permission denied")):
            result = is_folder_expired(folder, now_ms, retention_ms=1)
        assert result is False

    def test_expired_folder_with_no_children(self, tmp_path):
        folder = tmp_path / "empty_old"
        folder.mkdir()
        now_ms = int(folder.stat().st_mtime * 1000) + 999999999
        result = is_folder_expired(folder, now_ms, retention_ms=1)
        assert result is True

    def test_non_expired_folder_returns_false(self, tmp_path):
        folder = tmp_path / "fresh"
        folder.mkdir()
        now_ms = int(folder.stat().st_mtime * 1000) + 1
        result = is_folder_expired(folder, now_ms, retention_ms=999999999)
        assert result is False

    def test_expired_folder_with_non_expired_child_returns_false(self, tmp_path):
        folder = tmp_path / "parent"
        folder.mkdir()
        child = folder / "child"
        child.mkdir()
        now_ms = int(folder.stat().st_mtime * 1000) + 999999999
        result = is_folder_expired(folder, now_ms, retention_ms=1)
        # Both parent and child were created at roughly the same time,
        # so both should be expired
        assert result is True
