#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import pytest

from tabsdata._utils.temps import TabsdataTemp, tabsdata_temp_folder

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


class TestTabsdataTemp:
    def test_temp_root_folder_is_string(self):
        t = TabsdataTemp()
        assert isinstance(t.temp_root_folder, str)
        assert "td-temp" in t.temp_root_folder

    def test_temp_root_path_is_path(self):
        t = TabsdataTemp()
        assert isinstance(t.temp_root_path, Path)

    def test_tabsdata_temp_folder_creates_dir(self):
        folder = tabsdata_temp_folder()
        assert os.path.isdir(folder)
        assert "td-temp" in folder
