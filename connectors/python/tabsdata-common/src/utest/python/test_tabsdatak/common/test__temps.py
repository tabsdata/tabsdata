#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os
from pathlib import Path

# noinspection PyProtectedMember
from tabsdatak.common._temps import TD_TEMP, TabsdataTemp, tabsdata_temp_folder


def test_td_temp_constant_is_stable():
    assert TD_TEMP == "td-temp"


def test_tabsdata_temp_folder_creates_and_returns_existing_path():
    folder = tabsdata_temp_folder()
    assert Path(folder).is_dir()
    # Second call returns the same string and the directory is still there.
    assert tabsdata_temp_folder() == folder
    assert Path(folder).is_dir()


def test_tabsdatatemp_cached_property_returns_same_object():
    t = TabsdataTemp()
    first = t.temp_root_folder
    second = t.temp_root_folder
    assert first is second
    # And matches the platform temp root + td-temp.
    import tempfile

    assert first == os.path.join(tempfile.gettempdir(), TD_TEMP)


def test_tabsdatatemp_temp_root_path_matches_folder():
    t = TabsdataTemp()
    assert str(t.temp_root_path) == t.temp_root_folder
