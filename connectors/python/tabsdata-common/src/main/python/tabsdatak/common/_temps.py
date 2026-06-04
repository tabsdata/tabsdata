#
# Copyright 2026 Tabs Data Inc.
#

"""Tabsdata-owned temp-folder accessor.

A single per-process ``TabsdataTemp`` instance lazily resolves a
``td-temp/`` directory under the platform's temp root; the
``tabsdata_temp_folder()`` helper creates it on demand and returns
the path."""

from __future__ import annotations

import os
import tempfile
from functools import cached_property
from pathlib import Path

TD_TEMP = "td-temp"


class TabsdataTemp:
    def __init__(self):
        self.td_temp = TD_TEMP

    @cached_property
    def temp_root_folder(self):
        return os.path.join(tempfile.gettempdir(), self.td_temp)

    @cached_property
    def temp_root_path(self):
        return Path(self.temp_root_folder)


tabsdata_temp = TabsdataTemp()


def tabsdata_temp_folder() -> str:
    if not tabsdata_temp.temp_root_path.exists():
        os.makedirs(tabsdata_temp.temp_root_folder, exist_ok=True)
    return tabsdata_temp.temp_root_folder
