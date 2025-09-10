#
# Copyright 2025 Tabs Data Inc.
#

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
