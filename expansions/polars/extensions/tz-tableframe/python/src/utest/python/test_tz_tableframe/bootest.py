#
# Copyright 2026 Tabsdata Inc.
#

import os
import sys
from pathlib import Path

TESTS_ROOT_FOLDER = str(Path(__file__).absolute().parent)

# fmt: off
TESTING_RESOURCES_PATH = str(
    Path(__file__).absolute()
    .parent  # test_tz_tableframe/
    .parent  # python/
    .parent  # utest/
    / "resources"
    / "test_tz_tableframe"
)
# fmt: on

if os.path.abspath(TESTING_RESOURCES_PATH) not in sys.path:
    sys.path.append(os.path.abspath(TESTING_RESOURCES_PATH))


def _enrich_sys_path():
    pass
