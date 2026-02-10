#
# Copyright 2025 Tabs Data Inc.
#

import os
import sys
from pathlib import Path

# fmt: off
TESTING_RESOURCES_PATH = str(
    Path(__file__).absolute()
    .parent  # test_te_variant/
    .parent  # python/
    .parent  # utest/
    / "resources"
    / "test_te_variant"
)
# fmt: on

TESTING_RESOURCES_PATH_PARENT = str(Path(TESTING_RESOURCES_PATH).parent)

if os.path.abspath(TESTING_RESOURCES_PATH_PARENT) not in sys.path:
    sys.path.append(os.path.abspath(TESTING_RESOURCES_PATH_PARENT))
