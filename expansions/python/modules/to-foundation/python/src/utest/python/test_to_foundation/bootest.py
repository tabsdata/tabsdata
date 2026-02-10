#
# Copyright 2025 Tabs Data Inc.
#

import os
import sys

TESTS_ROOT_FOLDER = os.path.dirname(__file__)

TESTING_RESOURCES_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",  # Go up from test_to_foundation/ to python/
        "..",  # Go up from python/ to utest/
        "resources",
        "test_to_foundation",  # Resources subfolder matching test package
    )
)

# Add resources path to sys.path for direct imports
if os.path.abspath(TESTING_RESOURCES_PATH) not in sys.path:
    sys.path.append(os.path.abspath(TESTING_RESOURCES_PATH))


def _enrich_sys_path():
    pass
