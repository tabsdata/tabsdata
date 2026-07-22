#
# Copyright 2025 Tabsdata Inc.
#

import os
import sys

TESTING_RESOURCES_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",  # Go up from test_tabsdata_mssql/ to python/
        "..",  # Go up from python/ to utest/
        "resources",
        "test_tabsdata_mssql",  # Resources subfolder matching test package
    )
)

# Add resources path to sys.path for direct imports
if os.path.abspath(TESTING_RESOURCES_PATH) not in sys.path:
    sys.path.append(os.path.abspath(TESTING_RESOURCES_PATH))
