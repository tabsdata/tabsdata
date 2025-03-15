#
# Copyright 2025 Tabs Data Inc.
#

import os
import sys

from tests_tabsdata.bootest import enrich_sys_path, root_folder
from tests_tabsdata_salesforce.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    root = root_folder()
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "connectors",
                "python",
                "tabsdata_salesforce",
            )
        ),
    )


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()
