#
# Copyright 2024 Tabs Data Inc.
#

import os
from typing import List

from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
)
DEFAULT_SAVE_LOCATION = os.path.join(ROOT_PROJECT_DIR, "local_dev")


# In this example, we are obtaining the data from all files that match the
# source_*.log wildcard in the current folder, and then performing a concatenation.
# The output is saved in output.json, and expected_result.json contains the expected
# output of applying the function to the  input data.
@td.publisher(
    td.LocalFileSource(os.path.join(ABSOLUTE_LOCATION, "source_*.log")),
    "output",
)
def input_file_log_wildcard(dataframes: List[td.TableFrame]):
    if len(dataframes) != 2:
        raise ValueError
    else:
        df1 = dataframes[0]
        df2 = dataframes[1]
        return td.concat([df1, df2])


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_file_log_wildcard,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
