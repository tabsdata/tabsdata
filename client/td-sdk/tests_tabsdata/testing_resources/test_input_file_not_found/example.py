#
# Copyright 2025 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
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
DEFAULT_SAVE_LOCATION = TDLOCAL_FOLDER


# In this example, we are obtaining the data from the file data.csv and then dropping
# the null values. The output is saved in output.json, and expected_result.json
# contains the expected output of applying the function to the input data.
@td.publisher(
    td.LocalFileSource(os.path.join(ABSOLUTE_LOCATION, "not_found"), format="csv"),
    "output",
)
def input_file_not_found(df: td.TableFrame):
    assert df is None  # This is the key part of the test, we expect df to be None
    return df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_file_not_found,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
