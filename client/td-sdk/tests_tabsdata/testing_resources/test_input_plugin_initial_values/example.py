#
# Copyright 2024 Tabs Data Inc.
#

import os

from custom_importer_with_initial_values import ImporterWithInitialValues
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


# In this example, we are obtaining the data from a file that matches the
# source_*.csv wildcard and with * equal the value of file_number in initial values,
# and then returning it as is and increasing the number.
# The output is saved in output.json, and expected_result.json contains the expected
# output of applying the function to the  input data.
@td.publisher(
    name="test_input_plugin_initial_values",
    source=ImporterWithInitialValues(folder=f"{ABSOLUTE_LOCATION}", file_number=1),
    tables="output",
)
def input_plugin_initial_values(df: td.TableFrame):
    return df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_plugin_initial_values,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
