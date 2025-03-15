#
# Copyright 2024 Tabs Data Inc.
#

import os

from custom_importer import Importer

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
# source_*.csv wildcard in the current folder, and then performing an inner join by ID.
# The output is saved in output.json, and expected_result.json contains the expected
# output of applying the function to the  input data.
@td.publisher(
    name="test_input_plugin_multiple_inputs",
    source=Importer(folder=f"{ABSOLUTE_LOCATION}", file="source_1.csv"),
    tables="output",
)
def input_plugin_multiple_inputs(df: td.TableFrame, df2: td.TableFrame):
    return td.concat([df, df2])


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_plugin_multiple_inputs,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
