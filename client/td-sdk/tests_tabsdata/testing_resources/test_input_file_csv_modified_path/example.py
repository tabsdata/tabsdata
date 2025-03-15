#
# Copyright 2024 Tabs Data Inc.
#

import os

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

# In this example, we are obtaining the data from the file data.csv and then dropping
# the null values. The output is saved in output.json, and expected_result.json
# contains the expected output of applying the function to the input data.
local_file_input = td.LocalFileSource(
    os.path.join(ABSOLUTE_LOCATION, "data.csv"),
    format=td.CSVFormat(separator=";"),
)
local_file_input.path = os.path.join(ABSOLUTE_LOCATION, "data")


@td.publisher(
    local_file_input,
    "output",
)
def input_file_csv_modified_path(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_file_csv_modified_path,
        local_packages=LOCAL_PACKAGES_LIST,
        path_to_code=os.path.dirname(ABSOLUTE_LOCATION),
        save_location=DEFAULT_SAVE_LOCATION,
    )
