#
# Copyright 2025 Tabs Data Inc.
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


# In this example, we are obtaining the data from the file mock_table.parquet and then
# dropping the null values. The output is saved in output.json, and expected_result.json
# contains the expected output of applying the function to the input data.
# The URI provided is just a Mock, what will happen is we will inject the URI of
# data.parquet into the input.yaml sent to the tabsserver.
@td.transformer(
    "collection/table",
    "output",
)
def output_table_with_none(df: td.TableFrame):
    df.drop_nulls()
    return None


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_table_with_none,
        local_packages=ROOT_PROJECT_DIR,
        save_location=DEFAULT_SAVE_LOCATION,
    )
