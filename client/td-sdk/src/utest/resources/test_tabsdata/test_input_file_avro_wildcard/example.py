#
# Copyright 2025 Tabs Data Inc.
#

import os
from typing import List

import tabsdata as td
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
)
DEFAULT_SAVE_LOCATION = TDLOCAL_FOLDER


# In this example, we are obtaining the data from all files that match the
# source_*.log wildcard in the current folder, and then performing a concatenation.
# The output is saved in output.json, and expected_result.json contains the expected
# output of applying the function to the  input data.
@td.publisher(
    td.LocalFileSource(os.path.join(ABSOLUTE_LOCATION, "source_*.avro")),
    "output",
)
def input_file_avro_wildcard(dataframes: List[td.TableFrame]):
    if len(dataframes) != 2:
        raise ValueError
    else:
        df1 = dataframes[0]
        df2 = dataframes[1]
        return df1.join(df2, on="ID", how="inner")


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_file_avro_wildcard,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
