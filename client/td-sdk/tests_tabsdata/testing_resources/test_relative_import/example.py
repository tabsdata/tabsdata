#
# Copyright 2024 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive

from .custom_importer_relative_import import Importer

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
# source_*.csv wildcard in the current folder, and then performing an inner join by ID.
# The output is saved in output.json, and expected_result.json contains the expected
# output of applying the function to the  input data.
@td.publisher(
    name="test_relative_import",
    source=Importer(folder=f"{ABSOLUTE_LOCATION}", file="source_1.csv"),
    tables="output",
)
def relative_import(df: td.TableFrame):
    return df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        relative_import,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
