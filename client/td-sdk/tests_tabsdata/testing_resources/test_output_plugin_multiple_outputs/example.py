#
# Copyright 2025 Tabs Data Inc.
#

import os

from custom_output_plugin_multiple_outputs import CustomDestinationPlugin
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


@td.subscriber(
    name="output_plugin_multiple_outputs",
    tables="collection/table",
    destination=CustomDestinationPlugin(
        destination_json_file=os.path.join(ABSOLUTE_LOCATION, "output.json"),
        second_destination_json_file=os.path.join(
            ABSOLUTE_LOCATION, "second_output.json"
        ),
    ),
)
def output_plugin_multiple_outputs(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df, new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_plugin_multiple_outputs,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
