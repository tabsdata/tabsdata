#
# Copyright 2024 Tabs Data Inc.
#

import os

from custom_output_plugin_with_none import CustomDestinationPlugin
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


@td.subscriber(
    name="output_plugin_with_none",
    tables="collection/table",
    destination=CustomDestinationPlugin(
        destination_json_file=os.path.join(ABSOLUTE_LOCATION, "output.json")
    ),
)
def output_plugin_with_none(df: td.TableFrame):
    df.drop_nulls()
    return None


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_plugin_with_none,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
