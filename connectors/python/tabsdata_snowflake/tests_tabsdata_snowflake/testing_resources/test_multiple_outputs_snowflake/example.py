#
# Copyright 2025 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata_snowflake.conftest import REAL_CONNECTION_PARAMETERS

import tabsdata as td
from tabsdata._utils.bundle_utils import create_bundle_archive

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
    name="multiple_outputs_snowflake",
    tables="collection/table",
    destination=td.SnowflakeDestination(
        REAL_CONNECTION_PARAMETERS,
        ["multiple_outputs_snowflake_table_0", "multiple_outputs_snowflake_table_1"],
    ),
)
def multiple_outputs_snowflake(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df, new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        multiple_outputs_snowflake,
        save_location=DEFAULT_SAVE_LOCATION,
    )
