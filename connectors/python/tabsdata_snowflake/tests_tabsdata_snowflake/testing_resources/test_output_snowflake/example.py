#
# Copyright 2025 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER

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

connection_parameters = {
    "account": td.EnvironmentSecret("TD_SNOWFLAKE_ACCOUNT"),
    "user": td.EnvironmentSecret("TD_SNOWFLAKE_USER"),
    "password": td.EnvironmentSecret("TD_SNOWFLAKE_PASSWORD"),
    "role": "SYSADMIN",
    "database": "TESTING_DB",
    "schema": "PUBLIC",
    "warehouse": "SNOWFLAKE_LEARNING_WH",
}


@td.subscriber(
    name="output_snowflake",
    tables="collection/table",
    destination=td.SnowflakeDestination(
        connection_parameters, "output_snowflake_table"
    ),
)
def output_snowflake(df: td.TableFrame) -> td.TableFrame:
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_snowflake,
        save_location=DEFAULT_SAVE_LOCATION,
    )
