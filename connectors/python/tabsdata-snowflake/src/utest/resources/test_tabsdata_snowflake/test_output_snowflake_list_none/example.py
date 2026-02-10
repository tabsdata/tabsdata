#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER

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
    "password": td.EnvironmentSecret("TD_SNOWFLAKE_PAT"),
    "role": td.EnvironmentSecret("TD_SNOWFLAKE_ROLE"),
    "database": td.EnvironmentSecret("TD_SNOWFLAKE_DATABASE"),
    "schema": td.EnvironmentSecret("TD_SNOWFLAKE_SCHEMA"),
    "warehouse": td.EnvironmentSecret("TD_SNOWFLAKE_WAREHOUSE"),
}


@td.subscriber(
    name="multiple_outputs_snowflake",
    tables="collection/table",
    destination=td.SnowflakeDestination(
        connection_parameters,
        ["output_snowflake_table_0", "output_snowflake_table_1"],
    ),
)
def output_snowflake_list_none(
    df: td.TableFrame,
) -> tuple[td.TableFrame | None, td.TableFrame | None]:
    df.drop_nulls()
    return None, None


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_snowflake_list_none,
        save_location=DEFAULT_SAVE_LOCATION,
    )
