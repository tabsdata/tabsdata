#
# Copyright 2025 Tabs Data Inc.
#

import os

from tests_tabsdata_databricks.conftest import (
    DATABRICKS_CATALOG,
    DATABRICKS_HOST,
    DATABRICKS_SCHEMA,
    DATABRICKS_VOLUME,
    DATABRICKS_WAREHOUSE_NAME,
)

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


@td.subscriber(
    name="multiple_outputs_databricks",
    tables="collection/table",
    destination=td.DatabricksDestination(
        DATABRICKS_HOST,
        td.EnvironmentSecret("TD_DATABRICKS_TOKEN"),
        [
            (
                f"{DATABRICKS_CATALOG}."
                f"{DATABRICKS_SCHEMA}.multiple_outputs_databricks_table_1"
            ),
            (
                f"{DATABRICKS_CATALOG}."
                f"{DATABRICKS_SCHEMA}.multiple_outputs_databricks_table_2"
            ),
        ],
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
    ),
)
def multiple_outputs_databricks(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df, new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        multiple_outputs_databricks,
        save_location=DEFAULT_SAVE_LOCATION,
    )
