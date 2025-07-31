#
# Copyright 2025 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata_databricks.conftest import (
    DATABRICKS_CATALOG,
    DATABRICKS_HOST,
    DATABRICKS_SCHEMA,
    DATABRICKS_VOLUME,
    DATABRICKS_WAREHOUSE_NAME,
)

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
    name="output_databricks",
    tables="collection/table",
    destination=td.DatabricksDestination(
        DATABRICKS_HOST,
        td.EnvironmentSecret("TD_DATABRICKS_TOKEN"),
        "output_databricks_table",
        DATABRICKS_VOLUME,
        warehouse=DATABRICKS_WAREHOUSE_NAME,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
    ),
)
def output_databricks_none(df: td.TableFrame) -> td.TableFrame | None:
    df.drop_nulls()
    return None


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_databricks_none,
        save_location=DEFAULT_SAVE_LOCATION,
    )
