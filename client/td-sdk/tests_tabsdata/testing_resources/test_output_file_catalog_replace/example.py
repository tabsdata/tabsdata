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
DEFAULT_SAVE_LOCATION = os.path.join(ROOT_PROJECT_DIR, "client", "td-sdk", "local_dev")

warehouse_path = os.path.join(
    DEFAULT_SAVE_LOCATION, "output_file_catalog_replace_warehouse"
)

catalog_definition = {
    "name": "default",
    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
    "warehouse": f"file://{warehouse_path}",
}

catalog = td.Catalog(
    definition=catalog_definition,
    tables=[
        "testing_namespace.output_file_parquet",
        "testing_namespace.second_output_file",
    ],
    if_table_exists="replace",
)


# In this example, we are obtaining the data from the file mock_table.parquet and then
# dropping the null values. The output is saved in output_file_parquet.parquet,
# and expected_result.json
# contains the expected output of applying the function to the input data.
# The URI provided is just a Mock, what will happen is we will inject the URI of
# data.parquet into the input.yaml sent to the tabsserver.
@td.subscriber(
    name="output_file_catalog_replace",
    tables="collection/table",
    destination=td.LocalFileDestination(
        [
            os.path.join(DEFAULT_SAVE_LOCATION, "output_file_parquet.parquet"),
            os.path.join(DEFAULT_SAVE_LOCATION, "second_output_file.parquet"),
        ],
        catalog=catalog,
    ),
)
def output_file_catalog_replace(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df, new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_file_catalog_replace,
        local_packages=ROOT_PROJECT_DIR,
        save_location=DEFAULT_SAVE_LOCATION,
    )
