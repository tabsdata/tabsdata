#
# Copyright 2025 Tabs Data Inc.
#

import os

import numpy as np
import pandas as pd
import polars as pl

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
NUMBER_OF_PARTITIONS = 10

catalog_definition = {
    "name": "default",
    "type": "glue",
    "client.access-key-id": td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"),
    "client.secret-access-key": td.EnvironmentSecret(
        "TRANSPORTER_AWS_SECRET_ACCESS_KEY"
    ),
    "client.region": "us-east-1",
}

catalog = td.AWSGlue(
    definition=catalog_definition,
    tables="testing_namespace.test_output_s3_catalog_partition",
    if_table_exists="append",
    partitioned_table=True,
)

s3_credentials = td.S3AccessKeyCredentials(
    td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"),
    td.EnvironmentSecret("TRANSPORTER_AWS_SECRET_ACCESS_KEY"),
)


# In this example, we are obtaining the data from the file mock_table.parquet and then
# dropping the null values. The output is saved in output_file_parquet.parquet,
# and expected_result.json
# contains the expected output of applying the function to the input data.
# The URI provided is just a Mock, what will happen is we will inject the URI of
# data.parquet into the input.yaml sent to the tabsserver.
@td.subscriber(
    name="output_s3_catalog_partition",
    tables="collection/table",
    destination=td.S3Destination(
        "s3://tabsdata-testing-bucket/testing_output"
        "/output_s3_catalog_partition_$FRAGMENT_IDX.parquet",
        s3_credentials,
        catalog=catalog,
        region="us-east-1",
    ),
)
def output_s3_catalog_partition(df: td.TableFrame):
    date_range = pd.date_range(start="1900-01-01", end=pd.Timestamp.now(), freq="D")
    random_dates = np.random.choice(date_range, size=NUMBER_OF_PARTITIONS)
    random_numbers = np.random.rand(NUMBER_OF_PARTITIONS)
    polars_df = pl.DataFrame(
        {
            "timestamp": random_dates,
            "random_number": random_numbers,
        }
    )
    polars_df = polars_df.with_columns(polars_df["timestamp"].cast(pl.Datetime("us")))
    tfs = []
    for i in range(NUMBER_OF_PARTITIONS):
        tf = td.TableFrame(polars_df.slice(i, 1).to_dict())
        tfs.append(tf)
    return tfs


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_s3_catalog_partition,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
