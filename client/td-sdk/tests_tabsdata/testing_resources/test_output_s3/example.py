#
# Copyright 2024 Tabs Data Inc.
#

import os

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

s3_credentials = td.S3AccessKeyCredentials(
    os.environ.get("TRANSPORTER_AWS_ACCESS_KEY_ID", "FAKE_ID"),
    os.environ.get("TRANSPORTER_AWS_SECRET_ACCESS_KEY", "FAKE_KEY"),
)


# In this example, we are obtaining the data from the file mock_table.parquet and then
# dropping the null values. The output is saved in output_s3_parquet.parquet,
# and expected_result.json
# contains the expected output of applying the function to the input data.
# The URI provided is just a Mock, what will happen is we will inject the URI of
# data.parquet into the input.yaml sent to the tabsserver.
@td.subscriber(
    "collection/table",
    td.S3Destination(
        "s3://tabsdata-testing-bucket/testing_output/output_s3_parquet.parquet",
        s3_credentials,
    ),
)
def output_s3(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_s3,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
