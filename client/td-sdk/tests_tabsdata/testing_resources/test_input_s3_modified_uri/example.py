#
# Copyright 2024 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

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

s3_credentials = td.S3AccessKeyCredentials(
    os.environ.get("TRANSPORTER_AWS_ACCESS_KEY_ID", "FAKE_ID"),
    os.environ.get("TRANSPORTER_AWS_SECRET_ACCESS_KEY", "FAKE_KEY"),
)

# In this example, we are obtaining the data from the file data.csv in a s3
# bucket, and then dropping the null values. The output is saved in output.json, and
# expected_result.json contains the expected output of applying the function to the
# input data.
s3_input = td.S3Source(
    "s3://tabsdata-testing-bucket/testing_nested_import/data.parquet", s3_credentials
)
s3_input.uri = "s3://tabsdata-testing-bucket/testing_nested_import/data.csv"


@td.publisher(
    name="test_input_s3_modified_uri",
    source=s3_input,
    tables="output",
)
def input_s3_modified_uri(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_s3_modified_uri,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
