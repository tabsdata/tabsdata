#
# Copyright 2024 Tabs Data Inc.
#

import os

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

s3_credentials = td.S3AccessKeyCredentials(
    os.environ.get("TRANSPORTER_AWS_ACCESS_KEY_ID", "FAKE_ID"),
    os.environ.get("TRANSPORTER_AWS_SECRET_ACCESS_KEY", "FAKE_KEY"),
)


# In this example, we are obtaining the data from the file data.csv in a s3
# bucket in eu-north-1 region, and then dropping the null values. The output is saved
# in a table, and expected_result.json contains the expected output of applying the
# function to the input data.
@td.publisher(
    name="test_input_s3_eu_north_region",
    source=td.S3Source(
        "s3://tabsdata-tucu-test/importer/input.csv",
        s3_credentials,
        region="eu-north-1",
    ),
    tables="output",
)
def input_s3_eu_north_region(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_s3_eu_north_region,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
