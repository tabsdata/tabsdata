#
# Copyright 2024 Tabs Data Inc.
#

import os
from typing import List

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


# In this example, we are obtaining the data from a csv file in a s3 bucket that was
# last modified after a specific time, to ensure the correct one is picked by the
# importer. We are then dropping the null values. The output is saved in output.json,
# and expected_result.json contains the expected output of applying the function to the
# input data.
@td.publisher(
    name="input_s3_select_datetime",
    source=td.S3Source(
        "s3://tabsdata-testing-bucket/testing_nested_import/*.csv",
        s3_credentials,
        initial_last_modified="2024-09-05T01:01:00.01",
    ),
    tables="output",
)
def input_s3_select_datetime(df: List[td.TableFrame]):
    len_df = len(df)
    if len_df != 1:
        # Note: this exception is raised for the sake of testing, in a real
        # environment, it is perfectly plausible and acceptable to receive 0 files in
        # an incremental import, and that would not cause an error.
        raise ValueError(
            f"Expected exactly one file to be imported, {len_df} found instead."
        )
    df = df[0]
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_s3_select_datetime,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
