#
# Copyright 2024 Tabs Data Inc.
#

import os
from typing import List

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


# In this example, we are obtaining the data from all files that match the
# source_*.csv wildcard in a s3 bucket, and then performing an inner join by ID. The
# output is saved in output.json, and expected_result.json contains the expected output
# of applying the function to the  input data.
@td.publisher(
    name="input_s3_wildcard",
    source=td.S3Source(
        "s3://tabsdata-testing-bucket/wildcard_testing/source_*.csv", s3_credentials
    ),
    tables="output",
)
def input_s3_wildcard(dataframes: List[td.TableFrame]):
    if len(dataframes) != 2:
        raise ValueError
    else:
        df1 = dataframes[0]
        df2 = dataframes[1]
        return df1.join(df2, on="ID", how="inner")


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_s3_wildcard,
        local_packages=ROOT_PROJECT_DIR,
        save_location=DEFAULT_SAVE_LOCATION,
    )
