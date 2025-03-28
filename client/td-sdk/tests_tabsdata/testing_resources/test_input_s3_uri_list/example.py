#
# Copyright 2024 Tabs Data Inc.
#

import os
from typing import List

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
# bucket, and then dropping the null values. The output is saved in output.json, and
# expected_result.json contains the expected output of applying the function to the
# input data.
@td.publisher(
    name="input_s3_uri_list",
    source=td.S3Source(
        [
            "s3://tabsdata-testing-bucket/testing_nested_import/data.csv",
            "s3://tabsdata-testing-bucket/wildcard_testing/source_*.csv",
        ],
        s3_credentials,
    ),
    tables=["output1", "output2"],  # required,
)
def input_s3_uri_list(df: td.TableFrame, dataframes: List[td.TableFrame]):
    new_df = df.drop_nulls()
    if len(dataframes) != 2:
        raise ValueError
    else:
        df1 = dataframes[0]
        df2 = dataframes[1]
    return new_df, df1.join(df2, on="ID", how="inner")


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_s3_uri_list,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
