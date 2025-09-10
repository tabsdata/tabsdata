#
# Copyright 2025 Tabs Data Inc.
#

import os

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

gcs_credentials = td.GCPServiceAccountKeyCredentials(
    os.environ.get("S30__GCP_SERVICE_ACCOUNT_KEY", "FAKE_KEY"),
)


# In this example, we are obtaining the data from the file data.log in Azure
# and then dropping the null values. The output is a table, and
# expected_result.json contains the expected output of applying the function to the
# input data.
@td.publisher(
    td.GCSSource("gs://BUCKET_NAME/testing_resources/data.log", gcs_credentials),
    "output",
)
def input_gcs_log(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_gcs_log,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
