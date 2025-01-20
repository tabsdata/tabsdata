#
# Copyright 2024 Tabs Data Inc.
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
DEFAULT_SAVE_LOCATION = os.path.join(ROOT_PROJECT_DIR, "local_dev")

azure_credentials = td.AzureAccountKeyCredentials(
    os.environ.get("TRANSPORTER_AZURE_ACCOUNT_NAME", "FAKE_ID"),
    os.environ.get("TRANSPORTER_AZURE_ACCOUNT_KEY", "FAKE_KEY"),
)


# In this example, we are obtaining the data from the file data.log in Azure
# and then dropping the null values. The output is a table, and
# expected_result.json contains the expected output of applying the function to the
# input data.
@td.publisher(
    td.AzureSource("az://tabsdataci/testing_resources/data.log", azure_credentials),
    "output",
)
def input_azure_log(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_azure_log,
        local_packages=ROOT_PROJECT_DIR,
        save_location=DEFAULT_SAVE_LOCATION,
    )
