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

# In this example, we are obtaining the data from the file data.csv and then dropping
# the null values. The output is saved in output.json, and expected_result.json
# contains the expected output of applying the function to the input data.
sql_output = td.MySQLDestination(
    "mysql://wrongip:3306/testing",
    ["wrong_table", "second_second_wrong_table"],
    credentials=td.UserPasswordCredentials("wronguser", "wrongpassword"),
)

sql_output.uri = "mysql://127.0.0.1:3306/testing"
sql_output.destination_table = [
    "output_sql_modified_params",
    "second_output_sql_modified_params",
]
sql_output.credentials = td.UserPasswordCredentials("@dmIn", "p@ssw0rd#")


@td.subscriber(
    name="output_sql_modified_params",
    tables="collection/table",
    destination=sql_output,
)
def output_sql_modified_params(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df, new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_sql_modified_params,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
