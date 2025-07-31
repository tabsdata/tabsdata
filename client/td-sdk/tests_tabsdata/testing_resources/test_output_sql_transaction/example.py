#
# Copyright 2025 Tabs Data Inc.
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


# In this example, we are obtaining the data from the file data.csv and then dropping
# the null values. The output is saved in output.json, and expected_result.json
# contains the expected output of applying the function to the input data.
@td.subscriber(
    name="output_sql_transaction",
    tables="collection/table",
    destination=td.MySQLDestination(
        "mysql://127.0.0.1:3306/testing",
        ["output_sql_transaction", "second_output_sql_transaction"],
        credentials=td.UserPasswordCredentials("@dmIn", "p@ssw0rd#"),
    ),
)
def output_sql_transaction(df: td.TableFrame):
    new_df = df.drop_nulls()
    # Storing the next dataframe will fail. That is intended, as the aim of this test is
    # to verify transactional behavior, specifically that the first dataframe is NOT
    # stored in the database since there is a transaction rollback.
    incorrect_df_to_store = td.TableFrame({"a": [[1], [2], [3]], "b": [4, 5, 6]})
    return new_df, incorrect_df_to_store


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_sql_transaction,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
