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

# In this example, we are obtaining the data from two tables in the database.
# INVOICE_HEADER contains the following data:
# [{"ID":1,"NAME":"Arvind"},{"ID":2,"NAME":"Tucu"},{"ID":3,"NAME":"Dimas"},
# {"ID":4,"NAME":"Joaquin"},{"ID":5,"NAME":"Jennifer"},{"ID":6,"NAME":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"ID":1,"NAME":"Leonardo"},{"ID":2,"NAME":"Donatello"},{"ID":3,
# "NAME":"Michelangelo"},{"ID":4,"NAME":"Raphael"},{"ID":5,"NAME":"Splinter"}]
data = [
    "select * from INVOICE_HEADER where id > :number",
    "select * from INVOICE_ITEM where id > :number",
]


@td.publisher(
    name="test_input_oracle_initial_values",
    source=td.OracleSource(
        "oracle://127.0.0.1:1521/FREE",
        data,
        credentials=td.UserPasswordCredentials("system", "p@ssw0rd#"),
        initial_values={"number": "2"},
    ),
    tables=["output1", "output2"],  # required,
)
def input_oracle_initial_values(
    headers: td.TableFrame, items: td.TableFrame
) -> (td.TableFrame, td.TableFrame, dict):
    # transformations can be done here
    new_initial_values = {"number": "3"}
    return headers, items, new_initial_values


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_oracle_initial_values,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
