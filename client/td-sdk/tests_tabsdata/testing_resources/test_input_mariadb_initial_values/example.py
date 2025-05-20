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
# [{"id":1,"name":"Arvind"},{"id":2,"name":"Tucu"},{"id":3,"name":"Dimas"},
# {"id":4,"name":"Joaquin"},{"id":5,"name":"Jennifer"},{"id":6,"name":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"id":1,"name":"Leonardo"},{"id":2,"name":"Donatello"},{"id":3,
# "name":"Michelangelo"},{"id":4,"name":"Raphael"},{"id":5,"name":"Splinter"}]
data = [
    "select * from INVOICE_HEADER where id > :number",
    "select * from INVOICE_ITEM where id > :number",
]


@td.publisher(
    name="test_input_mariadb_initial_values",
    source=td.MariaDBSource(
        "mariadb://127.0.0.1:3307/testing",
        data,
        credentials=td.UserPasswordCredentials("@dmIn", "p@ssw0rd#"),
        initial_values={"number": 2},
    ),
    tables=["output1", "output2"],  # required,
)
def input_mariadb_initial_values(
    headers: td.TableFrame, items: td.TableFrame
) -> (td.TableFrame, td.TableFrame, dict):
    # transformations can be done here
    new_initial_values = {"number": 3}
    return headers, items, new_initial_values


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_mariadb_initial_values,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
