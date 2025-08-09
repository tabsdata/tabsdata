#
# Copyright 2024 Tabs Data Inc.
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

# In this example, we are obtaining the data from two tables in the database.
# INVOICE_HEADER contains the following data:
# [{"id":1,"name":"Arvind"},{"id":2,"name":"Tucu"},{"id":3,"name":"Dimas"},
# {"id":4,"name":"Joaquin"},{"id":5,"name":"Jennifer"},{"id":6,"name":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"id":1,"name":"Leonardo"},{"id":2,"name":"Donatello"},{"id":3,
# "name":"Michelangelo"},{"id":4,"name":"Raphael"},{"id":5,"name":"Splinter"}]
data = [
    "select * from INVOICE_HEADER where id > 0",
    "select * from INVOICE_ITEM where id > 0",
]


@td.publisher(
    name="test_input_sql",
    source=td.MySQLSource(
        uri="mysql://127.0.0.1:3306/testing",
        query=data,
        credentials=td.UserPasswordCredentials("@dmIn", "p@ssw0rd#"),
    ),
    tables=["output1", "output2"],  # required,
)
def input_sql(
    headers: td.TableFrame, items: td.TableFrame
) -> (td.TableFrame, td.TableFrame):
    # transformations can be done here
    return headers, items


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_sql,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
