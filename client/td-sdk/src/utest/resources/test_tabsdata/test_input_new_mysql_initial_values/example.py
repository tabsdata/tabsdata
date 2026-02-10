#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

data = [
    "select * from INVOICE_HEADER where id > :number",
    "select * from INVOICE_ITEM where id > :number",
]


def testing_tabsdata_function(
    uri: str, user: str, password: str, loader="polars_sqlalchemy"
):
    @td.publisher(
        name="test_input_new_mysql_initial_values",
        source=td.MySQLSrc(
            conn=td.MySQLConn(
                uri=uri,
                credentials=td.UserPasswordCredentials(user, password),
            ),
            queries=data,
            loader=loader,
            initial_values={"number": 2},
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_new_mysql_initial_values(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame, dict):
        # transformations can be done here
        new_initial_values = {"number": 3}
        return headers, items, new_initial_values

    return input_new_mysql_initial_values
