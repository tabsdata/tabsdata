#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

data = [
    "select * from INVOICE_HEADER where id > :number",
    "select * from INVOICE_ITEM where id > :number",
]

mysql_input = td.MySQLSrc(
    conn=td.MySQLConn(
        uri="mysql://wrongip:3306/testing",
        credentials=td.UserPasswordCredentials("wronguser", "wrongpassword"),
    ),
    queries=["select * from NO_TABLE where id > :number"],
    initial_values={"number": 7},
)


def testing_tabsdata_function(uri: str, user: str, password: str):
    mysql_input.conn.uri = uri
    mysql_input.queries = data
    mysql_input.conn.credentials = td.UserPasswordCredentials(user, password)
    mysql_input.initial_values = {"number": 2}

    @td.publisher(
        name="test_input_new_mysql_modified_params",
        source=mysql_input,
        tables=["output1", "output2"],  # required,
    )
    def input_new_mysql_modified_params(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame, dict):
        # transformations can be done here
        new_initial_values = {"number": "3"}
        return headers, items, new_initial_values

    return input_new_mysql_modified_params
