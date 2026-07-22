#
# Copyright 2024 Tabsdata Inc.
#

import tabsdata as td

# In this example, we are obtaining the data from two tables in the database.
# INVOICE_HEADER contains the following data:
# [{"id":1,"name":"Arvind"},{"id":2,"name":"Tucu"},{"id":3,"name":"Dimas"},
# {"id":4,"name":"Joaquin"},{"id":5,"name":"Jennifer"},{"id":6,"name":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"id":1,"name":"Leonardo"},{"id":2,"name":"Donatello"},{"id":3,
# "name":"Michelangelo"},{"id":4,"name":"Raphael"},{"id":5,"name":"Splinter"}]


def testing_tabsdata_function(
    uri: str,
    credentials=None,
):
    data = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]

    mysql_input = td.MySQLSource(
        "mysql://wrongip:3306/testing",
        ["select * from NO_TABLE where id > :number"],
        credentials=td.UserPasswordCredentials("wronguser", "wrongpassword"),
        initial_values={"number": 7},
    )

    mysql_input.uri = uri
    mysql_input.query = data
    mysql_input.credentials = credentials
    mysql_input.initial_values = {"number": 2}

    @td.publisher(
        name="test_input_sql_modified_params",
        source=mysql_input,
        tables=["output1", "output2"],  # required,
    )
    def input_sql_modified_params(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame, dict):
        # transformations can be done here
        new_initial_values = {"number": "3"}
        return headers, items, new_initial_values

    return input_sql_modified_params
