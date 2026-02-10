#
# Copyright 2024 Tabs Data Inc.
#

import tabsdata as td

# In this example, we are obtaining the data from two tables in the database.
# INVOICE_HEADER contains the following data:
# [{"id":1,"name":"Arvind"},{"id":2,"name":"Tucu"},{"id":3,"name":"Dimas"},
# {"id":4,"name":"Joaquin"},{"id":5,"name":"Jennifer"},{"id":6,"name":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"id":1,"name":"Leonardo"},{"id":2,"name":"Donatello"},{"id":3,
# "name":"Michelangelo"},{"id":4,"name":"Raphael"},{"id":5,"name":"Splinter"}]


def testing_tabsdata_function(uri: str, credentials=None, initial_values: dict = None):
    data = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]

    @td.publisher(
        name="test_input_sql_initial_values",
        source=td.MySQLSource(
            uri,
            data,
            credentials=credentials,
            initial_values=initial_values,
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_sql_initial_values(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame, dict):
        # transformations can be done here
        new_initial_values = {"number": 3}
        return headers, items, new_initial_values

    return input_sql_initial_values
