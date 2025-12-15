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


def testing_tabsdata_function(
    uri: str,
    credentials=None,
):
    data = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]

    @td.publisher(
        name="test_input_postgres",
        source=td.PostgresSource(
            uri=uri,
            query=data,
            credentials=credentials,
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_postgres(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        # transformations can be done here
        return headers, items

    return input_postgres
