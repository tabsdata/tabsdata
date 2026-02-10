#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td

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


def testing_tabsdata_function(
    uri: str,
    user: str,
    password: str,
    loader="polars_sqlalchemy",
    transactional=True,
    chunk_size=10000,
):
    @td.publisher(
        name="test_input_new_postgres_initial_values",
        source=td.PostgresSrc(
            conn=td.PostgresConn(
                uri=uri,
                credentials=td.UserPasswordCredentials(user, password),
            ),
            queries=data,
            initial_values={"number": 2},
            loader=loader,
            transactional=transactional,
            chunk_size=chunk_size,
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_new_postgres_initial_values(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame, dict):
        # transformations can be done here
        new_initial_values = {"number": 3}
        return headers, items, new_initial_values

    return input_new_postgres_initial_values
