#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

data = [
    "select * from INVOICE_HEADER where id > 0",
    "select * from INVOICE_ITEM where id > 0",
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
        name="test_input_new_mysql",
        source=td.MySQLSrc(
            conn=td.MySQLConn(
                uri=uri,
                credentials=td.UserPasswordCredentials(user=user, password=password),
            ),
            queries=data,
            loader=loader,
            transactional=transactional,
            chunk_size=chunk_size,
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_new_mysql(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        # transformations can be done here
        return headers, items

    return input_new_mysql
