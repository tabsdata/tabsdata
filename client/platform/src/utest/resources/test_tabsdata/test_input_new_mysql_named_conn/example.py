#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td

data = [
    "select * from INVOICE_HEADER where id > 0",
    "select * from INVOICE_ITEM where id > 0",
]


def testing_tabsdata_function(conn_name: str):
    @td.publisher(
        name="test_input_new_mysql_named_conn",
        source=td.MySQLSrc(
            conn=td.NamedConn(name=conn_name),
            queries=data,
        ),
        tables=["output1", "output2"],
    )
    def input_new_mysql_named_conn(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        return headers, items

    return input_new_mysql_named_conn
