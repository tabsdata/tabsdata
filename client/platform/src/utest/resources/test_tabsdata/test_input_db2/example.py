#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    transactional=False,
    chunk_size=100000,
    schema_overrides=None,
    loader="polars_sqlalchemy"
):
    data = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]

    @td.publisher(
        name="test_input_db2",
        source=td.Db2Src(
            conn=td.Db2Conn(uri=uri, credentials=credentials),
            queries=data,
            transactional=transactional,
            chunk_size=chunk_size,
            loader=loader,
            schema_overrides=schema_overrides,
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_db2(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        # transformations can be done here
        return headers, items

    return input_db2
