#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    loader="polars_sqlalchemy",
    transactional=False,
    chunk_size=100000,
    schema_overrides=None,
):
    data = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]

    @td.publisher(
        name="test_input_new_oracle",
        source=td.OracleSrc(
            conn=td.OracleConn(uri=uri, credentials=credentials),
            queries=data,
            transactional=transactional,
            chunk_size=chunk_size,
            schema_overrides=schema_overrides,
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_new_oracle(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        # transformations can be done here
        return headers, items

    return input_new_oracle
