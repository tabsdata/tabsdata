#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
):
    data = [
        "select * from INVOICE_HEADER where id > :number",
        "select * from INVOICE_ITEM where id > :number",
    ]

    @td.publisher(
        name="test_input_db2_initial_values",
        source=td.Db2Src(
            conn=td.Db2Conn(uri=uri, credentials=credentials),
            queries=data,
            initial_values={"number": 2},
        ),
        tables=["output1", "output2"],  # required,
    )
    def input_db2_initial_values(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame, dict):
        # transformations can be done here
        new_initial_values = {"number": 3}
        return headers, items, new_initial_values

    return input_db2_initial_values
