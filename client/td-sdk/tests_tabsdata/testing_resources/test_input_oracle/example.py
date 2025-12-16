#
# Copyright 2024 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
):
    data = [
        "select * from INVOICE_HEADER where id > 0",
        "select * from INVOICE_ITEM where id > 0",
    ]

    @td.publisher(
        name="test_input_oracle",
        source=td.OracleSource(uri=uri, query=data, credentials=credentials),
        tables=["output1", "output2"],  # required,
    )
    def input_oracle(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        # transformations can be done here
        return headers, items

    return input_oracle
