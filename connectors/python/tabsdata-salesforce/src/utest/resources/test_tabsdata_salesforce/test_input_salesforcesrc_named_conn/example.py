#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, query: str):
    @td.publisher(
        name="input_salesforcesrc_named_conn",
        source=td.SalesforceSrc(
            conn=td.NamedConn(name=conn_name),
            query=query,
        ),
        tables="output",
    )
    def input_salesforcesrc_named_conn(df: td.TableFrame) -> td.TableFrame:
        return df.drop_nulls()

    return input_salesforcesrc_named_conn
