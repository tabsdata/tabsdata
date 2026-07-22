#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, path: str):
    @td.publisher(
        name="test_input_new_azure_named_conn",
        source=td.AzureSrc(
            conn=td.NamedConn(name=conn_name),
            path=path,
        ),
        tables="output",
    )
    def input_new_azure_named_conn(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return input_new_azure_named_conn
