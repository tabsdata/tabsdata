#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, path: str):
    @td.subscriber(
        name="output_new_azure_named_conn",
        tables="collection/table",
        destination=td.AzureDest(
            conn=td.NamedConn(name=conn_name),
            path=path,
        ),
    )
    def output_new_azure_named_conn(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_azure_named_conn
