#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, query: str, warehouse: str):
    @td.publisher(
        name="input_databricks_named_conn",
        source=td.DatabricksSrc(
            conn=td.NamedConn(name=conn_name),
            queries=query,
            warehouse=warehouse,
        ),
        tables="output",
    )
    def input_databricks_named_conn(df: td.TableFrame) -> td.TableFrame:
        return df

    return input_databricks_named_conn
