#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    conn_name: str,
    table_name: str,
    volume: str,
    warehouse: str,
    catalog: str,
    schema: str,
):
    @td.subscriber(
        name="output_databricks_named_conn",
        tables="collection/table",
        destination=td.DatabricksDest(
            conn=td.NamedConn(name=conn_name),
            tables=table_name,
            volume=volume,
            warehouse=warehouse,
            catalog=catalog,
            schema=schema,
        ),
    )
    def output_databricks_named_conn(df: td.TableFrame) -> td.TableFrame:
        return df.drop_nulls()

    return output_databricks_named_conn
