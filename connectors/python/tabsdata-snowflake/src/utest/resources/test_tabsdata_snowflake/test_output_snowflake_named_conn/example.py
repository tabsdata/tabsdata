#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, table_name: str):
    @td.subscriber(
        name="output_snowflake_named_conn",
        tables="collection/table",
        destination=td.SnowflakeDest(
            conn=td.NamedConn(name=conn_name),
            destination_table=table_name,
        ),
    )
    def output_snowflake_named_conn(df: td.TableFrame) -> td.TableFrame:
        return df.drop_nulls()

    return output_snowflake_named_conn
