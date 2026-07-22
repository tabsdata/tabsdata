#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, destination_table=None):
    @td.subscriber(
        name="output_new_mysql_named_conn",
        tables="collection/table",
        destination=td.MySQLDest(
            conn=td.NamedConn(name=conn_name),
            destination_tables=destination_table,
        ),
    )
    def output_mysql_named_conn(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_mysql_named_conn
