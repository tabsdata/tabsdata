#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None, if_table_exists="append"):
    @td.subscriber(
        name="output_new_clickhouse_list",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=destination_table,
            if_table_exists=if_table_exists,
        ),
    )
    def output_new_clickhouse_list(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_new_clickhouse_list
