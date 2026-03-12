#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None):
    @td.subscriber(
        name="output_new_clickhouse_none",
        tables="collection/table",
        destination=td.ClickHouseDest(
            conn=conn,
            destination_tables=destination_table,
        ),
    )
    def output_new_clickhouse_none(df: td.TableFrame):
        df.drop_nulls()
        return None

    return output_new_clickhouse_none
