#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None):
    @td.subscriber(
        name="output_druid_time",
        tables="collection/table",
        destination=td.DruidDest(
            conn=conn,
            destination_tables=destination_table,
        ),
    )
    def output_druid_time(df: td.TableFrame):
        return df.drop_nulls()

    return output_druid_time
