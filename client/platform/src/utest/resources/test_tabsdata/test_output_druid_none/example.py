#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None):
    @td.subscriber(
        name="output_druid_none",
        tables="collection/table",
        destination=td.DruidDest(
            conn=conn,
            destination_tables=destination_table,
        ),
    )
    def output_druid_none(df: td.TableFrame):
        df.drop_nulls()
        return None

    return output_druid_none
