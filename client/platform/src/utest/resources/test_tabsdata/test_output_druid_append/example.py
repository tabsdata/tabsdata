#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None):
    @td.subscriber(
        name="output_druid_append",
        tables="collection/table",
        destination=td.DruidDest(
            conn=conn,
            destination_tables=destination_table,
        ),
    )
    def output_druid_append(df: td.TableFrame):
        return df.drop_nulls()

    return output_druid_append
