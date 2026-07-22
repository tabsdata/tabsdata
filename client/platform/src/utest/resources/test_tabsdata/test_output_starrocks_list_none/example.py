#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None):
    @td.subscriber(
        name="output_new_starrocks_list_none",
        tables="collection/table",
        destination=td.StarRocksDest(
            conn=conn,
            destination_tables=destination_table,
        ),
    )
    def output_new_starrocks_list_none(df: td.TableFrame):
        df.drop_nulls()
        return None, None

    return output_new_starrocks_list_none
