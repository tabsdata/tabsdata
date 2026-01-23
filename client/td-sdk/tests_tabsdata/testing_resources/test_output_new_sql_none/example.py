#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    @td.subscriber(
        name="output_new_sql_none",
        tables="collection/table",
        destination=td.MySQLDest(
            td.MySQLConn(uri, credentials),
            destination_table,
        ),
    )
    def output_new_sql_none(df: td.TableFrame):
        df.drop_nulls()
        return None

    return output_new_sql_none
