#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    uri = uri.replace("mysql://", "mysql+wrongdriver://")

    @td.subscriber(
        name="output_new_sql_wrong_driver_fails",
        tables="collection/table",
        destination=td.MySQLDest(
            conn=td.MySQLConn(uri=uri, credentials=credentials),
            destination_tables=destination_table,
        ),
    )
    def output_new_sql_wrong_driver_fails(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_new_sql_wrong_driver_fails
