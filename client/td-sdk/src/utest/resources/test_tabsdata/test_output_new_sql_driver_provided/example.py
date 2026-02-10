#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    uri = uri.replace("mysql://", "mysql+mysqlconnector://")

    @td.subscriber(
        name="output_new_sql_driver_provided",
        tables="collection/table",
        destination=td.MySQLDest(
            td.MySQLConn(uri, credentials),
            destination_table,
        ),
    )
    def output_new_sql_driver_provided(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_sql_driver_provided
