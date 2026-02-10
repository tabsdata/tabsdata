#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_tables=None,
):
    uri = uri.replace("mariadb://", "mariadb+mysqlconnector://")

    @td.subscriber(
        "collection/table",
        td.MariaDBDest(
            td.MariaDBConn(uri, credentials=credentials),
            destination_tables=destination_tables,
        ),
    )
    def output_new_mariadb_driver_provided(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_mariadb_driver_provided
