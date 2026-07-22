#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_tables=None,
):
    @td.subscriber(
        name="output_mariadb_with_charset",
        tables="collection/table",
        destination=td.MariaDBDest(
            td.MariaDBConn(uri + "?charset=utf8mb4", credentials=credentials),
            destination_tables=destination_tables,
        ),
    )
    def output_new_mariadb_with_charset(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_mariadb_with_charset
