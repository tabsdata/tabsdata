#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_tables=None,
    chunk_size: int = 100000,
):
    @td.subscriber(
        "collection/table",
        td.MariaDBDest(
            td.MariaDBConn(uri, credentials=credentials),
            destination_tables=destination_tables,
            chunk_size=chunk_size,
        ),
    )
    def output_new_mariadb_list(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_new_mariadb_list
