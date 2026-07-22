#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_table=None,
    chunk_size: int = 100000,
):
    @td.subscriber(
        name="output_new_postgres_list",
        tables="collection/table",
        destination=td.PostgresDest(
            td.PostgresConn(uri, credentials),
            destination_tables=destination_table,
            chunk_size=chunk_size,
        ),
    )
    def output_new_postgres_list(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_new_postgres_list
