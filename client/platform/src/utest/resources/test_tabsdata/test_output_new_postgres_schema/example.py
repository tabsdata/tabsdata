#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_table=None,
):
    @td.subscriber(
        name="output_new_postgres_schema",
        tables="collection/table",
        destination=td.PostgresDest(
            td.PostgresConn(uri, credentials),
            destination_tables=destination_table,
            if_table_exists="replace",
        ),
    )
    def output_new_postgres_schema(df: td.TableFrame):
        return df

    return output_new_postgres_schema
