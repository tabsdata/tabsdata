#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    uri = uri.replace("postgres://", "postgres+psycopg2://")

    @td.subscriber(
        name="output_new_postgres_driver_provided",
        tables="collection/table",
        destination=td.PostgresDest(
            conn=td.PostgresConn(uri, credentials), destination_tables=destination_table
        ),
    )
    def output_new_postgres_driver_provided(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_postgres_driver_provided
