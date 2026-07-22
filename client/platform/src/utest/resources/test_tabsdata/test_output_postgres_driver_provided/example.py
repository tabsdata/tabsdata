#
# Copyright 2025 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    uri = uri.replace("postgres://", "postgres+psycopg2://")

    @td.subscriber(
        name="output_postgres_driver_provided",
        tables="collection/table",
        destination=td.PostgresDestination(
            uri, destination_table=destination_table, credentials=credentials
        ),
    )
    def output_postgres_driver_provided(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_postgres_driver_provided
