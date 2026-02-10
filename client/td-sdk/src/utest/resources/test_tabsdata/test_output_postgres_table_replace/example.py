#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_table=None,
):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    @td.subscriber(
        name="output_postgres_table_replace",
        tables="collection/table",
        destination=td.PostgresDestination(
            uri,
            destination_table,
            credentials=credentials,
            if_table_exists="replace",
        ),
    )
    def output_postgres_table_replace(df: td.TableFrame):
        return df

    return output_postgres_table_replace
