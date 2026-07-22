#
# Copyright 2024 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_tables=None,
):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    @td.subscriber(
        name="output_mariadb_with_collation",
        tables="collection/table",
        destination=td.MariaDBDestination(
            uri + "?charset=utf8mb4&collation=utf8mb4_unicode_520_ci",
            destination_table=destination_tables,
            credentials=credentials,
        ),
    )
    def output_mariadb_with_collation(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_mariadb_with_collation
