#
# Copyright 2025 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    @td.subscriber(
        name="output_sql_list_none",
        tables="collection/table",
        destination=td.MySQLDestination(
            uri,
            destination_table,
            credentials=credentials,
        ),
    )
    def output_sql_list_none(df: td.TableFrame):
        df.drop_nulls()
        return None, None

    return output_sql_list_none
