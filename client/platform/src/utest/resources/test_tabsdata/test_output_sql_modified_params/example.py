#
# Copyright 2024 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    sql_output = td.MySQLDestination(
        "mysql://wrongip:3306/testing",
        ["wrong_table", "second_second_wrong_table"],
        credentials=td.UserPasswordCredentials("wronguser", "wrongpassword"),
    )

    sql_output.uri = uri
    sql_output.destination_table = destination_table
    sql_output.credentials = credentials

    @td.subscriber(
        name="output_sql_modified_params",
        tables="collection/table",
        destination=sql_output,
    )
    def output_sql_modified_params(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_sql_modified_params
