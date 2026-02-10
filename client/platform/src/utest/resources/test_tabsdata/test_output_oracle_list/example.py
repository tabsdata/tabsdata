#
# Copyright 2024 Tabs Data Inc.
#

import tabsdata as td


# In this example, we are obtaining the data from the file data.csv and then dropping
# the null values. The output is saved in output.json, and expected_result.json
# contains the expected output of applying the function to the input data.
def testing_tabsdata_function(uri: str, credentials=None, destination_tables=None):
    @td.subscriber(
        "collection/table",
        td.OracleDestination(
            uri,
            destination_tables,
            credentials=credentials,
            if_table_exists="append",
        ),
    )
    def output_oracle_list(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_oracle_list
