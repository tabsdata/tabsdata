#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_tables=None):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    @td.subscriber(
        "collection/table",
        td.OracleDestination(
            uri,
            destination_tables,
            credentials=credentials,
            if_table_exists="append",
        ),
    )
    def output_oracle_transaction(df: td.TableFrame):
        new_df = df.drop_nulls()
        # Storing the next dataframe will fail. That is intended, as the aim of this test is
        # to verify transactional behavior, specifically that the first dataframe is NOT
        # stored in the database since there is a transaction rollback.
        incorrect_df_to_store = td.TableFrame({"a": [[1], [2], [3]], "b": [4, 5, 6]})
        return new_df, incorrect_df_to_store

    return output_oracle_transaction
