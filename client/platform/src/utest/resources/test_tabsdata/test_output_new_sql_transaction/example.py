#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str, credentials=None, destination_table=None, transactional: bool = True
):
    @td.subscriber(
        name="output_new_sql_transaction",
        tables="collection/table",
        destination=td.MySQLDest(
            td.MySQLConn(uri, credentials),
            destination_table,
            transactional=transactional,
        ),
    )
    def output_new_sql_transaction(df: td.TableFrame):
        new_df = df.drop_nulls()
        # Storing the next dataframe will fail. That is intended, as the aim of this test is
        # to verify transactional behavior, specifically that the first dataframe is NOT
        # stored in the database since there is a transaction rollback.
        incorrect_df_to_store = td.TableFrame({"a": [[1], [2], [3]], "b": [4, 5, 6]})
        return new_df, incorrect_df_to_store

    return output_new_sql_transaction
