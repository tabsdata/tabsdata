#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_tables=None,
    chunk_size=10000,
    if_table_exists="append",
):
    @td.subscriber(
        "collection/table",
        td.OracleDest(
            td.OracleConn(uri, credentials=credentials),
            destination_tables=destination_tables,
            if_table_exists=if_table_exists,
            chunk_size=chunk_size,
        ),
    )
    def output_new_oracle_list(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_new_oracle_list
