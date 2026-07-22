#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_tables=None):
    uri = uri.replace("oracle://", "oracle+oracledb://")

    @td.subscriber(
        "collection/table",
        td.OracleDest(
            td.OracleConn(uri, credentials=credentials),
            destination_tables=destination_tables,
            if_table_exists="append",
        ),
    )
    def output_new_oracle_driver_provided(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_oracle_driver_provided
