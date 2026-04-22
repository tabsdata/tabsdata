#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    connection_string: str,
    credentials: None,
    destination_tables: list[str],
    loader="polars_sqlalchemy",
    chunk_size=100000,
    transactional=True,
    if_table_exists="append",
):
    @td.subscriber(
        name="output_mssql",
        tables="collection/table",
        destination=td.MSSQLDest(
            td.MSSQLConn(
                connection_string_or_uri=connection_string,
                credentials=credentials,
            ),
            destination_tables=destination_tables,
            if_table_exists=if_table_exists,
            transactional=transactional,
            chunk_size=chunk_size,
            loader=loader,
        ),
    )
    def output_new_mssql(df: td.TableFrame) -> td.TableFrame:
        new_df = df.drop_nulls()
        return new_df

    return output_new_mssql
