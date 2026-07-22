#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    destination_table=None,
    transactional=True,
    chunk_size=1000,
):
    @td.subscriber(
        name="output_new_mariadb_list_types_check",
        tables="collection/table",
        destination=td.MariaDBDest(
            conn=td.MariaDBConn(uri=uri, credentials=credentials),
            destination_tables=destination_table,
            transactional=transactional,
            chunk_size=chunk_size,
        ),
    )
    def output_new_mariadb_list_types_check(df: td.TableFrame):
        return df

    return output_new_mariadb_list_types_check
