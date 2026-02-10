#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td

# In this example, we are obtaining the data from two tables in the database.
# INVOICE_HEADER contains the following data:
# [{"id":1,"name":"Arvind"},{"id":2,"name":"Tucu"},{"id":3,"name":"Dimas"},
# {"id":4,"name":"Joaquin"},{"id":5,"name":"Jennifer"},{"id":6,"name":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"id":1,"name":"Leonardo"},{"id":2,"name":"Donatello"},{"id":3,
# "name":"Michelangelo"},{"id":4,"name":"Raphael"},{"id":5,"name":"Splinter"}]


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    transactional: bool = True,
    chunk_size: int = 100000,
    loader="polars_sqlalchemy",
    schema_overrides=None,
    cx_src_configs_mariadb=None,
    cx_dst_configs_mariadb=None,
):
    data = [
        "select * from ALL_MARIADB_TYPES",
    ]

    @td.publisher(
        name="test_input_new_mariadb_types_check",
        source=td.MariaDBSrc(
            conn=td.MariaDBConn(
                uri=uri,
                credentials=credentials,
                cx_src_configs_mariadb=cx_src_configs_mariadb,
                cx_dst_configs_mariadb=cx_dst_configs_mariadb,
            ),
            queries=data,
            transactional=transactional,
            chunk_size=chunk_size,
            loader=loader,
            schema_overrides=schema_overrides,
        ),
        tables=["output1"],  # required,
    )
    def input_new_mariadb_types_check(headers: td.TableFrame) -> td.TableFrame:
        # transformations can be done here
        return headers

    return input_new_mariadb_types_check
