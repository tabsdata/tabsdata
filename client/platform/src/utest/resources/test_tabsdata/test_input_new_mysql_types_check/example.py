#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

data = ["select * from ALL_MYSQL_TYPES"]


def testing_tabsdata_function(
    uri: str,
    user: str,
    password: str,
    loader="polars_sqlalchemy",
    transactional=True,
    chunk_size=10000,
    schema_overrides=None,
):
    @td.publisher(
        name="test_input_new_mysql_types_check",
        source=td.MySQLSrc(
            conn=td.MySQLConn(
                uri=uri,
                credentials=td.UserPasswordCredentials(user=user, password=password),
                cx_src_configs_mysql={"connect_timeout": 10},
            ),
            queries=data,
            loader=loader,
            transactional=transactional,
            chunk_size=chunk_size,
            schema_overrides=schema_overrides,
        ),
        tables=["output1"],  # required,
    )
    def input_new_mysql_types_check(all_mysql_types: td.TableFrame) -> (td.TableFrame,):
        # transformations can be done here
        return all_mysql_types

    return input_new_mysql_types_check
