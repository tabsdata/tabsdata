#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td

data = [
    "select * from all_postgres_types",
]


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
        name="test_input_new_postgres_types_check",
        source=td.PostgresSrc(
            conn=td.PostgresConn(
                uri=uri,
                credentials=td.UserPasswordCredentials(user=user, password=password),
                cx_src_configs_postgres={
                    "options": "-csearch_path=public,type_check_test_schema"
                },
            ),
            queries=data,
            loader=loader,
            transactional=transactional,
            chunk_size=chunk_size,
            schema_overrides=schema_overrides,
        ),
        tables=["output1"],  # required,
    )
    def input_new_postgres_types_check(headers: td.TableFrame) -> td.TableFrame:
        # transformations can be done here
        return headers

    return input_new_postgres_types_check
