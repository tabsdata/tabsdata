#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

data = [
    "select * from all_oracle_types",
]


def testing_tabsdata_function(
    uri: str,
    credentials=None,
    loader="polars_sqlalchemy",
    transactional=True,
    chunk_size=10000,
    schema_overrides=None,
):
    @td.publisher(
        name="test_input_new_oracle_types_check",
        source=td.OracleSrc(
            conn=td.OracleConn(
                uri=uri,
                credentials=credentials,
            ),
            queries=data,
            loader=loader,
            transactional=transactional,
            chunk_size=chunk_size,
            schema_overrides=schema_overrides,
        ),
        tables=["output1"],  # required,
    )
    def input_new_oracle_types_check(headers: td.TableFrame) -> td.TableFrame:
        # transformations can be done here
        return headers

    return input_new_oracle_types_check
