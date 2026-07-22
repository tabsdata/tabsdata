#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td

data = [
    "select * from all_db2_types",
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
        name="test_input_db2_types_check",
        source=td.Db2Src(
            conn=td.Db2Conn(
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
    def input_db2_types_check(headers: td.TableFrame) -> td.TableFrame:
        # transformations can be done here
        return headers

    return input_db2_types_check
