#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, path: str):
    @td.publisher(
        name="test_input_new_s3_named_conn",
        source=td.S3Src(
            conn=td.NamedConn(name=conn_name),
            path=path,
        ),
        tables="output",
    )
    def input_new_s3_named_conn(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return input_new_s3_named_conn
