#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(conn_name: str, path: str):
    @td.subscriber(
        tables="collection/table",
        name="output_new_s3_named_conn",
        destination=td.S3Dest(
            conn=td.NamedConn(name=conn_name),
            path=path,
        ),
    )
    def output_new_s3_named_conn(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_s3_named_conn
