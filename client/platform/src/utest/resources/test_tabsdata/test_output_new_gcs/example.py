#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str,
    path: str,
    credentials=None,
    format=None,
):
    @td.subscriber(
        name="output_gcs",
        tables="collection/table",
        destination=td.GCSDest(
            conn=td.GCSConn(
                bucket_uri=bucket_uri,
                credentials=credentials,
            ),
            path=path,
            format=format,
        ),
    )
    def output_gcs(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_gcs
