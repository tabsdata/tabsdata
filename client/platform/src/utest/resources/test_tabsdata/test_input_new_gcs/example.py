#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str,
    gcs_credentials=None,
    format=None,
    path=None,
    initial_last_modified=None,
    file_info=None,
    infer_schema_length=None,
):
    @td.publisher(
        name="test_input_new_gcs",
        source=td.GCSSrc(
            conn=td.GCSConn(
                bucket_uri=bucket_uri,
                credentials=gcs_credentials,
            ),
            path=path,
            format=format,
            initial_last_modified=initial_last_modified,
            file_info=file_info,
            infer_schema_length=infer_schema_length,
        ),
        tables="output",
    )
    def input_new_gcs(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return input_new_gcs
