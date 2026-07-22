#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str,
    credentials=None,
    region=None,
    format=None,
    path=None,
    initial_last_modified=None,
    file_info=None,
):
    @td.publisher(
        name="test_input_new_s3_compressed",
        source=td.S3Src(
            conn=td.S3Conn(
                bucket_uri=bucket_uri,
                credentials=credentials,
                region=region,
            ),
            path=path,
            format=format,
            initial_last_modified=initial_last_modified,
            file_info=file_info,
        ),
        tables="output",
    )
    def input_new_s3_compressed(df: list[td.TableFrame]):
        df = df[0]
        new_df = df.drop_nulls()
        return new_df

    return input_new_s3_compressed
