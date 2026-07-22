#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str, path: str, credentials=None, region: str = None, format=None
):
    @td.subscriber(
        tables="collection/table",
        name="output_new_s3_frame_list",
        destination=td.S3Dest(
            conn=td.S3Conn(
                bucket_uri=bucket_uri,
                credentials=credentials,
                region=region,
            ),
            path=path,
            format=format,
        ),
    )
    def output_new_s3_frame_list(df: td.TableFrame):
        new_df = df.drop_nulls()
        tf = td.TableFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        return [new_df, tf]

    return output_new_s3_frame_list
