#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str,
    path: str,
    credentials=None,
    format=None,
    region=None,
    catalog=None,
):
    @td.subscriber(
        tables="collection/table",
        name="output_new_s3",
        destination=td.S3Dest(
            conn=td.S3Conn(
                bucket_uri=bucket_uri,
                credentials=credentials,
                region=region,
            ),
            path=path,
            format=format,
            catalog=catalog,
        ),
    )
    def output_new_s3(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_s3
