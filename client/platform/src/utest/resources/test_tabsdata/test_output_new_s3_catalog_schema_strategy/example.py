#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str,
    path: str,
    credentials=None,
    region: str = None,
    catalog=None,
    format=None,
):
    @td.subscriber(
        name="output_new_s3_catalog_schema_strategy",
        tables="collection/table",
        destination=td.S3Dest(
            conn=td.S3Conn(
                bucket_uri=bucket_uri,
                credentials=credentials,
                region=region,
            ),
            path=path,
            catalog=catalog,
            format=format,
        ),
    )
    def output_new_s3_catalog_schema_strategy(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_s3_catalog_schema_strategy
