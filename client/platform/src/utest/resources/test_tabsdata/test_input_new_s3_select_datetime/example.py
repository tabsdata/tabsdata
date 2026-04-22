#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td
import polars as pl
from tabsdata.extensions._tableframe.extension import SystemColumns
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder


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
        name="input_new_s3_select_datetime",
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
    def input_new_s3_select_datetime(df: list[td.TableFrame]):
        len_df = len(df)
        if len_df == 1:
            df = df[0]
            new_df = df.drop_nulls()
            return new_df
        elif len_df == 0:
            # Note: do NOT do this in production code, this is just for testing purposes.
            return td.TableFrame.__build__(
                df=pl.LazyFrame(
                    {"answer": 42, SystemColumns.TD_IDENTIFIER.value: "fake_id"}
                ),
                mode="raw",
                idx=0,
                properties=TableFramePropertiesBuilder.empty(),
            )
        else:
            # Note: this exception is raised for the sake of testing, in a real
            # environment, it is perfectly plausible and acceptable to receive multiple
            # files in an incremental import, and that would not cause an error.
            raise ValueError(
                f"Expected exactly one file or none to be imported, {len_df} found instead."
            )

    return input_new_s3_select_datetime
