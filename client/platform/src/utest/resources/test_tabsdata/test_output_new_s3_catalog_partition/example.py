#
# Copyright 2026 Tabsdata Inc.
#

import numpy as np
import pandas as pd
import polars as pl

import tabsdata as td

NUMBER_OF_PARTITIONS = 10


def testing_tabsdata_function(
    bucket_uri: str,
    path: str,
    credentials=None,
    region: str = None,
    catalog=None,
    format=None,
):
    @td.subscriber(
        name="output_new_s3_catalog_partition",
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
    def output_new_s3_catalog_partition(df: td.TableFrame):
        date_range = pd.date_range(start="1900-01-01", end=pd.Timestamp.now(), freq="D")
        random_dates = np.random.choice(date_range, size=NUMBER_OF_PARTITIONS)
        random_numbers = np.random.rand(NUMBER_OF_PARTITIONS)
        polars_df = pl.DataFrame(
            {
                "timestamp": random_dates,
                "random_number": random_numbers,
            }
        )
        polars_df = polars_df.with_columns(polars_df["timestamp"].cast(pl.Datetime("us")))
        tfs = []
        for i in range(NUMBER_OF_PARTITIONS):
            tf = td.TableFrame(polars_df.slice(i, 1).to_dict())
            tfs.append(tf)
        return tfs

    return output_new_s3_catalog_partition
