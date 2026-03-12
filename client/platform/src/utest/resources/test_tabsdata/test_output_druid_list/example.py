#
# Copyright 2026 Tabs Data Inc.
#

import polars as pl

import tabsdata as td


def testing_tabsdata_function(conn, destination_table=None, if_table_exists="append"):
    @td.subscriber(
        name="output_druid_list",
        tables="collection/table",
        destination=td.DruidDest(
            conn=conn,
            destination_tables=destination_table,
            if_table_exists=if_table_exists,
        ),
    )
    def output_druid_list(df: td.TableFrame):
        cleaned = df.drop_nulls()
        high_cal = cleaned.filter(pl.col("Calories") > 400)
        return cleaned, high_cal

    return output_druid_list
