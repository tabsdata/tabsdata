#
# Copyright 2024 Tabs Data Inc.
#

import polars as pl

from tabsdata import DestinationPlugin


class CustomDestinationPlugin(DestinationPlugin):

    def __init__(self, destination_json_file: str):
        self.destination_ndjson_file = destination_json_file

    def trigger_output(self, _: str, df: pl.LazyFrame):
        df.sink_ndjson(self.destination_ndjson_file)
