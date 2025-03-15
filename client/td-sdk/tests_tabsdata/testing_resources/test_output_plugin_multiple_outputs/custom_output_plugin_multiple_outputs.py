#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl

from tabsdata import DestinationPlugin


class CustomDestinationPlugin(DestinationPlugin):

    def __init__(self, destination_json_file: str, second_destination_json_file: str):
        self.destination_ndjson_file = destination_json_file
        self.second_destination_ndjson_file = second_destination_json_file

    def trigger_output(self, df: pl.LazyFrame, df2: pl.LazyFrame):
        df.sink_ndjson(self.destination_ndjson_file)
        df2.sink_ndjson(self.second_destination_ndjson_file)
