#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl

from tabsdata import DestinationPlugin


class CustomDestinationPlugin(DestinationPlugin):

    def __init__(self, destination_json_file: str, second_destination_json_file: str):
        self.destination_ndjson_file = destination_json_file
        self.second_destination_ndjson_file = second_destination_json_file

    def stream(self, _: str, lf: pl.LazyFrame, lf2: pl.LazyFrame):
        lf.sink_ndjson(self.destination_ndjson_file)
        lf2.sink_ndjson(self.second_destination_ndjson_file)
