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
        if lf is not None:
            raise ValueError("df should be None")
        if lf2 is not None:
            raise ValueError("df2 should be None")
