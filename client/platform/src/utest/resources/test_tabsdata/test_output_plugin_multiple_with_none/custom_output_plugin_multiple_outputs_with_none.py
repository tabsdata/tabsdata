#
# Copyright 2025 Tabs Data Inc.
#

from typing import List

import polars as pl

from tabsdata import DestinationPlugin


class CustomDestinationPlugin(DestinationPlugin):

    def __init__(self, destination_json_file: str, second_destination_json_file: str):
        self.destination_ndjson_file = destination_json_file
        self.second_destination_ndjson_file = second_destination_json_file

    def stream(
        self,
        working_dir: str,
        *results: List[pl.LazyFrame | None] | pl.LazyFrame | None
    ):
        lf0 = results[0]
        lf1 = results[1]
        if lf0 is not None:
            raise ValueError("df0 should be None")
        if lf1 is not None:
            raise ValueError("df1 should be None")
