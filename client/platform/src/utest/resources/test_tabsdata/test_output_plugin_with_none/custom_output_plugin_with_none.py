#
# Copyright 2025 Tabs Data Inc.
#

from typing import List

import polars as pl

from tabsdata import DestinationPlugin


class CustomDestinationPlugin(DestinationPlugin):

    def __init__(self, destination_json_file: str):
        self.destination_ndjson_file = destination_json_file

    def stream(
        self,
        working_dir: str,
        *results: List[pl.LazyFrame | None] | pl.LazyFrame | None
    ):
        lf = results[0]
        if lf is not None:
            raise ValueError("df should be None")
