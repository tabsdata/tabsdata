#
# Copyright 2024 Tabs Data Inc.
#

import os

import polars as pl

import tabsdata as td


class Importer(td.SourcePlugin):
    def __init__(self, folder: str, file: str):
        self.folder = folder
        self.file = file

    def chunk(self, working_dir: str) -> str:
        destination_file = "data.parquet"
        destination_path = os.path.join(working_dir, destination_file)
        pl.scan_csv(f"{self.folder}/{self.file}").sink_parquet(destination_path)
        return destination_file
