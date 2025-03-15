#
# Copyright 2024 Tabs Data Inc.
#

import os
from typing import Tuple

import polars as pl

import tabsdata as td


class Importer(td.SourcePlugin):
    def __init__(self, folder: str, file: str):
        self.folder = folder
        self.file = file

    def trigger_input(self, working_dir: str) -> Tuple[str, str]:
        destination_file = "data.parquet"
        destination_path = os.path.join(working_dir, destination_file)
        pl.scan_csv(f"{self.folder}/{self.file}").sink_parquet(destination_path)

        second_destination_file = "data2.parquet"
        second_destination_path = os.path.join(working_dir, second_destination_file)
        pl.scan_csv(os.path.join(self.folder, "another_file.csv")).sink_parquet(
            second_destination_path
        )
        return destination_file, second_destination_file
