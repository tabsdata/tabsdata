#
# Copyright 2024 Tabs Data Inc.
#

import os

import polars as pl

import tabsdata as td


class ImporterWithInitialValues(td.SourcePlugin):
    def __init__(self, folder: str, file_number: int):
        self.folder = folder
        self.file_name_pattern = "source_"
        self.initial_values = {"number": file_number}

    def chunk(self, working_dir: str) -> str:
        destination_file = "data.parquet"
        destination_path = os.path.join(working_dir, destination_file)
        origin_file = os.path.join(
            self.folder,
            f"{self.file_name_pattern}{self.initial_values['number']}.csv",
        )
        pl.scan_csv(origin_file).sink_parquet(destination_path)
        self.initial_values["number"] += 1
        return destination_file
