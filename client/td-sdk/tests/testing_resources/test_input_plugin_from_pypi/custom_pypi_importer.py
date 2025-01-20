#
# Copyright 2025 Tabs Data Inc.
#

import os

import polars as pl
import requests

import tabsdata as td


class PyPIPkgStatsSource(td.SourcePlugin):
    def __init__(self, package_name: str):
        self.package_name = package_name

    def trigger_input(self, working_dir: str) -> str:
        # Endpoint with the downloads information of the package
        base_endpoint = f"https://pypistats.org/api/packages/{self.package_name}"
        # Get the downloads by system
        downloads_by_system = requests.get(f"{base_endpoint}/system").json().get("data")

        # Store the information
        destination_file = "data.parquet"
        destination_path = os.path.join(working_dir, destination_file)
        pl.DataFrame(downloads_by_system).write_parquet(destination_path)
        return destination_file
