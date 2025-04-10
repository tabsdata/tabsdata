#
# Copyright 2025 Tabs Data Inc.
#


import tabsdata as td
from tabsdata import DestinationPlugin


class CustomDestinationPlugin(DestinationPlugin):

    def __init__(self, destination_json_file: str):
        self.destination_ndjson_file = destination_json_file

    def trigger_output(self, _: str, df: td.TableFrame):
        if df is not None:
            raise ValueError("df should be None")
