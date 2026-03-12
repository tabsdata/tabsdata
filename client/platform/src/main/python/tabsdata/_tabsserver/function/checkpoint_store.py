#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from tabsdata._tabsserver.function.global_utils import uri_join
from tabsdata._tabsserver.function.native_tables_utils import (
    scan_lf_from_location,
    sink_lf_to_location,
)

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.yaml_parsing import Location

CHECKPOINT_FILE_NAME = "TD_INTERNAL_CHECKPOINT_FILE.parquet"


class CheckpointStore:
    def __init__(
        self,
        location: Location,
        execution_context: ExecutionContext,
    ):
        location.uri = uri_join(location.uri, CHECKPOINT_FILE_NAME)
        self._location = location
        self._execution_context = execution_context
        self._df = None

    def read(self) -> pl.DataFrame:
        if self._df is None:
            # It is either the first execution of the stage trigger or there is a
            # persisted checkpoint to load
            lf = scan_lf_from_location(self._execution_context, self._location)
            try:
                df = lf.collect()
            except FileNotFoundError:
                # No file exists yet, meaning this is the first execution of the
                # stage trigger
                df = pl.DataFrame({})
            self._df = df
        return self._df

    def write(self, checkpoint: pl.DataFrame):
        self._df = checkpoint
        sink_lf_to_location(
            pl.LazyFrame(checkpoint), self._execution_context, self._location
        )
