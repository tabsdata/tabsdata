#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import contextlib
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


@contextlib.contextmanager
def spooled_parquet(frame: pl.LazyFrame, name: str) -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as work:
        path = Path(work) / name
        frame.sink_parquet(path)
        yield path
