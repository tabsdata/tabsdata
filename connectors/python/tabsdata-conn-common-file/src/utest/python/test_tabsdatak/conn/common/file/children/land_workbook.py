#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys

import polars as pl

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._excel import Excel

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore

BATCH_ROWS = 100


def main(folder: str, name: str, target: str) -> None:
    store = FileStore(folder)
    reader = Excel(batch_rows=BATCH_ROWS)
    reader.read(lambda: store.ranged(name)).frame.sink_parquet(target)
    print(pl.scan_parquet(target).select(pl.len()).collect().item())


if __name__ == "__main__":
    main(*sys.argv[1:])
