#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys

import polars as pl
from peak import peak_bytes

from tabsdatak.common._fileformat.taxonomy import SrcFileFormat

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _source

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._plugin import FilePluginErrors, ReaderConf

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.error import FileCommonErrorCode

ERROR = FileCommonErrorCode.FILE_COMMON_1


class Context:
    def get_attr(self, key: str) -> object:
        return None

    def set_attr(self, key: str, value: object) -> None:
        pass


def main(root: str, options: str, name: str, file_format: str, target: str) -> None:
    (slot,) = _source.read_files(
        ctx=Context(),
        cfg=ReaderConf(
            conn=None, paths=[name], file_format=SrcFileFormat[file_format]
        ),
        store=FileStore(root, json.loads(options)),
        origin=lambda store, path: path,
        errors=FilePluginErrors(read_failed=ERROR, write_failed=ERROR),
    )
    (table,) = list(slot)
    table.frame.sink_parquet(target)
    rows = pl.scan_parquet(target).select(pl.len()).collect().item()
    report = {"rows": rows, "peak": peak_bytes(), "threads": pl.thread_pool_size()}
    print(json.dumps(report))


if __name__ == "__main__":
    main(*sys.argv[1:])
