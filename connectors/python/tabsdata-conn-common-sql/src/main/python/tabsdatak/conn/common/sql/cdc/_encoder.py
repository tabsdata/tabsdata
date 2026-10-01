#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

from tabsdatak.conn.common.sql.cdc._model import CdcDataChange
from tabsdatak.conn.common.sql.cdc._sentinel import row_to_record
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


class CdcEncoder:
    def __init__(self, cdc_format: CdcFormat):
        self._cdc_format = cdc_format

    def encode(self, table: str, rows: list[CdcDataChange]) -> Iterator[pa.Table]:
        start = 0
        current_hash = rows[0].schema.hash
        for i in range(1, len(rows)):
            row_hash = rows[i].schema.hash
            if row_hash != current_hash:
                yield self.encode_section(rows, start, i)
                start = i
                current_hash = row_hash
        yield self.encode_section(rows, start, len(rows))

    def encode_section(
        self,
        rows: list[CdcDataChange],
        start: int,
        stop: int,
    ) -> pa.Table:
        records = [row_to_record(rows[i], self._cdc_format) for i in range(start, stop)]
        # noinspection PyArgumentList
        return pa.Table.from_pylist(
            mapping=records,
            schema=rows[start].schema.pa_schema,
        )
