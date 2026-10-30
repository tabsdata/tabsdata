#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from tabsdatak.conn.dev import (
    DataGenSrc,
    DataGenSrcConn,
    ProbeDest,
    ProbeDestConn,
    TrashDest,
    TrashDestConn,
)
from tabsdatak.conn.dev.error import DevErrorCode
from tabsdatak.spi import (
    DestBusOfLazyFrame,
    DestContext,
    DestDef,
    DestPlugin,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
    TableFileSpec,
    TableMode,
)

# Per-flow scratch-state key holding a chunked slot's next-row offset.
_OFFSET_PREFIX = "datagen.offset."


class DataGenSrcPlugin(SrcPlugin[DataGenSrcConn, DataGenSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: DataGenSrcConn,
        src: DataGenSrc,
    ) -> list[list[TableFileInput]]:
        slots: list[list[TableFileInput]] = []
        for i, data in enumerate(src.data):
            try:
                slots.append(self._read_slot(ctx, i, data))
            except Exception as exc:
                raise DevErrorCode.DEV_8.exception(cause=exc, slot=i)
        return slots

    @staticmethod
    def _read_slot(ctx: SrcPluginCtx, i: int, data) -> list[TableFileInput]:
        frame = data.frame
        if data.chunk_size is None:
            chunk = frame
        else:
            key = f"{_OFFSET_PREFIX}{i}"
            offset = int(ctx.get_attr(key) or 0)
            if offset >= frame.height:
                return []
            chunk = frame.slice(offset, data.chunk_size)
            ctx.set_attr(key, offset + chunk.height)
        path = ctx.work_dir / f"datagen_{i}.parquet"
        chunk.write_parquet(path)
        return [TableFileInput(file=path)]


class TrashDestPlugin(DestPlugin[TrashDestConn, TrashDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: TrashDestConn,
        dest: TrashDest,
        tables: list[TableFileSpec],
    ) -> None:
        pass


class ProbeDestPlugin(DestPlugin[ProbeDestConn, ProbeDest]):
    def write_out(  # type: ignore[override]
        self,
        ctx: DestContext,
        conn: ProbeDestConn,
        dest: ProbeDest,
        bus: DestBusOfLazyFrame,
    ) -> None:
        report = {
            "bus": type(bus).__name__,
            "channels": [
                {
                    "name": channel.name,
                    "position": channel.position,
                    "rows": channel.read().select(pl.len()).collect().item(),
                    "columns": channel.read().collect_schema().names(),
                }
                for channel in bus
            ],
        }
        path = Path(conn.folder) / f"{ctx.transaction_id}.json"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(report, indent=2))
        except OSError as error:
            raise DevErrorCode.DEV_12.exception(
                cause=error,
                path=path,
            )


# --- Bindings ----------------------------------------------------


DATAGEN_SRC = SrcDef(
    conn=DataGenSrcConn,
    type_="dev-datagen-in",
    system="Development",
    src_version="v1",
    src=DataGenSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.data),
    plugin=DataGenSrcPlugin,
    explorer=None,
    icon=None,
)


TRASH_DEST = DestDef(
    conn=TrashDestConn,
    type_="dev-trash-out",
    system="Development",
    dest_version="v1",
    dest=TrashDest,
    cardinality=lambda dest: 1,
    plugin=TrashDestPlugin,
    explorer=None,
    icon=None,
)


PROBE_DEST = DestDef(
    conn=ProbeDestConn,
    type_="dev-probe-out",
    system="Development",
    dest_version="v1",
    dest=ProbeDest,
    cardinality=lambda dest: dest.outputs,
    plugin=ProbeDestPlugin,
    explorer=None,
    icon=None,
)
