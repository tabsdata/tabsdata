#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from tabsdatak.conn.dev import (
    Data,
    DataGenSrc,
    DataGenSrcConn,
    DevFrameDest,
    DevFrameDestConn,
    DevFrameSrc,
    DevFrameSrcConn,
    TrashDest,
    TrashDestConn,
)
from tabsdatak.conn.dev.error import DevErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    LazyFrameInput,
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
    def _read_slot(ctx: SrcPluginCtx, i: int, data: Data) -> list[TableFileInput]:
        chunk = _next_chunk(ctx, i, data)
        if chunk is None:
            return []
        path = ctx.work_dir / f"datagen_{i}.parquet"
        chunk.write_parquet(path)
        return [TableFileInput(file=path)]


class DevFrameSrcPlugin(SrcPlugin[DevFrameSrcConn, DevFrameSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: DevFrameSrcConn,
        src: DevFrameSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        tables: list[Iterable[LazyFrameInput]] = []
        for i, data in enumerate(src.data):
            try:
                chunk = _next_chunk(ctx, i, data)
            except Exception as exc:
                raise DevErrorCode.DEV_16.exception(cause=exc, slot=i)
            tables.append([] if chunk is None else [LazyFrameInput(chunk.lazy())])
        return tables


def _next_chunk(ctx: SrcPluginCtx, i: int, data: Data) -> pl.DataFrame | None:
    frame = data.frame
    if data.chunk_size is None:
        return frame
    key = f"{_OFFSET_PREFIX}{i}"
    offset = int(ctx.get_attr(key) or 0)
    if offset >= frame.height:
        return None
    chunk = frame.slice(offset, data.chunk_size)
    ctx.set_attr(key, offset + chunk.height)
    return chunk


class TrashDestPlugin(DestPlugin[TrashDestConn, TrashDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: TrashDestConn,
        dest: TrashDest,
        tables: list[TableFileSpec],
    ) -> None:
        pass


class DevFrameDestPlugin(
    DestPlugin[DevFrameDestConn, DevFrameDest, list[pl.LazyFrame]],
):
    def write_out(
        self,
        ctx: DestContext,
        conn: DevFrameDestConn,
        dest: DevFrameDest,
        tables: list[pl.LazyFrame],
    ) -> None:
        report = {
            "tables": [
                {
                    "type": type(table).__name__,
                    "rows": table.select(pl.len()).collect().item(),
                    "columns": table.collect_schema().names(),
                }
                for table in tables
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


DEV_FRAME_SRC = SrcDef(
    conn=DevFrameSrcConn,
    type_="dev-frame-in",
    system="Development",
    src_version="v1",
    src=DevFrameSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.data),
    plugin=DevFrameSrcPlugin,
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


DEV_FRAME_DEST = DestDef(
    conn=DevFrameDestConn,
    type_="dev-frame-out",
    system="Development",
    dest_version="v1",
    dest=DevFrameDest,
    cardinality=lambda dest: dest.outputs,
    plugin=DevFrameDestPlugin,
    explorer=None,
    icon=None,
)
