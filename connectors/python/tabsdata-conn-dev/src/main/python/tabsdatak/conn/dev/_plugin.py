#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.dev import (
    DataGenSrc,
    DataGenSrcConn,
    TrashDest,
    TrashDestConn,
)
from tabsdatak.conn.dev.error import DevErrorCode
from tabsdatak.spi import (
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
