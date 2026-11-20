#
# Copyright 2026 Tabsdata Inc.
#

"""The last schema each output slot of a source served, kept in its state.

A source cannot always tell the columns of a slot without data: a folder
with no file, a query whose result types only come with its rows, a stream
idle since it started. It still serves the slot with its columns when an
earlier run saw them, and `None` when none did.

The state holds, per slot, the columns and types the function was served,
before any columns rule, as JSON of the types landed metadata stores:

    {"<column>": <encoded type>, ...}

under the key `@td.schema.<slot>`. A slot that never served a column has
no key: a schema, once seen, is never forgotten, so `None` is never
written.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping

import polars as pl

from tabsdatak.api import BasicDictSpec
from tabsdatak.common._storage.landed import (
    StoredMetadataError,
    decode_dtype,
    encode_dtype,
)
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.spi import FrameMetadata, LazyFrameInput, StreamStateSpec

SCHEMA_KEY_PREFIX = "@td.schema."

Slot = int | str
Read = Callable[[str], object]
Write = Callable[[str, str], None]


def schema_key(slot: Slot) -> str:
    return f"{SCHEMA_KEY_PREFIX}{slot}"


def encode_schema(slot: Slot, schema: pl.Schema) -> str:
    try:
        return json.dumps({name: encode_dtype(dtype) for name, dtype in schema.items()})
    except StoredMetadataError as error:
        raise ConnErrorCode.CONN_38.exception(cause=error, slot=slot, detail=error)


def decode_schema(slot: Slot, stored: object) -> pl.Schema | None:
    try:
        if not isinstance(stored, str):
            raise ValueError(f"a {type(stored).__name__}, not a string")
        columns = json.loads(stored)
        if not isinstance(columns, dict):
            raise ValueError(f"a {type(columns).__name__}, not an object")
        schema = pl.Schema({
            name: decode_dtype(dtype) for name, dtype in columns.items()
        })
    except ValueError as error:
        raise ConnErrorCode.CONN_39.exception(cause=error, slot=slot, detail=error)
    return schema or None


def typed(schema: pl.Schema) -> bool:
    """Whether `schema` tells the columns of a slot: it has some, and each
    has a type, not the `Null` a source gives a column it could not type."""
    return bool(schema) and all(dtype != pl.Null for dtype in schema.values())


def served(table: LazyFrameInput, metadata: FrameMetadata | None) -> pl.Schema:
    if metadata is not None and metadata.schema is not None:
        return metadata.schema
    return table.frame.collect_schema()


class SlotSchemas:
    """The last schema of each slot, read from and written to a state.

    Built over a batch source's per-flow attributes (`of`), or over any
    state the source keeps itself, such as a stream state.
    """

    def __init__(self, read: Read, write: Write) -> None:
        self.read = read
        self.write = write

    @classmethod
    def of(cls, ctx) -> SlotSchemas:
        return cls(ctx.get_attr, ctx.set_attr)

    def last(self, slot: Slot) -> pl.Schema | None:
        """The last schema `slot` served, or `None` when it served none."""
        stored = self.read(schema_key(slot))
        return None if stored is None else decode_schema(slot, stored)

    def remember(self, slot: Slot, schema: pl.Schema | None) -> None:
        """Keep `schema` as the last one of `slot`; one without columns, or
        `None`, leaves the last one as it was."""
        if schema:
            self.write(schema_key(slot), encode_schema(slot, schema))

    def served(self, slot: Slot, table: LazyFrameInput) -> LazyFrameInput:
        """`table`, remembering for `slot` the schema it is served with.

        Metadata learnt while the frame streams is remembered once it is
        learnt, after the data landed; other metadata, right away.
        """
        metadata = table.metadata
        if callable(metadata):
            learnt = metadata

            def remembered() -> FrameMetadata:
                described = learnt()
                self.remember(slot, served(table, described))
                return described

            return LazyFrameInput(table.frame, remembered)
        self.remember(slot, served(table, metadata))
        return table

    def empty(
        self,
        slot: Slot,
        metadata: FrameMetadata | None = None,
    ) -> LazyFrameInput | None:
        """An empty frame of the last schema `slot` served, or `None` when it
        served none.

        It keeps the columns, columns rules and system columns inception of
        `metadata`; its data is typed, so it takes no schema, format or
        parse options.
        """
        schema = self.last(slot)
        if schema is None:
            return None
        described = (
            None
            if metadata is None
            else FrameMetadata(
                columns=metadata.columns,
                rules=metadata.rules,
                inception=metadata.inception,
            )
        )
        return LazyFrameInput(pl.LazyFrame(schema=schema), described)


class KeptSchemas(SlotSchemas):
    """The slot schemas of a source that keeps a state of its own, such as a
    stream source: read from that state once, kept in memory as they change,
    and carried into every state the source commits, which replaces the
    last one as a whole.
    """

    def __init__(self, state: Mapping[str, object] | None = None) -> None:
        self.kept: dict[str, str] = {
            key: value
            for key, value in (state or {}).items()
            if key.startswith(SCHEMA_KEY_PREFIX)
        }
        super().__init__(self.kept.get, self.kept.__setitem__)

    def carried(self, state: StreamStateSpec) -> StreamStateSpec:
        """`state` with the schemas kept so far: added to it when it is a
        state, or to what it returns when it commits an offset the source
        owns."""
        if not self.kept:
            return state
        kept = dict(self.kept)
        if not callable(state):
            return {**state, **kept}
        commit = state

        def committed() -> BasicDictSpec:
            return {**(commit() or {}), **kept}

        return committed
