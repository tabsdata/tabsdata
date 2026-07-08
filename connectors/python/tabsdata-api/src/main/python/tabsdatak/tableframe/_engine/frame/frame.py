#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import Self

from tabsdatak.tableframe._core.base import BaseFrame
from tabsdatak.tableframe._engine.model.frame import TableFrameState


class EngineFrame(BaseFrame):
    _ACCESS_KEY = "tdgrant"
    _ACCESS_SECRET = object()

    @classmethod
    def _check_access(cls, kwargs) -> None:
        if kwargs.pop(cls._ACCESS_KEY, None) is not cls._ACCESS_SECRET:
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF1.exception()

    def __new__(cls, *args, **kwargs) -> Self:
        cls._check_access(kwargs)
        return object.__new__(cls)

    def __init__(self, data=None, schema=None, **kwargs) -> None:
        self._check_access(kwargs)
        super().__init__(data, schema, **kwargs)
        self._state = TableFrameState.empty()
        from tabsdatak.tableframe._core.translate import validate_schema

        validate_schema(self._lf)

    @classmethod
    def _wrap(cls, lf, state=None) -> Self:
        obj = cls.__new__(cls, **{cls._ACCESS_KEY: cls._ACCESS_SECRET})
        obj._lf = lf
        obj._state = state if state is not None else TableFrameState.empty()
        return obj

    def _clone(self, lf) -> Self:
        state = self._state.clone() if self._state is not None else None
        return type(self)._wrap(lf, state)

    def _identity(self):
        genesis = self._state.genesis if self._state is not None else None
        return genesis.id if genesis is not None else None

    def __eq__(self, other) -> bool:
        if isinstance(other, EngineFrame):
            return self._identity() == other._identity()
        return self._lf.__eq__(other)

    def __ne__(self, other) -> bool:
        if isinstance(other, EngineFrame):
            return self._identity() != other._identity()
        return self._lf.__ne__(other)

    def __gt__(self, other):
        return self._lf.__gt__(self._operand(other))

    def __lt__(self, other):
        return self._lf.__lt__(self._operand(other))

    def __ge__(self, other):
        return self._lf.__ge__(self._operand(other))

    def __le__(self, other):
        return self._lf.__le__(self._operand(other))

    @staticmethod
    def _operand(other):
        return other._lf if isinstance(other, EngineFrame) else other

    @classmethod
    def _resolve_idx(cls, idx) -> int | None:
        from tabsdatak.tableframe._engine.column.generator import IdxGenerator

        if isinstance(idx, IdxGenerator):
            return idx()
        if idx is None or isinstance(idx, int):
            return idx
        raise ValueError(f"Invalid idx: {idx}")

    @classmethod
    def _resolve_origin(cls, origin, source):
        from tabsdatak.tableframe._engine.taxonomy.frame import TableFrameOrigin

        if isinstance(source, EngineFrame):
            genesis = source._state.genesis if source._state is not None else None
            origin_of = genesis.origin if genesis is not None else None
            if origin_of is TableFrameOrigin.IMPORT:
                return TableFrameOrigin.IMPORT
            return TableFrameOrigin.TRANSFORM
        if origin is None:
            return TableFrameOrigin.BUILD
        if isinstance(origin, TableFrameOrigin):
            return origin
        raise ValueError(f"Invalid origin: {origin}")

    @classmethod
    def _arrange(cls, lf):
        from tabsdatak.tableframe._engine.constant.column.prefixes import (
            TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
            TD_SYSTEM_COLUMN_PREFIX,
        )

        prefixes = (TD_SYSTEM_COLUMN_PREFIX, TD_CONNECTOR_SYSTEM_COLUMN_PREFIX)
        names = lf.collect_schema().names()
        user = [name for name in names if not name.startswith(prefixes)]
        connector = sorted(
            name for name in names if name.startswith(TD_CONNECTOR_SYSTEM_COLUMN_PREFIX)
        )
        standard = sorted(
            name for name in names if name.startswith(TD_SYSTEM_COLUMN_PREFIX)
        )
        return lf.select(user + connector + standard)

    @classmethod
    def _check_required(cls, lf, registry) -> None:
        present = set(lf.collect_schema().names())
        missing = [name for name in registry.required() if name not in present]
        if missing:
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF4.exception(missing=missing)

    @classmethod
    def _build(
        cls, df, mode, *, idx=None, version=None, origin=None, source=None
    ) -> Self:
        from tabsdatak.common._id import encode_id
        from tabsdatak.extensions.tableframe._engine.column.registry import registry
        from tabsdatak.tableframe._core.translate import validate_schema
        from tabsdatak.tableframe._engine.column.assembly import add_system_columns
        from tabsdatak.tableframe._engine.model.frame import (
            DataGenesis,
            DataVersion,
            TableFrameState,
        )

        resolved_idx = cls._resolve_idx(idx)
        resolved_origin = cls._resolve_origin(origin, source)
        resolved_version = version if version is not None else DataVersion.empty()
        reg = registry()

        validate_schema(df)
        df = add_system_columns(
            df, reg, mode, index=resolved_idx, version=resolved_version
        )
        cls._check_required(df, reg)
        df = cls._arrange(df)

        state = TableFrameState(
            version=resolved_version,
            genesis=DataGenesis(
                idx=resolved_idx, origin=resolved_origin, id=encode_id()[1]
            ),
        )
        return cls._wrap(df, state)

    @classmethod
    def _assemble(cls, lf, mode) -> Self:
        return cls._build(lf, mode)

    def _strip(self, drop_system_columns):
        # The conversion surface (to_polars_* / to_pandas / to_dict) drops only the
        # standard `$td.` system columns, leaving connector `@td.` columns in place.
        if not drop_system_columns:
            return self._lf
        from tabsdatak.tableframe._engine.constant.column.prefixes import (
            TD_SYSTEM_COLUMN_PREFIX,
        )

        names = self._lf.collect_schema().names()
        keep = [name for name in names if not name.startswith(TD_SYSTEM_COLUMN_PREFIX)]
        return self._lf.select(keep)

    def _persisted(self):
        from tabsdatak.tableframe._engine.constant.column.prefixes import (
            TD_NAMESPACED_VIRTUAL_SYSTEM_COLUMN_PREFIXES,
        )

        names = self._lf.collect_schema().names()
        keep = [
            name
            for name in names
            if not name.startswith(TD_NAMESPACED_VIRTUAL_SYSTEM_COLUMN_PREFIXES)
        ]
        return self._lf.select(keep)

    def _sink_native(self, path) -> None:
        from tabsdatak.tableframe._core.hooks import get_sink_observer

        lf = self._persisted()
        observer = get_sink_observer(type(self))
        if observer is not None:
            lf = observer(lf, "sink_parquet", path)
        lf.sink_parquet(path, engine="streaming")

    # Construction surface. Classmethods so cls is the concrete subclass and the
    # wrapped result is a TableFrame; each normalizes its input to a polars
    # LazyFrame, then routes through _assemble to seed the edition's system columns.
    @classmethod
    def from_polars(cls, data=None, schema=None) -> Self:
        import polars as pl

        from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

        if isinstance(data, pl.LazyFrame):
            lf = data.cast(schema) if schema is not None else data
        elif isinstance(data, pl.DataFrame) or data is None:
            lf = pl.LazyFrame(data, schema=schema)
        else:
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF3.exception(
                factory="from_polars", type=type(data).__name__
            )
        return cls._assemble(lf, CreationMode.RAW)

    @classmethod
    def from_pandas(cls, data=None, schema=None) -> Self:
        import polars as pl

        from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

        if data is None:
            lf = pl.LazyFrame(None, schema=schema)
        else:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                lf = pl.from_pandas(data).lazy()
                if schema is not None:
                    lf = lf.cast(schema)
            else:
                from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

                raise TableFrameErrorCode.TF3.exception(
                    factory="from_pandas", type=type(data).__name__
                )
        return cls._assemble(lf, CreationMode.RAW)

    @classmethod
    def from_dict(cls, data=None, schema=None) -> Self:
        import polars as pl

        from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

        if data is None or isinstance(data, dict):
            lf = pl.LazyFrame(data, schema=schema)
        else:
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF3.exception(
                factory="from_dict", type=type(data).__name__
            )
        return cls._assemble(lf, CreationMode.RAW)

    @classmethod
    def empty(cls, schema=None) -> Self:
        import polars as pl

        from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

        return cls._assemble(pl.LazyFrame(schema=schema), CreationMode.TAB)

    # Materialization surface. Exits the wrapper into a plain polars / pandas / dict;
    # drop_system_columns (default True) removes the standard `$td.` columns.
    def to_polars_lf(self, drop_system_columns=True):
        return self._strip(drop_system_columns)

    def to_polars_df(self, drop_system_columns=True):
        return self._strip(drop_system_columns).collect(no_optimization=True)

    def to_pandas(self, drop_system_columns=True):
        return (
            self._strip(drop_system_columns).collect(no_optimization=True).to_pandas()
        )

    def to_dict(self, drop_system_columns=True):
        return (
            self
            ._strip(drop_system_columns)
            .collect(no_optimization=True)
            .to_dict(as_series=False)
        )
