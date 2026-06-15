#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import Self

from tabsdatak.tableframe._core.base import BaseFrame
from tabsdatak.tableframe._engine.model.tableframe import TableFrameState


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

    @classmethod
    def _assemble(cls, lf, mode) -> Self:
        from tabsdatak.extensions.tableframe._engine.column.registry import registry
        from tabsdatak.tableframe._core.translate import validate_schema
        from tabsdatak.tableframe._engine.column.assembly import add_system_columns
        from tabsdatak.tableframe._engine.model.tableframe import DataVersionProperties

        validate_schema(lf)
        lf = add_system_columns(
            lf, registry(), mode, properties=DataVersionProperties.empty()
        )
        return cls._wrap(lf)

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
