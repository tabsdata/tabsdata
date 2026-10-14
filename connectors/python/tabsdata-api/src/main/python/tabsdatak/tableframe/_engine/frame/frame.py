#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from tabsdatak.tableframe._core.base import BaseFrame
from tabsdatak.tableframe._engine.model.frame import TableFrameState

if TYPE_CHECKING:
    from tabsdatak.tableframe.datatypes import DataType
    from tabsdatak.tableframe.expr import Expr
    from tabsdatak.tableframe.grok import ColumnDefinition
    from tabsdatak.tableframe.udf import UDF


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

    @staticmethod
    def _lazy(frame):
        import polars as pl

        return frame.lazy() if isinstance(frame, pl.DataFrame) else frame

    @classmethod
    def _wrap(cls, lf, state=None) -> Self:
        obj = cls.__new__(cls, **{cls._ACCESS_KEY: cls._ACCESS_SECRET})
        obj._lf = cls._lazy(lf)
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
        from tabsdatak.extensions.tableframe._engine.column.registry import registry
        from tabsdatak.tableframe._engine.taxonomy.column import ColumnKind

        tf_registry = registry()
        names = lf.collect_schema().names()
        user = [name for name in names if tf_registry.kind_of(name) is ColumnKind.USER]
        connector = sorted(
            name for name in names if tf_registry.kind_of(name) is ColumnKind.CONNECTOR
        )
        internal = sorted(
            name for name in names if tf_registry.kind_of(name) is ColumnKind.INTERNAL
        )
        return lf.select(user + connector + internal)

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

        df = cls._lazy(df)
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

    def _strip(self, drop_system_columns, drop_internal_columns=False):
        if not drop_system_columns and not drop_internal_columns:
            return self._lf
        return self._select(
            with_internal=False, with_connector=not drop_internal_columns
        )

    def _select(self, *, with_internal: bool, with_connector: bool):
        from tabsdatak.extensions.tableframe._engine.column.registry import registry

        names = self._lf.collect_schema().names()
        keep = registry().keep(
            names, with_internal=with_internal, with_connector=with_connector
        )
        return self._lf.select(keep)

    def _user_columns(self):
        from tabsdatak.extensions.tableframe._engine.column.registry import registry
        from tabsdatak.tableframe._engine.taxonomy.column import ColumnKind

        reg = registry()
        return [
            name
            for name in self._lf.collect_schema().names()
            if reg.kind_of(name) is ColumnKind.USER
        ]

    def _persisted(self):
        from tabsdatak.tableframe._engine.constant.column.prefixes import (
            TD_NAMESPACED_VIRTUAL_SYSTEM_COLUMN_PREFIXES,
        )
        from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

        # Storing after a function run applies the per-column inception policy:
        # the columns marked REGENERATE ($td.id, $td.row) are dropped and
        # recreated, so the stored table carries a fresh identity and a row index
        # sequential from zero regardless of the ops that produced it.
        stored = type(self)._build(
            self._lf,
            CreationMode.SYS,
            idx=self._state.genesis.idx if self._state.genesis is not None else None,
            source=self,
        )
        names = stored._lf.collect_schema().names()
        keep = [
            name
            for name in names
            if not name.startswith(TD_NAMESPACED_VIRTUAL_SYSTEM_COLUMN_PREFIXES)
        ]
        return stored._lf.select(keep)

    def _sink_native(self, path) -> None:
        self._sink(self._persisted(), path)

    def _sink_user(self, path) -> None:
        self._sink(self._strip(drop_system_columns=True), path)

    def _sink_public(self, path) -> None:
        self._sink(
            self._strip(drop_system_columns=True, drop_internal_columns=True), path
        )

    def _sink(self, lf, path) -> None:
        from tabsdatak.tableframe._core.hooks import get_sink_observer

        observer = get_sink_observer(type(self))
        if observer is not None:
            lf = observer(lf, "sink_parquet", path)
        lf.sink_parquet(path, engine="streaming")

    # Construction surface. Class methods so cls is the concrete subclass and the
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

    def _to_polars_lf(self, drop_system_columns=True):
        return self._strip(drop_system_columns)

    def _to_polars_df(self, drop_system_columns=True):
        return self._strip(drop_system_columns).collect(engine="streaming")

    def _to_pandas(self, drop_system_columns=True):
        return self._strip(drop_system_columns).collect(engine="streaming").to_pandas()

    def _to_dict(self, drop_system_columns=True):
        return (
            self
            ._strip(drop_system_columns)
            .collect(engine="streaming")
            .to_dict(as_series=False)
        )

    def extract_one(
        self, with_connector_columns: bool = False, fail_if_empty: bool = False
    ) -> dict[str, DataType | None] | None:
        """Return the only row as a column-to-value dictionary.

        Internal columns are always dropped; connector columns are kept only when
        with_connector_columns is set. Fails if the TableFrame has more than one
        row. Returns None when the TableFrame is empty, unless fail_if_empty is
        set, in which case it fails.
        """
        lf = self._select(with_internal=False, with_connector=with_connector_columns)
        df = lf.limit(2).collect(engine="streaming")
        if df.height == 0:
            return self._on_empty("extract_one", fail_if_empty)
        if df.height > 1:
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF7.exception(op="extract_one")
        return df.row(0, named=True)

    def max_for(
        self, *cols: str, fail_if_empty: bool = False
    ) -> dict[str, DataType | None] | None:
        """Return the given columns of the row that ranks highest by them.

        Each column is compared in its own natural order (numbers numerically,
        dates chronologically, strings alphabetically). The columns are ordered
        comparison keys: the first is primary, each subsequent one breaks ties on
        the previous. Returns None when the TableFrame is empty, unless
        fail_if_empty is set, in which case it fails.
        """
        if not cols:
            raise ValueError("At least one column is required.")
        keyed = self._strip(drop_system_columns=True).top_k(1, by=list(cols))
        df = keyed.select(*cols).collect(engine="streaming")
        if df.height == 0:
            return self._on_empty("max_for", fail_if_empty)
        return df.row(0, named=True)

    def min_for(
        self, *cols: str, fail_if_empty: bool = False
    ) -> dict[str, DataType | None] | None:
        """Return the given columns of the row that ranks lowest by them.

        Each column is compared in its own natural order (numbers numerically,
        dates chronologically, strings alphabetically). The columns are ordered
        comparison keys: the first is primary, each subsequent one breaks ties on
        the previous. Returns None when the TableFrame is empty, unless
        fail_if_empty is set, in which case it fails.
        """
        if not cols:
            raise ValueError("At least one column is required.")
        keyed = self._strip(drop_system_columns=True).bottom_k(1, by=list(cols))
        df = keyed.select(*cols).collect(engine="streaming")
        if df.height == 0:
            return self._on_empty("min_for", fail_if_empty)
        return df.row(0, named=True)

    @staticmethod
    def _on_empty(op: str, fail_if_empty: bool):
        if not fail_if_empty:
            return None
        from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

        raise TableFrameErrorCode.TF8.exception(op=op)

    # Data Quality as a TableFrame namespace is in hibernation: the `_dq`
    # accessor is intentionally disabled until the namespace is revived.
    # @property
    # def _dq(self):
    #     from tabsdatak.common._actions.dataquality.engine import DataQualityEngine
    #
    #     return DataQualityEngine(self)

    def udf(self, on: str | Expr | list[str | Expr], function: UDF) -> Self:
        """Append the output columns of a user defined function.

        The function reads only the columns `on` selects and runs as part of
        the streaming plan, one batch at a time.
        """
        from collections import Counter

        import polars as pl

        from tabsdatak.tableframe._core.hooks import compute
        from tabsdatak.tableframe._core.translate import unwrap
        from tabsdatak.tableframe.udf import UDF
        from tabsdatak.tableframe.udf.error import UDFErrorCode

        if not isinstance(function, UDF):
            raise UDFErrorCode.UDF_15.exception(got=type(function).__name__)
        selected = on if isinstance(on, (list, tuple)) else [on]
        inputs = tuple(
            pl.col(item) if isinstance(item, str) else unwrap(item) for item in selected
        )
        if not inputs:
            raise UDFErrorCode.UDF_9.exception()
        names: Counter[str] = Counter()
        for expression in inputs:
            try:
                names.update(self._lf.select(expression).collect_schema().names())
            except pl.exceptions.ColumnNotFoundError as error:
                raise UDFErrorCode.UDF_16.exception(cause=error, input=str(expression))
        repeated = sorted(name for name, count in names.items() if count > 1)
        if repeated:
            raise UDFErrorCode.UDF_13.exception(names=repeated)
        dtype = function._dtype()
        # Elementwise, so the streaming engine feeds the function batch by
        # batch instead of collecting whole columns first.
        output = (
            pl
            .struct(*inputs)
            .map_batches(function, return_dtype=dtype, is_elementwise=True)
            .struct.unnest()
        )
        return compute(
            "udf",
            (self,),
            {"inputs": inputs, "outputs": dict(dtype.to_schema())},
            call=lambda lf: lf.with_columns(output),
        )

    def grok(
        self,
        expr: str | Expr,
        pattern: str,
        schema: dict[str, ColumnDefinition],
    ) -> Self:
        """Parse text into columns with a Grok pattern.

        Appends one column per named capture in the pattern; a row whose
        text does not match gets null in each of them. The schema maps
        each capture name to the ColumnDefinition for the column it
        produces. Documented on the stub, like the rest of the API.
        """

        import polars as pl

        from tabsdata.expansions.tableframe.features.grok.api._handler import (
            GrokParser,
        )
        from tabsdatak.tableframe._core.translate import unwrap

        def group_by_dtype(dmap):
            groups = {}
            for name, dtype in dmap.items():
                groups.setdefault(dtype, []).append(name)
            return groups

        target = pl.col(expr) if isinstance(expr, str) else unwrap(expr)

        parser = GrokParser(pattern, schema)
        grok = parser.rust(target)

        lf = self._lf.with_columns(grok.alias(parser.temp_column))

        fields = [schema[capture].name or capture for capture in schema.keys()]
        dtypes = {
            schema[capture].name or capture: schema[capture].dtype or pl.String
            for capture in schema.keys()
        }

        lf = (
            lf
            .with_columns(
                pl.col(parser.temp_column).list.to_struct(fields=fields).alias("grok")
            )
            .unnest("grok")
            .with_columns(*[
                pl.col(column).cast(dtype)
                for dtype, column in group_by_dtype(dtypes).items()
                if column
            ])
            .drop(parser.temp_column)
        )

        return self._clone(lf)
