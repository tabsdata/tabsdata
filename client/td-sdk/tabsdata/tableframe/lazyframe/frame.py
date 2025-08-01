#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import enum
import logging
import os
from collections.abc import Collection, Iterable, Mapping, Sequence
from enum import auto
from typing import TYPE_CHECKING, Any, List, Literal, NoReturn, TypeVar, Union, cast

import polars as pl
from accessify import accessify, private

# noinspection PyProtectedMember
from polars.dependencies import numpy as np

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._common as td_common

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._generators as td_generators

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._helpers as td_helpers

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._reflection as td_reflection

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._translator as td_translator

# noinspection PyProtectedMember
import tabsdata.tableframe._typing as td_typing
import tabsdata.tableframe.dataframe.frame as td_frame

# noinspection PyProtectedMember
import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.lazyframe.group_by as td_group_by
from tabsdata._utils.annotations import pydoc
from tabsdata.exceptions import ErrorCode, TableFrameError
from tabsdata.extensions._tableframe.extension import TableFrameExtension

if TYPE_CHECKING:
    import pandas as pd

# ToDo: SDK-128: Define the logging model for SDK CLI execution
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

IndexInput = Union[int, td_generators.IdxGenerator, None]


# These origins are currently shallowly used, and no relevant functional difference
# should be perceived depending on mark in TableFrames. Currently, these marks are
# mainly informative.
class TableFrameOrigin(enum.Enum):
    # A TableFrame that was created from external data, or from a TableFrame that
    # already was marked as IMPORT. Here, external data means data not loaded from
    # a function. It serves to recognize pure external data.
    IMPORT = auto()
    # A TableFrame that was created from internal data. Here, internal data is data
    # loaded from a function, or from an existing table. It serves to recognize pure
    # internal data.
    BUILD = auto()
    # A TableFrame that was created using the init function, which is the standard way
    # to declare some data as internal even if actually external. It serves to recognize
    # TableFrames the user created out of existing TableFrames or out of some volatile
    # data used in functions processing. Main difference with IMPORT data is that these
    # TableFrames will have provenance support.
    INIT = auto()
    # A TableFrame that was created from a TableFrame using any of the available
    # transformation functions available in the TableFrame API, except when the rules
    # above indicated to mark them differently.
    TRANSFORM = auto()


@accessify
class TableFrame:
    """> Private Functions"""

    @classmethod
    def _from_lazy(cls, lf: pl.LazyFrame) -> TableFrame:
        """Use only for testing."""
        return TableFrame.__build__(
            df=lf,
            mode="raw",
            idx=0,
        )

    def _to_lazy(self) -> pl.LazyFrame:
        return self._lf

    """> Initialization Functions """

    @classmethod
    @pydoc(categories="tableframe")
    def empty(cls) -> TableFrame:
        """
        Creates an empty (no column - no row) TableFrame.
        """
        return TableFrame.__build__(
            origin=TableFrameOrigin.IMPORT,
            df=None,
            mode="tab",
            idx=None,
        )

    @classmethod
    @pydoc(categories="tableframe")
    def from_polars(
        cls,
        data: pl.LazyFrame | pl.DataFrame | None = None,
    ) -> TableFrame:
        """
        Creates tableframe from a polars dataframe or lazyframe, or None.
        `None` produces as an empty (no column - no row) tableframe.

        Args:
            data: Input data.
        """
        # noinspection PyProtectedMember
        if data is None:
            data_out = pl.LazyFrame(None)
        elif isinstance(data, pl.LazyFrame):
            data_out = data
        elif isinstance(data, pl.DataFrame):
            data_out = data.lazy()
        else:
            raise TableFrameError(ErrorCode.TF11, type(data))
        return cls.__build__(
            origin=TableFrameOrigin.IMPORT, df=data_out, mode="raw", idx=None
        )

    @classmethod
    @pydoc(categories="tableframe")
    def from_pandas(
        cls,
        data: pd.DataFrame | None = None,
    ) -> TableFrame:
        """
        Creates tableframe from a pandas dataframe, or None.
        `None` produces as an empty (no column - no row) tableframe.

        Args:
            data: Input data.
        """

        import pandas as pd

        # noinspection PyProtectedMember
        if data is None:
            data_out = pl.LazyFrame(None)
        elif isinstance(data, pd.DataFrame):
            data_out = pl.from_pandas(data)
        else:
            raise TableFrameError(ErrorCode.TF12, type(data))
        return cls.__build__(
            origin=TableFrameOrigin.IMPORT, df=data_out, mode="raw", idx=None
        )

    @classmethod
    @pydoc(categories="tableframe")
    def from_dict(
        cls,
        data: td_typing.TableDictionary | None = None,
    ) -> TableFrame:
        """
        Creates tableframe from a dictionary, or None.
        `None` produces as an empty (no column - no row) tableframe.

        Args:
            data: Input data.
        """
        # noinspection PyProtectedMember
        if data is None:
            data_out = pl.LazyFrame(None)
        elif isinstance(data, dict):
            data_out = pl.LazyFrame(data)
        else:
            raise TableFrameError(ErrorCode.TF13, type(data))
        return cls.__build__(
            origin=TableFrameOrigin.IMPORT, df=data_out, mode="raw", idx=None
        )

    @pydoc(categories="tableframe")
    def to_polars_lf(self) -> pl.LazyFrame:
        """
        Creates a polars lazyframe from this tableframe.
        """
        # noinspection PyProtectedMember
        return td_translator._unwrap_table_frame(self)

    @pydoc(categories="tableframe")
    def to_polars_df(self) -> pl.DataFrame:
        """
        Creates a polars dataframe from this tableframe.
        """
        # noinspection PyProtectedMember
        return td_translator._unwrap_table_frame(self).collect(no_optimization=True)

    @pydoc(categories="tableframe")
    def to_pandas(self) -> pd.DataFrame:
        """
        Creates a pandas dataframe from this tableframe.
        """
        # noinspection PyProtectedMember
        return (
            td_translator._unwrap_table_frame(self)
            .collect(no_optimization=True)
            .to_pandas()
        )

    @pydoc(categories="tableframe")
    def to_dict(self) -> dict[str, list[Any]]:
        """
        Creates a dictionary from this tableframe.
        """
        # noinspection PyProtectedMember
        return (
            td_translator._unwrap_table_frame(self)
            .collect(no_optimization=True)
            .to_dict(as_series=False)
        )

    # Passing a IdxGenerator for idx is meant to be used only when populating pub
    # tables, An IdxGenerator is a stateful callable class that ensures a unique
    # sequential id is generated in each invocation.
    @classmethod
    # flake8: noqa: C901
    def __build__(
        cls,
        *,
        origin: TableFrameOrigin | None = None,
        df: td_typing.TableDictionary | pl.LazyFrame | pl.DataFrame | TableFrame | None,
        mode: td_common.AddSystemColumnsMode,
        idx: IndexInput,
    ) -> TableFrame:
        if isinstance(df, TableFrame):
            if df._origin.value == TableFrameOrigin.IMPORT.value:
                origin = TableFrameOrigin.IMPORT
            else:
                origin = TableFrameOrigin.TRANSFORM
        elif origin is None:
            origin = TableFrameOrigin.BUILD
        elif isinstance(origin, TableFrameOrigin):
            pass
        else:
            raise ValueError(f"Invalid origin: {origin}")

        if isinstance(idx, td_generators.IdxGenerator):
            # noinspection PyProtectedMember
            idx = idx()
        elif idx is None:
            idx = None
        elif isinstance(idx, int):
            idx = idx
        else:
            raise ValueError(f"Invalid idx: {idx}")

        # noinspection PyProtectedMember
        if df is None:
            df = pl.LazyFrame(None)
        elif isinstance(df, dict):
            df = pl.LazyFrame(df)
        elif isinstance(df, pl.LazyFrame):
            pass
        elif isinstance(df, pl.DataFrame):
            df = df.lazy()
        elif isinstance(df, TableFrame):
            df = df._lf
        else:
            raise TableFrameError(ErrorCode.TF2, type(df))
        df = td_common.add_system_columns(lf=df, mode=mode, idx=idx)
        td_reflection.check_required_columns(df)

        instance = cls.__new__(cls)
        instance._origin = origin
        # noinspection PyProtectedMember
        instance._id = td_generators._id()
        instance._idx = idx
        instance._lf = _arrange_columns(df)
        return instance

    def __init__(
        self,
        df: td_typing.TableDictionary | TableFrame | None = None,
        *,
        origin: TableFrameOrigin | None = TableFrameOrigin.INIT,
    ) -> None:
        if isinstance(df, TableFrame):
            # noinspection PyProtectedMember
            if df._origin.value == TableFrameOrigin.IMPORT.value:
                origin = TableFrameOrigin.IMPORT
            else:
                origin = TableFrameOrigin.TRANSFORM
        elif origin is None:
            origin = TableFrameOrigin.INIT
        elif isinstance(origin, TableFrameOrigin):
            pass
        else:
            raise ValueError(f"Invalid origin: {origin}")

        if isinstance(df, TableFrame):
            mode = "tab"
            # noinspection PyProtectedMember
            idx = df._idx
            # noinspection PyProtectedMember
            df = df._lf
        else:
            mode = "raw"
            idx = None
            if df is None:
                df = pl.LazyFrame(None)
            elif isinstance(df, dict):
                df = pl.LazyFrame(df)
            else:
                raise TableFrameError(ErrorCode.TF2, type(df))
        df = td_common.add_system_columns(
            lf=df,
            mode=cast(td_common.AddSystemColumnsMode, mode),
            idx=idx,
        )
        td_reflection.check_required_columns(df)

        self._origin = origin
        # noinspection PyProtectedMember
        self._id = td_generators._id()
        self._idx = idx
        self._lf: pl.LazyFrame | None = _arrange_columns(df)

    def columns(
        self, kind: Literal["all", "user", "system"] | None = "user"
    ) -> list[str]:
        kind = kind or "user"
        all_columns = self._lf.collect_schema().names()
        system_columns = set(td_helpers.SYSTEM_COLUMNS)
        if kind == "all":
            return all_columns
        elif kind == "user":
            return [col for col in all_columns if col not in system_columns]
        elif kind == "system":
            return [col for col in all_columns if col in system_columns]
        else:
            raise ValueError(f"Unknown column kind: {kind}")

    @pydoc(categories="attributes")
    @property
    def dtypes(self) -> list[td_typing.DataType]:
        return self._lf.collect_schema().dtypes()

    @property
    def schema(self) -> td_typing.Schema:
        return self._lf.collect_schema()

    @property
    def width(self) -> int:
        return self.schema.len()

    """> Special Functions """

    # ToDo: pending restricted access and system td columns handling.
    # status(Status.TODO)
    def __getattr__(self, name):
        if name in self._lf.__dict__:
            attr = getattr(self._lf, name)
            if callable(attr):

                def wrapper(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    if isinstance(result, pl.LazyFrame):
                        return TableFrame.__build__(
                            df=result,
                            mode="tab",
                            idx=self._idx,
                        )
                    return result

                return wrapper
            return attr
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def __bool__(self) -> NoReturn:
        return not self.is_empty()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TableFrame):
            return self._id == other._id
        else:
            return self._lf.__eq__(other=other)

    def __ne__(self, other: object) -> bool:
        if isinstance(other, TableFrame):
            return self._id != other._id
        else:
            return self._lf.__ne__(other=other)

    def __gt__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__gt__(other=other._lf)
        else:
            return self._lf.__gt__(other=other)

    def __lt__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__lt__(other=other._lf)
        else:
            return self._lf.__lt__(other=other)

    def __ge__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__ge__(other=other._lf)
        else:
            return self._lf.__ge__(other=other)

    def __le__(self, other: Any) -> NoReturn:
        if isinstance(other, TableFrame):
            return self._lf.__le__(other=other._lf)
        else:
            return self._lf.__le__(other=other)

    # ToDo: should we block system td columns?
    def __contains__(self, key: str) -> bool:
        return self._lf.__contains__(key=key)

    def __copy__(self) -> TableFrame:
        msg = (
            "__copy__ not supported for TableFrame objects as it is not deterministic."
        )
        raise TypeError(msg)

    def __deepcopy__(self, memo: None = None) -> TableFrame:
        msg = (
            "__deepcopy__ not supported for TableFrame objects as it is not"
            " deterministic."
        )
        raise TypeError(msg)

    def __getitem__(self, item: int | range | slice) -> TableFrame:
        return TableFrame.__build__(
            df=self._lf.__getitem__(item=item),
            mode="tab",
            idx=self._idx,
        )

    def __str__(self) -> str:
        return self._lf.explain(optimized=False)

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} at 0x{id(self):X}> with {self._lf.__repr__()}"
        ).replace("LazyFrame", "TableFrame")

    @private
    def _repr_html_(self) -> str:
        # noinspection PyProtectedMember
        return self._lf._repr_html_().replace("LazyFrame", "TableFrame")

    """> Description Functions """

    @private
    @pydoc(categories="description")
    def explain(self) -> None:
        logger.info(
            self._lf.explain(
                format="plain",
                optimized=True,
                type_coercion=True,
                predicate_pushdown=True,
                projection_pushdown=True,
                simplify_expression=True,
                slice_pushdown=True,
                comm_subplan_elim=True,
                comm_subexpr_elim=True,
                cluster_with_columns=True,
                collapse_joins=True,
                streaming=False,
                tree_format=None,
            )
        )

    @private
    @pydoc(categories="description")
    def show_graph(self) -> str | None:
        logger.info(
            self._lf.show_graph(
                optimized=True,
                show=True,
                output_path=None,
                raw_output=False,
                figsize=(16.0, 12.0),
                type_coercion=True,
                predicate_pushdown=True,
                projection_pushdown=True,
                simplify_expression=True,
                slice_pushdown=True,
                comm_subplan_elim=True,
                comm_subexpr_elim=True,
                cluster_with_columns=True,
                collapse_joins=True,
                streaming=False,
            )
        )

    @private
    def inspect(self, fmt: str = "{}") -> TableFrame:
        return TableFrame.__build__(
            df=self._lf.inspect(fmt=fmt),
            mode="tab",
            idx=self._idx,
        )

    @pydoc(categories="tableframe")
    def has_same_schema(self, tf: TableFrame) -> bool:
        """
        Verifies if the schema of the current TableFrame is same than the provided
        TableFrame.

        Args:
            tf: The TableFrame to compare with.

        Returns:
            bool: Whether the condition is met or not.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf1: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        └──────┴──────┘
        >>>
        >>> tf2: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ c    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        └──────┴──────┘
        >>> tf1.has_same_schema(tf2)
        >>>
        False
        >>>
        >>> tf1: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        └──────┴──────┘
        >>>
        >>> tf2: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ str  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        └──────┴──────┘
        >>> tf1.has_same_schema(tf2)
        >>>
        False
        """
        return self.schema == tf.schema

    @pydoc(categories="tableframe")
    def is_empty(self) -> bool:
        """
        Checks if a TableFrame has no rows.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        └──────┴──────┘
        >>>
        >>> tf.is_empty()
        >>>
        False
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        └──────┴──────┘
        >>>
        >>> tf.is_empty()
        >>>
        True
        """
        return self._lf.limit(1).collect().height == 0

    @pydoc(categories="tableframe")
    def has_cols(
        self, cols: str | list[str], exact: bool | None = False
    ) -> (bool, set[str], set[str]):
        """
        Verifies the presence of (non-system) columns in the TableFrame.

        If `exact` is True, the check ensures that the TableFrame contains exactly the
        specified columns (excluding system columns), with no extras or omissions.

        Args:
            cols: The column name(s) to verify. Can be a string or a list of strings.
            exact: If True, checks that the TableFrame contains exactly the specified
                columns.

        Returns:
            tuple[bool, set[str], set[str]]:
                - A boolean indicating whether the check was successful.
                - A set of columns in `cols` missing in the TableFrame.
                - A set of columns in the TableFrame missing in `cols`.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        └──────┴──────┘
        >>>
        >>> tf.has_cols("a")
        >>>
        (True, {}, {"b"})
        >>>
        >>> tf.has_cols(["a", "c", "d"])
        >>>
        (False, {"c", "d"}, {"b"})
        >>>
        >>> tf.has_cols("a", exact=True)
        >>>
        (False, {}, {"b"})
        >>>
        >>> tf.has_cols(["a", "b"], exact=True)
        >>>
        (True, {}, {})
        """
        # noinspection DuplicatedCode
        if not isinstance(cols, str) and (
            not isinstance(cols, list)
            or not all(isinstance(column, str) for column in cols)
        ):
            raise TypeError(
                "Columns to check need to be either a single string or a list of"
                " strings."
            )

        tableframe_columns = self._lf.collect_schema().names()

        system_columns_set = set(td_helpers.SYSTEM_COLUMNS)
        table_frame_columns_set = set(tableframe_columns) - system_columns_set
        expected_columns_set = set(cols) - system_columns_set

        in_cols_not_in_tf = expected_columns_set - table_frame_columns_set
        in_tf_not_in_cols = table_frame_columns_set - expected_columns_set
        return (
            (
                (len(in_cols_not_in_tf) == 0 and len(in_tf_not_in_cols) == 0)
                if exact
                else len(in_cols_not_in_tf) == 0
            ),
            in_cols_not_in_tf,
            in_tf_not_in_cols,
        )

    @pydoc(categories="tableframe")
    def assert_has_cols(
        self, cols: str | list[str], exact: bool | None = False
    ) -> None:
        """
        Ensures that the (non-system) columns in the TableFrame match the expected
        columns.

        Raises an exception if the expectation is not met.

        If `exact` is True, the check verifies that the TableFrame contains exactly the
        expected columns, with no extra or missing ones.

        Args:
            cols: The expected column name(s). Can be a string or a list of strings.
            exact: If True, checks that the TableFrame contains exactly the specified
                columns.

        Raises:
            ValueError: If expected columns are missing or unexpected columns are
                present in the TableFrame.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>> tf.assert_has_cols("a")
        >>> tf.assert_has_cols(["a", "b"], exact=True)
        """
        success, not_in_tf, not_in_cols = self.has_cols(cols, exact=exact)
        if not success:
            raise ValueError(
                "Column check failed.\n"
                f"Missing in TableFrame: {sorted(not_in_tf)}\n"
                f"Unexpected in TableFrame: {sorted(not_in_cols)}"
            )

    """> Transformation Functions """

    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="tableframe")
    def sort(
        self,
        by: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
        *more_by: td_expr.IntoExpr,
        descending: bool | Sequence[bool] = False,
        nulls_last: bool | Sequence[bool] = False,
        maintain_order: bool = False,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Sort the `TableFrame` by the given column(s) or expression(s).

        Args:
            by: Column(s) or expression(s) to sort by.
            more_by: Additional colums to sort by.
            descending: Specifies if the sorting should be descending or not.
            nulls_last: Specifies if `null` values should be placed last.
            maintain_order: Preserve the order of equal rows.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.sort(td.col("a"), descending = True)
        >>>
        ┌──────┬───────┐
        │ a    ┆ b     │
        │ ---  ┆ ---   │
        │ str  ┆ f32   │
        ╞══════╪═══════╡
        │ A    ┆ 1.0   │
        │ X    ┆ 10.0  │
        │ C    ┆ 3.0   │
        │ D    ┆ 5.0   │
        │ M    ┆ 9.0   │
        │ A    ┆ 100.0 │
        │ M    ┆ 50.0  │
        │ null ┆ 20.0  │
        │ F    ┆ null  │
        └──────┴───────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.sort(
                by=td_translator._unwrap_into_tdexpr([by] + list(more_by)),
                *more_by,
                descending=descending,
                nulls_last=nulls_last,
                maintain_order=maintain_order,
                multithreaded=False,
            ),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: disallow transformations in system td columns.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def cast(
        self,
        dtypes: (
            Mapping[
                td_typing.ColumnNameOrSelector | td_typing.DataType, td_typing.DataType
            ]
            | td_typing.DataType
        ),
        *,
        strict: bool = True,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Cast columns to a new data type.

        Args:
            dtypes: Mapping of the column name(s) to the new data type(s).
            strict: If `True`, raises an error if the cast cannot be performed.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.cast({"b":td.Float32}).collect()
        >>>
        ┌──────┬───────┐
        │ a    ┆ b     │
        │ ---  ┆ ---   │
        │ str  ┆ f32   │
        ╞══════╪═══════╡
        │ A    ┆ 1.0   │
        │ X    ┆ 10.0  │
        │ C    ┆ 3.0   │
        │ D    ┆ 5.0   │
        │ M    ┆ 9.0   │
        │ A    ┆ 100.0 │
        │ M    ┆ 50.0  │
        │ null ┆ 20.0  │
        │ F    ┆ null  │
        └──────┴───────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.cast(
                dtypes=td_translator._unwrap_tdexpr(dtypes), strict=strict
            ),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: should we allow only clear to 0 rows?
    # status(Status.TODO)
    @pydoc(categories="tableframe")
    def clear(self, n: int = 0) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Clears all rows of the `TableFrame` preserving the schema.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.cast({"b":td.Float32}).collect()
        >>>
        ┌──────┬───────┐
        │ a    ┆ b     │
        │ ---  ┆ ---   │
        │ str  ┆ f32   │
        ╞══════╪═══════╡
        └──────┴───────┘
        """
        return TableFrame.__build__(
            df=self._lf.clear(n=n),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="join")
    def join(
        self,
        other: TableFrame,
        on: str | td_expr.Expr | Sequence[str | td_expr.Expr] | None = None,
        how: td_typing.JoinStrategy = "inner",
        *,
        left_on: str | td_expr.Expr | Sequence[str | td_expr.Expr] | None = None,
        right_on: str | td_expr.Expr | Sequence[str | td_expr.Expr] | None = None,
        suffix: str = "_right",
        join_nulls: bool = False,
        coalesce: bool | None = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Join the `TableFrame` with another `TableFrame`.

        Args:
            other: The `TableFrame` to join.
            on: Name(s) of the columns to join on. The column name(s) must be in
                both `TableFrame's. Don't use this parameter if using `left_on`
                and `right_on` parameters, or if `how="cross"`.
            how: Join strategy:
                * `inner`: An inner join.
                * `left`: A left join.
                * `right`: A rigth join.
                * `full`: A full join.
                * `cross`: The cartesian product.
                * `semi`: An inner join but only returning the columns from left
                          `TableFrame`.
                * *anti*: Rows from the left `TableFrame` that have no match
                          in the right `TableFrame`.
            left_on: Name(s) of the columns to join on from the left `TableFrame`.
                It must be used together wit the `right_on` parameter.
                It cannot be used with the `on` parameter.
            right_on: Name(s) of the columns to join on from the right `TableFrame`.
                It must be used together wit the `left_on` parameter.
                It cannot be used with the `on` parameter.
            suffix: Duplicate columns on the right `Table` are appended this suffix.
            join_nulls: If `null` value matches should produce join rows or not.
            coalesce: Collapse join columns into a single column.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf1: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf2: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 3    │
        │ Y    ┆ 4    │
        │ Z    ┆ 5    │
        │ A    ┆ 0    │
        │ M    ┆ 6    │
        │ null ┆ 8    │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        An inner join:
        >>>
        >>> tf1.join(tf2, on="a", how="inner")
        >>>
        ┌─────┬──────┬─────────┐
        │ a   ┆ b    ┆ b_right │
        │ --- ┆ ---  ┆ ---     │
        │ str ┆ i64  ┆ i64     │
        ╞═════╪══════╪═════════╡
        │ A   ┆ 1    ┆ 3       │
        │ A   ┆ 1    ┆ 0       │
        │ M   ┆ 9    ┆ 6       │
        │ A   ┆ 100  ┆ 3       │
        │ A   ┆ 100  ┆ 0       │
        │ M   ┆ 50   ┆ 6       │
        │ F   ┆ null ┆ null    │
        └─────┴──────┴─────────┘
        >>>
        A left join:
        >>>
        >>> tf1.join(tf2, on="a", how="left")
        >>>
        ┌──────┬──────┬─────────┐
        │ a    ┆ b    ┆ b_right │
        │ ---  ┆ ---  ┆ ---     │
        │ str  ┆ i64  ┆ i64     │
        ╞══════╪══════╪═════════╡
        │ A    ┆ 1    ┆ 3       │
        │ A    ┆ 1    ┆ 0       │
        │ X    ┆ 10   ┆ null    │
        │ C    ┆ 3    ┆ null    │
        │ D    ┆ 5    ┆ null    │
        │ …    ┆ …    ┆ …       │
        │ A    ┆ 100  ┆ 3       │
        │ A    ┆ 100  ┆ 0       │
        │ M    ┆ 50   ┆ 6       │
        │ null ┆ 20   ┆ null    │
        │ F    ┆ null ┆ null    │
        └──────┴──────┴─────────┘
        >>>
        Turning off column coalesce:
        >>>
        >>> tf1.join(tf2, on="a", coalesce=False)
        >>>
        ┌─────┬──────┬─────────┬─────────┐
        │ a   ┆ b    ┆ a_right ┆ b_right │
        │ --- ┆ ---  ┆ ---     ┆ ---     │
        │ str ┆ i64  ┆ str     ┆ i64     │
        ╞═════╪══════╪═════════╪═════════╡
        │ A   ┆ 1    ┆ A       ┆ 3       │
        │ A   ┆ 1    ┆ A       ┆ 0       │
        │ M   ┆ 9    ┆ M       ┆ 6       │
        │ A   ┆ 100  ┆ A       ┆ 3       │
        │ A   ┆ 100  ┆ A       ┆ 0       │
        │ M   ┆ 50   ┆ M       ┆ 6       │
        │ F   ┆ null ┆ F       ┆ null    │
        └─────┴──────┴─────────┴─────────┘
        """
        # noinspection PyProtectedMember
        lf = self._lf.join(
            other=other._lf,
            on=td_translator._unwrap_tdexpr(on),
            how=how,
            left_on=td_translator._unwrap_tdexpr(left_on),
            right_on=td_translator._unwrap_tdexpr(right_on),
            suffix=suffix,
            validate="m:m",
            join_nulls=join_nulls,
            coalesce=coalesce,
            allow_parallel=True,
            force_parallel=False,
        )
        return TableFrame.__build__(
            df=_assemble_system_columns(lf),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="projection")
    def with_columns(
        self,
        *exprs: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
        **named_exprs: td_expr.IntoExpr,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Add columns to the `TableFrame`.

        Args:
            exprs: Columns or expressions to add.
            named_exprs: Named expressions to add.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ 10.0 ┆ 10.0 │
        │ 4.0  ┆ 10.0 │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.with_columns(td.col("x").mul(td.col("y")).alias("z"))
        >>>
        ┌──────┬──────┬──────┐
        │ x    ┆ y    ┆ z    │
        │ ---  ┆ ---  ┆ ---  │
        │ f64  ┆ f64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ 1.0  ┆ 2.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  ┆ 4.0  │
        │ NaN  ┆ NaN  ┆ NaN  │
        │ 4.0  ┆ NaN  ┆ NaN  │
        │ 5.0  ┆ null ┆ null │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.with_columns(
                *[td_translator._unwrap_into_tdexpr_column(column) for column in exprs],
                **named_exprs,
            ),
            mode="tab",
            idx=self._idx,
        )

    @pydoc(categories="projection")
    def rename(self, mapping: dict[str, str]) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Rename columns from the `TableFrame`.

        Args:
            mapping
                A dictionary mapping column names to their new names.
                The operation will fail if any specified column name does not exist.

        Examples:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ i    ┆ f    │
        │ ---  ┆ ---  │
        │ i32  ┆ f64  │
        ╞══════╪══════╡
        │ 1    ┆ 3.1  │
        │ 2    ┆ 4.1  │
        │ 3    ┆ 5.9  │
        │ 4    ┆ 2.6  │
        │ 5    ┆ 53.5 │
        │ 6    ┆ 8.97 │
        └──────┴──────┘
        >>>
        >>> tf.{"i": "index", "f": "amount"})
        >>>
        ┌───────┬────────┐
        │ index ┆ amount │
        │ ----- ┆ ------ │
        │ i32   ┆ f64    │
        ╞═══════╪════════╡
        │ 1     ┆ 3.1    │
        │ 2     ┆ 4.1    │
        │ 3     ┆ 5.9    │
        │ 4     ┆ 2.6    │
        │ 5     ┆ 53.5   │
        │ 6     ┆ 8.97   │
        └───────┴────────┘

        """

        if not isinstance(mapping, dict):
            raise TypeError("Expected a dictionary of type dict[str, str]")

        for old_name, new_name in mapping.items():
            if not isinstance(old_name, str):
                raise TypeError(
                    f"Expected dict[str, str], but got old column name: '{old_name!r}'"
                )
            if not isinstance(new_name, str):
                raise TypeError(
                    f"Expected dict[str, str], but got new column name: '{old_name!r}'"
                )
            td_common.check_column_name(old_name)
            td_common.check_column_name(new_name)
        return TableFrame.__build__(
            df=self._lf.rename(mapping, strict=True),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="projection")
    def drop(
        self,
        *columns: td_typing.ColumnNameOrSelector
        | Iterable[td_typing.ColumnNameOrSelector],
        strict: bool = True,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Discard columns from the `TableFrame`.

        Args:
            columns: Columns to drop.
            strict: If True, raises an error if a column does not exist.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ 10.0 ┆ 10.0 │
        │ 4.0  ┆ 10.0 │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.drop("y")
        >>>
        ┌──────┐
        │ x    │
        │ ---  │
        │ f64  │
        ╞══════╡
        │ 1.0  │
        │ 2.0  │
        │ NaN  │
        │ 4.0  │
        │ 5.0  │
        │ null │
        └──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.drop(td_translator._unwrap_tdexpr(*columns), strict=strict),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def fill_null(
        self,
        value: Any | td_expr.Expr | None = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Replace all `null` values in the `TableFrame` with the given value.

        Args:
            value: The value to replace `null` with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ NaN  ┆ NaN  │
        │ 4.0  ┆ NaN  │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.fill_null(20)
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ NaN  ┆ NaN  │
        │ 4.0  ┆ NaN  │
        │ 5.0  ┆ 20.0 │
        │ 20.0 ┆ 20.0 │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.fill_null(
                value=td_translator._unwrap_tdexpr(value),
                strategy=None,
                limit=None,
                matches_supertype=True,
            ),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def fill_nan(self, value: int | float | td_expr.Expr | None) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Replace all `NaN` values in the `TableFrame` with the given value.

        Args:
            value: The value to replace `NaN` with.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ NaN  ┆ NaN  │
        │ 4.0  ┆ NaN  │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.fill_nan(10)
        >>>
        ┌──────┬──────┐
        │ x    ┆ y    │
        │ ---  ┆ ---  │
        │ f64  ┆ f64  │
        ╞══════╪══════╡
        │ 1.0  ┆ 2.0  │
        │ 2.0  ┆ 2.0  │
        │ 10.0 ┆ 10.0 │
        │ 4.0  ┆ 10.0 │
        │ 5.0  ┆ null │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.fill_nan(value=td_translator._unwrap_tdexpr(value)),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="filters")
    def unique(
        self,
        subset: (
            td_typing.ColumnNameOrSelector
            | Collection[td_typing.ColumnNameOrSelector]
            | None
        ) = None,
        *,
        keep: td_typing.UniqueKeepStrategy = "any",
        maintain_order: bool = False,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Deduplicate rows from the `TableFrame`.

        Args:
            subset: Columns to evaluate for duplicate values. If None, all columns are
                considered.
            keep: Strategy to keep duplicates: `first`, `last`, `any`, `none` (
                eliminate duplicate rows).
            maintain_order: Preserve the order of the rows.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ A    ┆ 1    │
        │ X    ┆ 10   │
        │ C    ┆ 3    │
        │ D    ┆ 5    │
        │ M    ┆ 9    │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ null ┆ 20   │
        │ F    ┆ null │
        └──────┴──────┘
        >>>
        >>> tf.unique("a", keep="last")
        >>>
        ┌──────┬──────┐
        │ a    ┆ b    │
        │ ---  ┆ ---  │
        │ str  ┆ i64  │
        ╞══════╪══════╡
        │ D    ┆ 5    │
        │ C    ┆ 3    │
        │ X    ┆ 10   │
        │ A    ┆ 100  │
        │ M    ┆ 50   │
        │ F    ┆ null │
        │ null ┆ 20   │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.unique(
                subset=td_translator._unwrap_tdexpr(subset),
                keep=keep,
                maintain_order=maintain_order,
            ),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def drop_nans(
        self,
        subset: (
            td_typing.ColumnNameOrSelector
            | Collection[td_typing.ColumnNameOrSelector]
            | None
        ) = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Drop rows with `NaN` values.

        Args:
            subset: Columns to look for `Nan` values. If None, all columns are
                considered.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ null ┆ null ┆ null │
        └──────┴──────┴──────┘
        >>>
        >>> tf.unique("a", keep="last")
        ┌─────┬─────┬──────┐
        │ ss  ┆ u   ┆ ff   │
        │ --- ┆ --- ┆ ---  │
        │ str ┆ i64 ┆ f64  │
        ╞═════╪═════╪══════╡
        │ A   ┆ 1   ┆ 1.1  │
        │ B   ┆ 0   ┆ 0.0  │
        │ A   ┆ 2   ┆ 2.2  │
        │ B   ┆ 3   ┆ 3.3  │
        │ B   ┆ 4   ┆ 4.4  │
        │ C   ┆ 5   ┆ -1.1 │
        │ C   ┆ 6   ┆ -2.2 │
        │ C   ┆ 7   ┆ -3.3 │
        │ D   ┆ 8   ┆ inf  │
        └─────┴─────┴──────┘
        >>>
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.drop_nans(subset=td_translator._unwrap_tdexpr(subset)),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # ToDo: ensure system td columns are left unchanged.
    # status(Status.TODO)
    @pydoc(categories="manipulation")
    def drop_nulls(
        self,
        subset: (
            td_typing.ColumnNameOrSelector
            | Collection[td_typing.ColumnNameOrSelector]
            | None
        ) = None,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Drop rows with null values.

        Args:
            subset: Columns to evaluate for null values. If None, all columns are
                considered.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌──────┬──────┬──────┐
        │ ss   ┆ u    ┆ ff   │
        │ ---  ┆ ---  ┆ ---  │
        │ str  ┆ i64  ┆ f64  │
        ╞══════╪══════╪══════╡
        │ A    ┆ 1    ┆ 1.1  │
        │ B    ┆ 0    ┆ 0.0  │
        │ A    ┆ 2    ┆ 2.2  │
        │ B    ┆ 3    ┆ 3.3  │
        │ B    ┆ 4    ┆ 4.4  │
        │ C    ┆ 5    ┆ -1.1 │
        │ C    ┆ 6    ┆ -2.2 │
        │ C    ┆ 7    ┆ -3.3 │
        │ D    ┆ 8    ┆ inf  │
        │ F    ┆ 9    ┆ NaN  │
        │ G    ┆ null ┆ 2.3  │
        └──────┴──────┴──────┘
        >>>
        >>> tf.drop_nulls("a")
        >>>
        ┌─────┬─────┬──────┐
        │ ss  ┆ u   ┆ ff   │
        │ --- ┆ --- ┆ ---  │
        │ str ┆ i64 ┆ f64  │
        ╞═════╪═════╪══════╡
        │ A   ┆ 1   ┆ 1.1  │
        │ B   ┆ 0   ┆ 0.0  │
        │ A   ┆ 2   ┆ 2.2  │
        │ B   ┆ 3   ┆ 3.3  │
        │ B   ┆ 4   ┆ 4.4  │
        │ C   ┆ 5   ┆ -1.1 │
        │ C   ┆ 6   ┆ -2.2 │
        │ C   ┆ 7   ┆ -3.3 │
        │ D   ┆ 8   ┆ inf  │
        │ F   ┆ 9   ┆ NaN  │
        └─────┴─────┴──────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.drop_nulls(subset=td_translator._unwrap_tdexpr(subset)),
            mode="tab",
            idx=self._idx,
        )

    """> Retrieval Functions """

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    @pydoc(categories="filters")
    def filter(
        self,
        *predicates: (
            td_expr.IntoExprColumn
            | Iterable[td_expr.IntoExprColumn]
            | bool
            | list[bool]
            | np.ndarray[Any, Any]
        ),
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Filter the `TableFrame` based on the given predicates.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        │ A   ┆ 100 │
        │ M   ┆ 50  │
        └─────┴─────┘
        >>>
        >>> tf.filter(td.col("a").is_in(["A", "C"]).or_(td.col("b").eq(10)))
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ A   ┆ 100 │
        └─────┴─────┘
        """
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=self._lf.filter(
                *[
                    td_translator._unwrap_into_tdexpr_column(column)
                    for column in predicates
                ],
            ),
            mode="tab",
            idx=self._idx,
        )

    # TODO: should we hide the named_exprs parameter?
    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="projection")
    def select(
        self,
        *exprs: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
        **named_exprs: td_expr.IntoExpr,
    ) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Select column(s) from the `TableFrame`.

        Args:
            exprs: Columns or expressions to select.
            named_exprs: Named expressions to select.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        │ A   ┆ 100 │
        │ M   ┆ 50  │
        └─────┴─────┘
        >>>
        >>> tf.select(td.col("a"), td.col("b").mul(2).alias("bx2"),)
        >>>
        ┌─────┬─────┐
        │ a   ┆ bx2 │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 2   │
        │ X   ┆ 20  │
        │ C   ┆ 6   │
        │ D   ┆ 10  │
        │ M   ┆ 18  │
        │ A   ┆ 200 │
        │ M   ┆ 100 │
        └─────┴─────┘
        """
        source_columns = self._lf.collect_schema().names()
        # noinspection PyProtectedMember
        target_columns = (
            self._lf.select(
                *[td_translator._unwrap_into_tdexpr(column) for column in exprs],
                **named_exprs,
            )
            .collect_schema()
            .names()
        )
        # noinspection PyProtectedMember
        necessary_columns = [
            col
            for col in td_helpers.REQUIRED_COLUMNS
            if col in source_columns and col not in target_columns
        ]
        # noinspection PyProtectedMember
        columns = td_translator._args_to_tuple(
            *[td_translator._unwrap_into_tdexpr(column) for column in exprs],
            necessary_columns,
        )
        return TableFrame.__build__(
            df=_assemble_system_columns(
                self._lf.select(
                    columns,
                    **named_exprs,
                )
            ),
            mode="tab",
            idx=self._idx,
        )

    # ToDo: allways attach system td columns.
    # ToDo: dedicated algorithm for proper provenance handling.
    # ToDo: check for undesired operations of system td columns.
    # ToDo: proper expressions handling.
    # status(Status.TODO)
    @pydoc(categories="aggregation")
    def group_by(
        self,
        *by: td_expr.IntoExpr | Iterable[td_expr.IntoExpr],
    ) -> td_group_by.TableFrameGroupBy:
        # noinspection PyShadowingNames
        """
        Perform a group by on the `TableFrame`.

        Args:
            by: Columns or expressions to group by.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        │ A   ┆ 100 │
        │ M   ┆ 50  │
        └─────┴─────┘
        >>>
        >>> tf.group_by(td.col("a")).agg(td.col("b").sum())
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ M   ┆ 59  │
        │ A   ┆ 101 │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ X   ┆ 10  │
        └─────┴─────┘
        """
        # noinspection PyProtectedMember
        return td_group_by.TableFrameGroupBy(
            self._lf.group_by(
                *[td_translator._unwrap_into_tdexpr(column) for column in by],
                maintain_order=False,
            )
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def slice(self, offset: int, length: int | None = None) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with a slice of the original `TableFrame`

        Args:
            offset: Slice starting index.
            length: The length of the slice. `None` means all the way to the end.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.slice(2,2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        └─────┴─────┘
        """
        return TableFrame.__build__(
            df=self._lf.slice(offset=offset, length=length),
            mode="tab",
            idx=self._idx,
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def limit(self, n: int = 5) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the first `n` rows.
        This is equivalent to `head`.

        Args:
            n: The number of rows to return.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.limit(2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        └─────┴─────┘
        """
        return TableFrame.__build__(
            df=self._lf.limit(n=n),
            mode="tab",
            idx=self._idx,
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def head(self, n: int = 5) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the first `n` rows.

        Args:
            n: The number of rows to return.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.head(2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        └─────┴─────┘
        """
        return TableFrame.__build__(
            df=self._lf.head(n=n),
            mode="tab",
            idx=self._idx,
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def tail(self, n: int = 5) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the last `n` rows.

        Args:
            n: The number of rows to return.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.tail(2)
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        """
        return TableFrame.__build__(
            df=self._lf.tail(n=n),
            mode="tab",
            idx=self._idx,
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def last(self) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the last row.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.last()
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ M   ┆ 9   │
        └─────┴─────┘
        """
        return TableFrame.__build__(
            df=self._lf.last(),
            mode="tab",
            idx=self._idx,
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def first(self) -> TableFrame:
        # noinspection PyShadowingNames
        """
        Return a `TableFrame` with the first row.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.first()
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        └─────┴─────┘
        """
        return TableFrame.__build__(
            df=self._lf.first(),
            mode="tab",
            idx=self._idx,
        )

    # status(Status.DONE)
    @pydoc(categories="filters")
    def last_row(
        self,
        named: bool = False,
    ) -> tuple[Any, ...] | dict[str, Any] | None:
        # noinspection PyShadowingNames
        """
        Return a `tuple` or `dictionary` with the last row, or None if no row.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ A   ┆ B   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ a   ┆ 1   │
        │ b   ┆ 2   │
        │ c   ┆ 3   │
        └─────┴─────┘
        >>>
        >>> tf.last_row()
        >>>
        ('c', 3)
        >>>
        >>> tf.last_row(named=True)
        >>>
        {'A': 'c', 'B': 3}
        """

        df = td_common.drop_system_columns(lf=self._lf.last()).collect()
        if df.is_empty():
            return None
        # noinspection PyTypeChecker
        return df.row(0, named=named)

    # status(Status.DONE)
    @pydoc(categories="filters")
    def first_row(
        self,
        named: bool = False,
    ) -> tuple[Any, ...] | dict[str, Any] | None:
        # noinspection PyShadowingNames
        """
        Return a `tuple` or `dictionary` with the first row, or None if no row.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ A   ┆ B   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ a   ┆ 1   │
        │ b   ┆ 2   │
        │ c   ┆ 3   │
        └─────┴─────┘
        >>>
        >>> tf.last_row()
        >>>
        ('a', 1)
        >>>
        >>> tf.last_row(named=True)
        >>>
        {'A': 'a', 'B': '1'}
        """
        df = td_common.drop_system_columns(lf=self._lf.first()).collect()
        if df.is_empty():
            return None
        # noinspection PyTypeChecker
        return df.row(0, named=named)

    """> Functions Derived from DataFrame """

    @pydoc(categories="projection")
    def item(self) -> Any:
        # noinspection PyShadowingNames
        """
        Returns a scalar value if the TableFrame contains exactly one user column and
        one row.

        Raises an exception if there is more than one user column or more than one row.

        Returns `None` if the TableFrame is empty.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┐
        │ a   │
        │ --- │
        │ str │
        ╞═════╡
        │ A   │
        └─────┘
        >>>
        >>> tf.python_version()
        >>>
        A
        """
        # noinspection PyProtectedMember
        return td_frame.DataFrame.item(td_translator._unwrap_table_frame(self))

    @pydoc(categories="filters")
    def extract_as_rows(self, offset: int, length: int) -> list[dict[str, Any]]:
        """
        Extract a slice of rows from the TableFrame as a list of dictionaries.

        Each dictionary represents one row, where keys are column names
        and values are the corresponding cell values.

        Parameters:
            offset (int): The starting row index of the slice.
            length (int): The number of rows to include in the slice.

        Returns:
            list[dict[str, Any]]: A list of row dictionaries.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.extract_as_rows(offset=0, length=2)
        [
        >>>
        >>> tf.extract_as_rows(offset=0, length=2)
            [
                {"a": "A", "b": 1},
                {"a": "X", "b": 10},
            ]
        """
        # noinspection PyProtectedMember
        return (
            td_translator._unwrap_table_frame(self)
            .slice(offset, length)
            .collect()
            .to_dicts()
        )

    @pydoc(categories="filters")
    def extract_as_columns(self, offset: int, length: int) -> dict[str, list[Any]]:
        """
        Extract a slice of rows from the table as a column-oriented dictionary.

        The result is a mapping of column names to lists of values from the selected rows.

        Parameters:
            offset (int): The starting row index of the slice.
            length (int): The number of rows to include in the slice.

        Returns:
            dict[str, list[Any]]: A dictionary where each key is a column name,
            and its value is a list of values from the selected slice.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ str ┆ i64 │
        ╞═════╪═════╡
        │ A   ┆ 1   │
        │ X   ┆ 10  │
        │ C   ┆ 3   │
        │ D   ┆ 5   │
        │ M   ┆ 9   │
        └─────┴─────┘
        >>>
        >>> tf.extract_as_columns(offset=0, length=2)
            {
                "a": ["A", 1],
                "b": ["X", 10]
            }
        """
        # noinspection PyProtectedMember
        return (
            td_translator._unwrap_table_frame(self)
            .slice(offset, length)
            .collect()
            .to_dict(as_series=False)
        )


TdType = TypeVar("TdType", "TableFrame", "td_typing.Series", "td_expr.Expr")

"""> Internal Private Functions """


def _assemble_system_columns(f: TableFrame | pl.LazyFrame) -> TableFrame:
    if isinstance(f, pl.LazyFrame):
        return TableFrame.__build__(
            df=TableFrameExtension.instance().assemble_system_columns(f),
            mode="tab",
            idx=None,
        )
    elif isinstance(f, TableFrame):
        # noinspection PyProtectedMember
        return TableFrame.__build__(
            df=TableFrameExtension.instance().assemble_system_columns(f._lf),
            mode="tab",
            idx=f._idx,
        )
    else:
        raise TypeError(
            "Expected frame to be of type TableFrame or LazyFrame, but got"
            f" {type(f).__name__} instead."
        )


def _split_columns(columns: list[str]) -> (list[str], list[str]):
    user_columns = [
        column
        for column in columns
        if not column.startswith(td_constants.TD_COLUMN_PREFIX)
    ]
    system_columns = sorted(
        [
            column
            for column in columns
            if column.startswith(td_constants.TD_COLUMN_PREFIX)
        ]
    )
    return user_columns, system_columns


def _arrange_columns(lf: pl.LazyFrame) -> pl.LazyFrame | None:
    if lf is None:
        return None
    user_columns, system_columns = _split_columns(lf.collect_schema().names())
    return lf.select(user_columns + system_columns)


def get_class_methods(cls) -> List[str]:
    methods = [func for func in dir(cls) if callable(getattr(cls, func))]
    methods.sort()
    return methods


def get_missing_methods():
    polars_methods = get_class_methods(pl.LazyFrame)
    tabsdata_methods = get_class_methods(TableFrame)
    tabsdata_all_methods = set(
        tabsdata_methods
        + td_constants.DUPLICATE_METHODS
        + td_constants.FUNCTION_METHODS
        + td_constants.INTERNAL_METHODS
        + td_constants.MATERIALIZE_METHODS
        + td_constants.RENAME_METHODS
        + td_constants.UNNECESSARY_METHODS
        + td_constants.UNRECOMMENDED_METHODS
        + td_constants.UNSUPPORTED_METHODS
        + td_constants.UNSTABLE_METHODS
    )
    diff = list(set(polars_methods) - tabsdata_all_methods)
    # We determine if running inside a pytest test using the standard procedure:
    # https://docs.pytest.org/en/stable/example/simple.html#detect-if-running-from
    # -within-a-pytest-run
    if diff:
        if os.environ.get(td_constants.PYTEST_CONTEXT_ACTIVE) is not None:
            logger.warning(
                "🧨 There are some polars LazyDataFrame methods not available in"
                " TableFrame"
            )
            for polars_method in diff:
                logger.warning(f"   👀 {polars_method}")


def check_polars_api():
    """
    Check polars API.
    """
    if os.environ.get(td_constants.PYTEST_CONTEXT_ACTIVE):
        logger.debug("Available TableFrame methods:")
        for method in get_class_methods(TableFrame):
            logger.debug(f"   {method}")
    get_missing_methods()


# Check polars API changes the first time this module is loaded.
check_polars_api()
