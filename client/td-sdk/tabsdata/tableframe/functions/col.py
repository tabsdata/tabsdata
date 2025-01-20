#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable

import polars as pl

# noinspection PyProtectedMember
from polars._typing import PolarsDataType, PythonDataType

import tabsdata.tableframe.expr.expr as td_expr

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._common as td_common

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _new_col(
    check: bool,
    name: (
        str
        | PolarsDataType
        | PythonDataType
        | Iterable[str]
        | Iterable[PolarsDataType | PythonDataType]
    ),
    *more_names: str | PolarsDataType | PythonDataType,
) -> td_expr.TdExpr:
    if check:
        td_common.check_columns(name, *more_names)
    return _create_col(name, *more_names)


def _create_col(
    name: (
        str
        | PolarsDataType
        | PythonDataType
        | Iterable[str]
        | Iterable[PolarsDataType | PythonDataType]
    ),
    *more_names: str | PolarsDataType | PythonDataType,
) -> td_expr.TdExpr:
    return td_expr.TdExpr(pl.col(name, *more_names))


class TdCol:
    """
    This class is used to create TableFrame column expressions.

    An instance of this class is available as `col`. It can be called like a function
    (e.g., `td.col("name")`).
    For more information, refer to the `__call__` method documentation.

    This helper class provides an alternative way to create column expressions using
    attribute lookup.
    For instance, `col.name` is equivalent to `col("name")`.  Refer to
    :func:`__getattr__` method.

    Example:
    >>> import tabsdata as td
    >>>
    >>> tf: td.TableFrame ...
    >>>
    >>> tf = tf.with_columns(full_name=(td.col("last_name") + ", " + \
    >>>     td.col("first_name"))
    """

    def __call__(
        self,
        name: (
            str
            | PolarsDataType
            | PythonDataType
            | Iterable[str]
            | Iterable[PolarsDataType | PythonDataType]
        ),
        *more_names: str | PolarsDataType | PythonDataType,
    ) -> td_expr.TdExpr:
        """
        Create a TableFrame column expression.

        Args:
            name: The name or data type of the column(s) to be represented.
                Regular expressions are supported; they should be enclosed by `^` and
                `$`.
            *more_names: Additional column names or data types, provided as
                         positional arguments.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col(["first_name", "last_name"])
        """
        return _new_col(True, name, *more_names)

    def __getattr__(self, name: str) -> td_expr.TdExpr:
        """
        Constructs a column expression using attribute syntax.

        Note: This method exclusively supports referencing a single column by name.

        Args:
            name: The name of the column to be reference.

        Example:
        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf = tf.select(td.col.last_name + ", " + td.col.first_name)
        """
        if name.startswith("__wrapped__"):
            return getattr(type(self), name)
        return _new_col(True, name)


tdcol: TdCol = TdCol()
