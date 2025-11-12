#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._common as td_common
import tabsdata.tableframe.expr.expr as td_expr

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing
from tabsdata._utils.tableframe._constants import SysCol

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _new_col(
    check: bool,
    name: (
        str
        | SysCol
        | td_typing.DataType
        | td_typing.PythonDataType
        | Iterable[str | SysCol]
        | Iterable[td_typing.DataType | td_typing.PythonDataType]
    ),
    *more_names: str | SysCol | td_typing.DataType | td_typing.PythonDataType,
) -> td_expr.Expr:
    if check:
        td_common.check_columns(name, *more_names)
    return _create_col(name, *more_names)


def _create_col(
    name: (
        str
        | SysCol
        | td_typing.DataType
        | td_typing.PythonDataType
        | Iterable[str | SysCol]
        | Iterable[td_typing.DataType | td_typing.PythonDataType]
    ),
    *more_names: str | SysCol | td_typing.DataType | td_typing.PythonDataType,
) -> td_expr.Expr:
    return td_expr.Expr(pl.col(name, *more_names))


class Col:
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
            | SysCol
            | td_typing.DataType
            | td_typing.PythonDataType
            | Iterable[str | SysCol]
            | Iterable[td_typing.DataType | td_typing.PythonDataType]
        ),
        *more_names: str | SysCol | td_typing.DataType | td_typing.PythonDataType,
    ) -> td_expr.Expr:
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

    def __getattr__(self, name: str) -> td_expr.Expr:
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


@dataclass(frozen=True, slots=True)
class Column:
    # noinspection PyProtectedMember
    """
    Represents a single column definition in a :class:`TableFrame`.

    A `Column` defines both the **name** and the **data type** of a column,
    along with other relevant metadata in the future. This abstraction provides
    a consistent way to declare and validate schema definitions for `TableFrame`
    objects.

    Parameters
    ----------
    name : str | None, optional
        The name of the column. Must be a valid string identifier for the
        `TableFrame` schema, or None if no name is provided. Defaults to None.
    dtype : td_typing.DataType
        The expected data type for the column. This determines how values
        in the column will be interpreted, validated, and serialized.

    Attributes
    ----------
    name : str | None
        The name of the column, or None if no name was provided.
    dtype : td_typing.DataType
        The declared data type of the column.

    Examples
    --------
    Create a column with a name and type:

    >>> import tabsdata as td
    >>> Column("customer_id", td.Int64)
    <Column name='customer_id' dtype=Int64>

    Use columns to define a TableFrame schema:

    >>> import tabsdata as td
    >>> schema = [
    ...     Column("customer_id", td.Int64),
    ...     Column("signup_date", td.Datetime),
    ... ]
    >>> for column in schema:
    ...     print(column.name, column.dtype)
    customer_id Int64
    signup_date Datetime
    """

    name: str | None = field(default=None)
    dtype: td_typing.DataType = field(default=pl.String)


col: Col = Col()
