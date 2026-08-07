#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TypeAlias

from polars.datatypes import DataTypeClass
from pydantic import ConfigDict

from tabsdatak import _api
from tabsdatak.tableframe.datatypes import DataType, String
from tabsdatak.tableframe.grok.error import GrokErrorCode

_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)

ColumnDataType: TypeAlias = DataType | DataTypeClass


@_api.dataclass(GrokErrorCode.GROK_1, config=_ARBITRARY)
class ColumnDefinition:
    """A single column definition in a :class:`TableFrame`.

    :categories: tableframe

    Args:
        name: The name of the column, or `None` when no name is given.
        dtype: The data type of the column, given either as a type (`td.Int64`)
            or as an instance (`td.Datetime("us")`). This determines how values
            in the column are interpreted, validated and serialized.

    Examples:
        Create a column with a name and a type:

        .. code-block:: python

            ColumnDefinition(name="customer_id", dtype=td.Int64)

        Use columns to define a TableFrame schema:

        .. code-block:: python

            schema = [
                ColumnDefinition(name="customer_id", dtype=td.Int64),
                ColumnDefinition(name="signup_date", dtype=td.Datetime),
            ]
    """

    name: str | None = None
    dtype: ColumnDataType = String
