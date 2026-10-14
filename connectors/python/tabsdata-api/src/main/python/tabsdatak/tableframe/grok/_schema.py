#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pydantic import ConfigDict

from tabsdatak import _api
from tabsdatak.tableframe.datatypes import ColumnDataType, String
from tabsdatak.tableframe.grok.error import GrokErrorCode

_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)


@_api.dataclass(GrokErrorCode.GROK_1, config=_ARBITRARY)
class ColumnDefinition:
    """The column one Grok capture produces: its name and data type.

    :categories: string, projection

    One of these per capture makes up the `schema` passed to
    `TableFrame.grok`, keyed by capture name. The captured text is parsed
    into `dtype`; a row that does not match leaves the column null.

    Args:
        name: Name of the column to add. `None` (the default) keeps the
            capture's own name, and any other value renames it.
        dtype: Data type the captured text is parsed into, from
            `tabsdatak.tableframe.datatypes` -- a type (`Int64`) or an
            instance where it is parameterized (`Datetime("us")`).
            Defaults to `String`, which keeps the text as captured.

    Examples:
        .. code-block:: python

            tf.grok(
                "logs",
                r"%{WORD:user}-%{INT:year}",
                {
                    "user": ColumnDefinition("user", String),
                    "year": ColumnDefinition("year", Int64),
                },
            )

        A `name` that differs from the capture renames the column it
        produces -- here the `user` capture lands in a column called
        `who`:

        .. code-block:: python

            tf.grok(
                "logs",
                r"%{WORD:user}-.*",
                {"user": ColumnDefinition("who", String)},
            )
    """

    name: str | None = None
    dtype: ColumnDataType = String
