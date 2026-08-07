#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from datetime import date, time
from typing import Any, Mapping

from tabsdatak.conn.common.sql.constants import (
    SQL_META_INITIAL_VALUES,
    SQL_META_QUERY,
)
from tabsdatak.spi import TableFileInput, TableFileSpec


def _json_default(value: Any) -> str:
    """Render a bind value ``json`` has no native encoding for --
    dates and times as ISO-8601, anything else (``Decimal``, ``UUID``,
    ...) as its ``str``."""
    if isinstance(value, (date, time)):
        return value.isoformat()
    return str(value)


def sql_metadata(
    query: str,
    initial_values: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Build the metadata a SQL source reports for one query slot: the
    query text and the JSON serialization of the bind values it ran
    with.

    Both entries are always present -- a query with no binds carries an
    empty JSON object -- so the metadata does not change shape from run
    to run. Keys are sorted so equal bind dicts serialize equally.
    """
    return {
        SQL_META_QUERY: query,
        SQL_META_INITIAL_VALUES: json.dumps(
            dict(initial_values or {}),
            sort_keys=True,
            default=_json_default,
        ),
    }


def to_sql_table_file_inputs(
    files: list[list[TableFileSpec]],
    queries: list[str],
    initial_values: Mapping[str, Any] | None,
) -> list[list[TableFileInput]]:
    """:func:`tabsdatak.spi.to_table_file_inputs` for SQL reads: every
    file of slot ``i`` carries the :func:`sql_metadata` of
    ``queries[i]``.

    ``files`` and ``queries`` are positionally aligned -- one slot per
    query -- so a length mismatch means the read broke its contract and
    raises rather than dropping files or queries.
    """
    inputs: list[list[TableFileInput]] = []
    for query, slot in zip(queries, files, strict=True):
        metadata = sql_metadata(query, initial_values)
        inputs.append([TableFileInput(file=file, metadata=metadata) for file in slot])
    return inputs
