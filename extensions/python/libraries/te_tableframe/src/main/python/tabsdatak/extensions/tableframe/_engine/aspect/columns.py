#
# Copyright 2026 Tabsdata Inc.
#

"""System-column guard as before hooks over the generated TableFrame.

The standard system columns ($td.*) are managed by the engine. A user operation
must not create, overwrite, or drop one: the guard inspects the op arguments
before the op runs and raises when an output name, rename target, or drop target
is a $td.* column. Reading a system column (for example ``select(col("$td.id"))``)
is allowed -- only producing or removing one is rejected.
"""

from __future__ import annotations

from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_SYSTEM_COLUMN_PREFIX,
)


def guard(lazyframes, op, views, params):
    offending = _offending(op, params)
    if offending:
        from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

        raise TableFrameErrorCode.TF6.exception(op=op, columns=sorted(offending))
    return lazyframes


def _offending(op, params) -> set[str]:
    if op in ("select", "with_columns"):
        return _written_system(params["exprs"], params["named"])
    if op == "rename":
        return _system_renames(params["mapping"])
    if op == "drop":
        return _system(_drop_names(params["columns"]))
    if op == "udf":
        return _system(params["outputs"])
    return set()


def _system(names) -> set[str]:
    return {name for name in names if name.startswith(TD_SYSTEM_COLUMN_PREFIX)}


def _written_system(positional, named) -> set[str]:
    """System columns a projection would create or overwrite. A plain read of a
    system column (``col("$td.id")``) passes it through unchanged and is allowed;
    any other expression with a $td.* output counts, including one that
    transforms the column itself (``col("$td.src").cast(...)``)."""
    written: set[str] = set()
    for name, expression in _named_pairs(named):
        if _writes_system(name, expression):
            written.add(name)
    for expression in _flatten(positional):
        name = _output_name(expression)
        if name is not None and _writes_system(name, expression):
            written.add(name)
    return written


def _writes_system(output_name: str, expression) -> bool:
    if not output_name.startswith(TD_SYSTEM_COLUMN_PREFIX):
        return False
    return not _reads(output_name, expression)


def _reads(name: str, expression) -> bool:
    if isinstance(expression, str):
        return expression == name
    meta = getattr(expression, "meta", None)
    if meta is None:
        return False
    try:
        bare = meta.is_column_selection(allow_aliasing=True)
    except Exception:
        return False
    return bare and _root_names(expression) == [name]


def _named_pairs(named) -> list[tuple[str, object]]:
    return list(named.items())


def _drop_names(columns) -> list[str]:
    names: list[str] = []
    for column in _flatten(columns):
        name = _output_name(column)
        if name is not None:
            names.append(name)
    return names


def _system_renames(mapping) -> set[str]:
    """The $td.* columns a rename would move: renaming one away is a drop, and
    renaming onto one an overwrite."""
    if not isinstance(mapping, dict):
        return set()
    return {
        name
        for old, new in mapping.items()
        if old != new
        for name in (old, new)
        if name.startswith(TD_SYSTEM_COLUMN_PREFIX)
    }


def _flatten(values) -> list:
    flat: list = []
    for value in values:
        if isinstance(value, (list, tuple)):
            flat += list(value)
        else:
            flat.append(value)
    return flat


def _output_name(value) -> str | None:
    if isinstance(value, str):
        return value
    meta = getattr(value, "meta", None)
    if meta is None:
        return None
    try:
        return meta.output_name()
    except Exception:
        # A multi-output expression comes from a selection by pattern, which
        # never includes the $td.* columns (functions.col leaves them out), so it
        # needs no guarding here.
        return None


def _root_names(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    meta = getattr(value, "meta", None)
    if meta is None:
        return []
    try:
        return list(meta.root_names())
    except Exception:
        return []
