#
# Copyright 2026 Tabs Data Inc.
#

"""Initial-values one-row Parquet codec.

Wire format for the per-flow scratch-state round-trip: every entry in
the dict becomes one column of a one-row Parquet table (column name =
key, cell = value). The empty dict writes an empty file (no columns, no
rows), which reads back as ``{}``.

`request.system_input[0]` carries the previous run's dict; the next
run's dict gets written to `request.system_output[0]`. This module
provides the codec only -- which slot to load from / write to and
whether a write is needed are concerns of the caller.

Validation:

  * Every key must match the ``ContextKeySpec`` regex
    (dotted-identifier, max 253 chars, segments 1-63 chars each).
  * Every value must be a ``ContextValueSpec`` shape: ``str``, ``int``,
    ``float``, ``bool``, ``list[str]``, or ``dict[str, str]``.

Bad input is rejected before any I/O: a malformed value never reaches
disk, and a malformed on-disk file never silently round-trips through.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import ConfigDict, TypeAdapter, ValidationError

from tabsdatak.api import ContextKeySpec, ContextValueSpec

# Validate keys and values against the canonical type aliases via
# pydantic ``TypeAdapter`` rather than re-deriving the rules here.
# Single source of truth -- if ``ContextKeySpec`` /
# ``ContextValueSpec`` change shape upstream (a tightened regex,
# a new union arm), this validator tracks automatically and no
# drift is possible between what the type says and what we accept.
#
# ``strict=True`` disables pydantic's coercion, so ``"1"`` is not
# silently promoted to ``int`` and ``True`` stays a ``bool`` rather
# than matching the ``int`` arm (since ``bool`` is an ``int`` subclass
# in Python). Both matter for the round-trip: the value we accept
# here is the same value Parquet stores and round-trips back.
#
# ``regex_engine='python-re'`` is required for ``ContextKeySpec``:
# the spec's regex uses a lookahead (``(?=.{1,253}$)`` for the
# total-length cap), which pydantic's default Rust regex engine
# rejects with "look-around ... is not supported". Python's stdlib
# ``re`` module handles it natively.
_STRICT = ConfigDict(strict=True, regex_engine="python-re")
_KEY_ADAPTER: TypeAdapter[ContextKeySpec] = TypeAdapter(ContextKeySpec, config=_STRICT)
_VALUE_ADAPTER: TypeAdapter[ContextValueSpec] = TypeAdapter(
    ContextValueSpec, config=_STRICT
)


class InvalidInitialValuesError(ValueError):
    """The persisted initial_values file is malformed or violates schema."""


def _validate_key(key: Any) -> None:
    try:
        _KEY_ADAPTER.validate_python(key)
    except ValidationError as exc:
        raise InvalidInitialValuesError(
            f"key {key!r} does not match ContextKeySpec pattern"
        ) from exc


def _validate_value(key: str, value: Any) -> None:
    try:
        _VALUE_ADAPTER.validate_python(value)
    except ValidationError as exc:
        raise InvalidInitialValuesError(
            f"value for key {key!r} is not a valid ContextValueSpec shape "
            f"(got type {type(value).__name__!r})"
        ) from exc


def read_initial_values(path: Path) -> dict[str, Any]:
    """Read the one-row Parquet at ``path`` and return its dict.

    An empty file (no columns) reads back as the empty dict ``{}``.

    Raises:
        FileNotFoundError: if ``path`` does not exist.
        InvalidInitialValuesError: if the file is unreadable as
            Parquet, has more than one row, or any column name fails
            ContextKeySpec validation.
    """
    if not path.is_file():
        raise FileNotFoundError(f"initial_values file not found: {path}")
    try:
        df = pl.read_parquet(path)
    except Exception as exc:
        raise InvalidInitialValuesError(
            f"could not read initial_values parquet {path}: {exc}"
        ) from exc
    if df.width == 0:
        return {}
    if df.height != 1:
        raise InvalidInitialValuesError(
            f"initial_values parquet {path} has {df.height} row(s); expected 1"
        )
    for col in df.columns:
        _validate_key(col)
    return dict(df.row(0, named=True))


def write_initial_values(values: dict[str, Any], path: Path) -> None:
    """Persist ``values`` as a one-row Parquet at ``path``.

    The empty dict writes an empty Parquet (no columns, no rows).

    The write is **crash-safe**: bytes land first at a sibling
    ``<path>.tmp`` and ``os.replace`` atomically swaps it into place.
    A crash mid-write (Python error, SIGKILL, host reboot) leaves the
    previous content of ``path`` untouched, never a truncated Parquet
    that the next run's ``read_initial_values`` would reject. Bad
    input is rejected before any I/O for the same reason.

    Raises:
        InvalidInitialValuesError: if any key fails ContextKeySpec
            validation or any value isn't a ContextValueSpec shape.
        OSError: on write errors (missing parent dir, permission, etc.).
    """
    for key, value in values.items():
        _validate_key(key)
        _validate_value(key, value)
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        if not values:
            pl.DataFrame().write_parquet(tmp_path)
        else:
            pl.DataFrame({k: [v] for k, v in values.items()}).write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        # Best-effort cleanup of the orphan tmp file on any failure --
        # but never mask the original exception.
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise
