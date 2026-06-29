#
# Copyright 2026 Tabs Data Inc.
#

"""Initial-values one-row Parquet codec.

Wire format for the per-flow scratch-state round-trip: a one-row,
one-column Parquet table whose single column ``user_namespace``
holds the entire scratch dict as a Struct cell. The empty dict
writes an empty file (no columns, no rows), which reads back as
``{}``.

``request.system_input[0]`` carries the previous run's state; the
next run's state gets written to ``request.system_output[0]``. This
module provides the codec only -- which slot to load from / write
to and whether a write is needed are concerns of the caller.

Validation:

  * Keys are plain strings (``ContextKeySpec``).
  * Values must match ``BasicTypeSpec`` -- a single scalar
    (``str`` / ``int`` / ``float`` / ``bool``); lists, dicts and other
    objects are rejected. ``Decimal`` is not a member: a connector that
    needs exact precision serialises it to ``str`` itself (see the CDC
    ``(seg, ord)`` cursor) rather than let it collapse into ``float``.

Bad input is rejected before any I/O: a malformed value never reaches
disk, and a malformed on-disk file never silently round-trips through.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import ConfigDict, TypeAdapter, ValidationError

from tabsdatak.api import BasicTypeSpec, ContextKeySpec
from tabsdatak.error import ErrorCode

if TYPE_CHECKING:
    from tabsdatak.server._tdrunner.function._lib.request import (
        InputTable,
        OutputTable,
        Request,
    )
    from tabsdatak.server._tdrunner.function._lib.request_ctx import (
        FromRequestCtx,
    )

logger = logging.getLogger(__name__)

# Validate keys and values against the canonical type aliases via
# pydantic ``TypeAdapter``. Single source of truth -- if
# ``ContextKeySpec`` / ``BasicTypeSpec`` change shape upstream,
# this validator tracks automatically and no drift is possible
# between what the SPI says and what we accept.
#
# ``strict=True`` disables pydantic's coercion, so ``"1"`` is not
# silently promoted to ``int`` and ``True`` stays a ``bool`` rather than
# matching the ``int`` arm (since ``bool`` is an ``int`` subclass in
# Python). This matters for the round-trip: the value we accept here is
# the same value Parquet stores and round-trips back.
_STRICT = ConfigDict(strict=True)
_KEY_ADAPTER: TypeAdapter[ContextKeySpec] = TypeAdapter(ContextKeySpec, config=_STRICT)
_VALUE_ADAPTER: TypeAdapter[BasicTypeSpec] = TypeAdapter(BasicTypeSpec, config=_STRICT)

# Reserved Parquet column name carrying the entire scratch dict as a
# Struct cell. The user's flat ``set_attr`` / ``get_attr`` keys are
# the fields *inside* that Struct -- they don't appear as Parquet
# columns themselves, so they're free of column-name constraints.
USER_NAMESPACE_COLUMN = "user_namespace"


class InvalidInitialValuesError(ValueError):
    """The persisted initial_values file is malformed or violates schema."""


def _validate_key(key: Any) -> None:
    try:
        _KEY_ADAPTER.validate_python(key)
    except ValidationError as exc:
        raise InvalidInitialValuesError(
            f"key {key!r} is not a valid ContextKeySpec (must be a str)"
        ) from exc


def _validate_value(key: str, value: Any) -> None:
    try:
        _VALUE_ADAPTER.validate_python(value)
    except ValidationError as exc:
        raise InvalidInitialValuesError(
            f"value for key {key!r} is not a valid BasicTypeSpec "
            f"(str/int/float/bool; got type {type(value).__name__!r})"
        ) from exc


def read_initial_values(path: Path) -> dict[str, Any]:
    """Read the one-row, one-column Parquet at ``path`` and return
    the scratch dict carried by the ``user_namespace`` cell.

    An empty file (no columns) reads back as the empty dict ``{}``.

    Raises:
        FileNotFoundError: if ``path`` does not exist.
        InvalidInitialValuesError: if the file is unreadable as
            Parquet, lacks the ``user_namespace`` column, or has
            more than one row.
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
    if df.width != 1 or df.columns[0] != USER_NAMESPACE_COLUMN:
        raise InvalidInitialValuesError(
            f"initial_values parquet {path} must have exactly one column "
            f"named {USER_NAMESPACE_COLUMN!r}; got columns {df.columns!r}"
        )
    if df.height != 1:
        raise InvalidInitialValuesError(
            f"initial_values parquet {path} has {df.height} row(s); expected 1"
        )
    cell = df.row(0, named=True)[USER_NAMESPACE_COLUMN]
    # Polars hands the Struct cell back as a Python dict already.
    return dict(cell) if cell is not None else {}


def write_initial_values(values: dict[str, Any], path: Path) -> None:
    """Persist ``values`` as a one-row, one-column Parquet at ``path``.

    The dict is wrapped in a single ``user_namespace`` Struct cell.
    The empty dict writes an empty Parquet (no columns, no rows).

    The write is **crash-safe**: bytes land first at a sibling
    ``<path>.tmp`` and ``os.replace`` atomically swaps it into place.
    A crash mid-write (Python error, SIGKILL, host reboot) leaves the
    previous content of ``path`` untouched, never a truncated Parquet
    that the next run's ``read_initial_values`` would reject. Bad
    input is rejected before any I/O for the same reason.

    Raises:
        InvalidInitialValuesError: if any key isn't a str or any
            value isn't a BasicTypeSpec shape.
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
            pl.DataFrame({USER_NAMESPACE_COLUMN: [values]}).write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        # Best-effort cleanup of the orphan tmp file on any failure --
        # but never mask the original exception.
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


# --- ctx-lifecycle helpers (used by run-source-plugin and run-function) -


def _resolve_state_file(
    prefer_output: bool,
    request: Request,
    state_dir: Path,
) -> Path | None:
    """Pick which on-disk state Parquet to hydrate from.

    "First non-None wins": try the output-slot file when
    ``prefer_output`` is set, otherwise fall through to the input-slot
    file. Returns ``None`` when neither yields a usable candidate (first
    run, no state declared, file not yet on disk). See
    :func:`hydrate_ctx_from_state` for the per-mode rationale.
    """
    state_file: Path | None = None
    if prefer_output:
        # If state was already drained to the output slot earlier in
        # the same run (run-source-plugin), prefer it -- the function
        # then sees values potentially modified by the source.
        state_file = _state_candidate(request.system_output, state_dir)
    return state_file or _state_candidate(request.system_input, state_dir)


def _state_candidate(
    slots: tuple[InputTable, ...] | tuple[OutputTable, ...],
    state_dir: Path,
) -> Path | None:
    """Return the on-disk state file for ``slots[0]``, or ``None`` if no
    usable candidate exists.

    Debug logs at each negative branch tell us *why* a slot was skipped
    (no slots declared / no location / file not yet on disk) plus a
    happy-path line on the slot that won. Logs are ``DEBUG``-level so
    they stay quiet in normal operation.
    """
    if not slots:
        logger.debug("state lookup: no slots declared")
        return None
    slot = slots[0]
    if slot.location is None:
        logger.debug("state lookup: slot %r has no location", slot.name)
        return None
    candidate = state_dir / f"{slot.name}.parquet"
    if not candidate.is_file():
        logger.debug("state lookup: %s not on disk yet", candidate)
        return None
    logger.debug("state lookup: found %s for slot %r", candidate, slot.name)
    return candidate


def hydrate_ctx_from_state_or_raise(
    ctx: FromRequestCtx,
    request: Request,
    state_dir: Path,
    *,
    error_code: ErrorCode,
    prefer_output: bool = False,
) -> None:
    """Typed-error counterpart of :func:`hydrate_ctx_from_state`.

    Takes the per-command ``error_code`` enum variant instead of an
    integer ``exit_code``. The bound exception template can reference
    ``{state_file}`` and ``{detail}`` placeholders.
    """
    state_file = _resolve_state_file(prefer_output, request, state_dir)
    if state_file is None:
        return
    try:
        values = read_initial_values(state_file)
    except InvalidInitialValuesError as exc:
        raise error_code.exception(cause=exc, state_file=state_file, detail=str(exc))
    ctx.hydrate(values)


def drain_ctx_to_state_or_raise(
    ctx: FromRequestCtx,
    request: Request,
    state_dir: Path,
    *,
    error_code: ErrorCode,
) -> None:
    """Typed-error counterpart of :func:`drain_ctx_to_state`.

    Takes the per-command ``error_code`` enum variant instead of an
    integer ``exit_code``. The bound exception template can reference
    ``{state_file}`` and ``{detail}`` placeholders.
    """
    if not request.system_output:
        return
    slot = request.system_output[0]
    if slot.location is None:
        return
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / f"{slot.name}.parquet"
    try:
        write_initial_values(ctx.drain(), state_file)
    except InvalidInitialValuesError as exc:
        raise error_code.exception(cause=exc, state_file=state_file, detail=str(exc))
