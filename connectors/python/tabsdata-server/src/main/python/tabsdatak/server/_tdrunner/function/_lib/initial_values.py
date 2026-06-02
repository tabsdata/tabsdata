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

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import ConfigDict, TypeAdapter, ValidationError

from tabsdatak.api import ContextKeySpec, ContextValueSpec
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError

if TYPE_CHECKING:
    from tabsdatak.server._tdrunner.function._lib.request import Request, Table
    from tabsdatak.server._tdrunner.function._lib.request_ctx import (
        FromRequestCtx,
    )

logger = logging.getLogger(__name__)

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


# --- ctx-lifecycle helpers (used by run-source-plugin and run-function) -


def hydrate_ctx_from_state(
    ctx: FromRequestCtx,
    request: Request,
    state_dir: Path,
    *,
    exit_code: int,
    prefer_output: bool = False,
) -> None:
    """Hydrate ``ctx``'s scratch store from the relevant on-disk state
    Parquet under ``state_dir``.

    Lookup strategy:

    * ``prefer_output=False`` (run-source-plugin): read from
      ``<state-dir>/<system_input[0].name>.parquet``. This is the
      file ``prep-localize-state`` + ``run-transporter`` materialised
      from the previous run's persisted state. No-op when
      ``request.system_input`` is empty, when the slot has no
      location, or when the file isn't on disk yet (first run).

    * ``prefer_output=True`` (run-function): read
      ``<state-dir>/<system_output[0].name>.parquet`` first --
      ``run-source-plugin`` drains its ctx there on the same run --
      and fall back to the ``system_input[0]`` file when the output
      side hasn't been written (transformer / subscriber chains
      where ``run-source-plugin`` doesn't run).

    Raises:
        CommandSpecificError: if the resolved state file exists but
            doesn't decode as a valid initial_values Parquet. The
            exit code is the command-specific value passed in by the
            caller (``EXIT_BAD_INITIAL_VALUES`` for both commands
            today; the helper does not enforce a specific number).
    """
    state_file = _resolve_state_file(prefer_output, request, state_dir)
    if state_file is None:
        return
    try:
        values = read_initial_values(state_file)
    except InvalidInitialValuesError as exc:
        raise CommandSpecificError(
            exit_code, f"unreadable state-in {state_file}: {exc}"
        ) from exc
    ctx.hydrate(values)


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
    slots: tuple[Table, ...],
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


def drain_ctx_to_state(
    ctx: FromRequestCtx,
    request: Request,
    state_dir: Path,
    *,
    exit_code: int,
) -> None:
    """Persist ``ctx``'s scratch store to
    ``<state-dir>/<system_output[0].name>.parquet``. The state-dir is
    created if it doesn't exist. No-op when ``request.system_output``
    is empty or the slot has no location.

    Raises:
        CommandSpecificError: if any value in the drained dict isn't a
            valid ``ContextValueSpec`` shape.
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
        raise CommandSpecificError(
            exit_code, f"unwritable state-out {state_file}: {exc}"
        ) from exc
