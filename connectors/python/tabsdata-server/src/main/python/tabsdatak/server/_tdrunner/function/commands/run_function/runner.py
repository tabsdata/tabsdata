#
# Copyright 2026 Tabs Data Inc.
#

"""run-function runner.

The command's logic, callable without argparse. The package
``__init__`` wraps :func:`run` in the ``@command`` entrypoint for the
console script; the compound imports :func:`run` directly and supplies
a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import inspect
import logging
import typing
from pathlib import Path
from typing import Any, Callable

import polars as pl

from tabsdatak._api import _is_array_slot, _is_single_slot, has_trx_ctx_param
from tabsdatak._connectors import get_dest
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    KIND_SUBSCRIBER_DEF,
    KIND_TRANSFORMER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    TransformerInfo,
    _unwrap_info,
)
from tabsdatak.server._tdrunner.function._lib import (
    inputs_manifest,
    outputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.initial_values import (
    drain_ctx_to_state_or_raise,
    hydrate_ctx_from_state_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.run_function.error import (
    RunFunctionErrorCode,
)
from tabsdatak.spi import FunctionCtx, TableMode

logger = logging.getLogger(__name__)

# The Info classes this command knows how to invoke. The runner is
# agnostic of *what* the function does — it just needs the call shape
# (input modes, output names/count) which it derives uniformly from
# each Info shape via `_output_table_names`.
#
# StreamPublisherInfo shares Publisher's call shape: by the time this
# command runs, the inputs have already been materialised on disk by
# `run_source_plugin` (which substitutes the user-declared
# StreamSrc/StreamSrcConn for the publisher-side StreamStageSrc +
# ManifestStageSrcPlugin). The user function is invoked exactly the
# same way; only the upstream input-production differs.
FunctionInfo = PublisherInfo | StreamPublisherInfo | TransformerInfo | SubscriberInfo


class _FunctionContext(FromRequestCtx, FunctionCtx):
    """Concrete :class:`FunctionCtx` for the run-function command.

    All behaviour comes from :class:`FromRequestCtx`; this class
    only fixes the abstract base so ``isinstance(ctx, FunctionCtx)``
    holds at the call boundary.
    """


_INFO_BY_KIND: dict[str, type[FunctionInfo]] = {
    KIND_PUBLISHER_DEF: PublisherInfo,
    KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
    KIND_TRANSFORMER_DEF: TransformerInfo,
    KIND_SUBSCRIBER_DEF: SubscriberInfo,
}


def _load_function_def(bundle_dir: Path) -> FunctionInfo:
    """Load the bundle's ``function_def.json`` -- thin wrapper over
    :func:`load_function_def_or_raise` accepting the three kinds
    run-function dispatches on.

    Both bundle-shape failures and unknown-kind failures land on
    ``BadBundleError`` (same exit code) to preserve the pre-refactor
    mapping (the test suite expects that)."""
    return load_function_def_or_raise(
        bundle_dir,
        allowed_kinds=_INFO_BY_KIND,
        bad_bundle_code=RunFunctionErrorCode.RF_2,
        bad_user_code_code=RunFunctionErrorCode.RF_3,
        wrong_kind_code=RunFunctionErrorCode.RF_4,
    )


def _infer_table_modes(func: Callable) -> list[TableMode]:
    """Derive the table-mode (SINGLE / ARRAY) for each input slot
    parameter of the user function from its type annotations.

    The count of returned modes equals the number of *slot*
    parameters: the trailing ``ctx: TrxCtx`` parameter (if declared)
    is stripped before iteration because it isn't an input slot.
    ``@publisher`` / ``@transformer`` / ``@subscriber`` validate at
    decoration time that the function has exactly the expected slot
    parameters plus the optional ctx, so iterating the remaining
    params here is correct for all three kinds.

    Each slot parameter must be annotated as either
    ``TableFrameSpec`` (single-mode) or ``TableFramesSpec``
    (array-mode). Because the decorator's
    ``_check_callable_signature`` already enforces that, the
    BadUserCodeError branches below are defensive fallbacks for
    runtime invariant violations rather than realistic user errors.
    """
    try:
        hints = typing.get_type_hints(func)
    except Exception as exc:
        raise RunFunctionErrorCode.RF_6.exception(cause=exc, detail=str(exc))
    params = list(inspect.signature(func).parameters.values())
    # The trailing ``ctx: TrxCtx`` parameter (if declared) is not an
    # input slot -- skip it when inferring slot modes.
    if has_trx_ctx_param(func) and params:
        params = params[:-1]
    modes: list[TableMode] = []
    for p in params:
        hint = hints.get(p.name)
        if hint is not None and _is_single_slot(hint):
            modes.append(TableMode.SINGLE)
        elif hint is not None and _is_array_slot(hint):
            modes.append(TableMode.ARRAY)
        else:
            raise RunFunctionErrorCode.RF_7.exception(param=p.name)
    return modes


def _output_table_names(info: FunctionInfo) -> list[str]:
    """Return the output table names for this function.

    Publishers and transformers declare these explicitly via
    `info.output_tables`. Subscribers don't declare names — output
    count is `dest.cardinality(info.destination)` and names are
    synthesised as `["out_0", "out_1", ...]`. The synthetic names
    only flow through internal artifacts (parquet filenames and the
    outputs manifest); the destination plugin downstream consumes
    the parquet list positionally and doesn't depend on the names.

    Raises:
        NoPluginError: the subscriber's destination class has no
            registered DestDef (the connector package isn't
            installed in the runtime).
    """
    if isinstance(info, SubscriberInfo):
        try:
            dest_def = get_dest(type(info.destination))
        except KeyError as exc:
            raise RunFunctionErrorCode.RF_8.exception(
                cause=exc, dest_type=type(info.destination).__name__, detail=str(exc)
            )
        n = dest_def.cardinality(info.destination)
        return [f"out_{i}" for i in range(n)]
    return list(info.output_tables)


def _scan_parquet_or_fail(path: Path) -> pl.LazyFrame:
    """Open `path` as a `pl.LazyFrame`, validating the file eagerly.

    `pl.scan_parquet` itself is lazy and won't touch the file until
    you ask for its schema or collect — so a bad parquet would
    silently slip past us and only blow up inside the user function.
    We fetch the schema here as a cheap header read to surface
    corruption / wrong-format failures with the right exit code at
    the right layer.
    """
    try:
        # [TODO: tabsdatak.tableframe scan and sink migration] revisit when
        # tabsdatak.tableframe ships scan and sink methods.
        lf = pl.scan_parquet(path)
        lf.collect_schema()
    except Exception as exc:
        raise RunFunctionErrorCode.RF_9.exception(cause=exc, path=path, detail=str(exc))
    return lf


def _input_version(record: dict):
    """Build a `DataVersion` from a manifest table record's version fields."""
    from tabsdatak.tableframe._engine.model.frame import DataVersion

    execution_id = record.get(inputs_manifest.MANIFEST_EXECUTION_ID_KEY)
    transaction_id = record.get(inputs_manifest.MANIFEST_TRANSACTION_ID_KEY)
    table_data_version_id = record.get(inputs_manifest.MANIFEST_VERSION_ID_KEY)
    triggered_on = record.get(inputs_manifest.MANIFEST_TRIGGERED_ON_KEY)
    if (
        execution_id is None
        or transaction_id is None
        or table_data_version_id is None
        or triggered_on is None
    ):
        return None
    return (
        DataVersion
        .builder()
        .with_execution(str(execution_id))
        .with_transaction(str(transaction_id))
        .with_version(str(table_data_version_id))
        .with_timestamp(triggered_on)
        .build()
    )


def _build_input(record: dict, *, info: FunctionInfo):
    """Wrap one manifest table record into a `TableFrame`.

    A record with a null `file` (a version member with no physical data)
    yields `None`, preserving its position in a multi-version slot.
    Publisher inputs are the enriched parquets stamp-inputs already wrote, so
    they keep their system columns as-is (TAB). The transformer / subscriber inputs
    are stored tables whose pristine columns are regenerated for a fresh
    per-function view (MUT), seeded with the record's version metadata and its
    provenance idx (`input_idx`).
    """
    from tabsdatak.tableframe import TableFrame
    from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

    file = record.get(inputs_manifest.MANIFEST_FILE_KEY)
    if file is None:
        return None
    lf = _scan_parquet_or_fail(Path(file))
    if isinstance(info, (PublisherInfo, StreamPublisherInfo)):
        # Stream-publisher inputs are also publisher-side: the
        # ``stamp-inputs`` step (0070) wrote enriched parquets with
        # the framework's system columns, just like a regular
        # publisher's. TAB mode preserves those columns instead of
        # regenerating them via MUT (which is the right choice for
        # transformer / subscriber inputs that come from stored
        # tables).
        return TableFrame._build(lf, CreationMode.TAB)
    return TableFrame._build(
        lf,
        CreationMode.MUT,
        idx=record.get(inputs_manifest.MANIFEST_INPUT_IDX_KEY),
        version=_input_version(record),
    )


def _materialize_input_slot(
    entry: dict,
    *,
    table_mode: TableMode,
    info: FunctionInfo,
):
    """Turn one inputs-manifest entry into the value the user function
    expects for that input parameter.

    SINGLE mode:
      * no records      → `None`
      * one record      → `TableFrame` (or `None` for a null `file`)
      * many records    → contract violation (a SINGLE slot resolves to
        exactly one table)

    ARRAY mode:
      * `is_empty` entry → `[]`
      * any records      → `list[TableFrame | None]`, one element per
        record in order (null-`file` members stay as `None`)
    """
    records = entry.get(inputs_manifest.MANIFEST_TABLES_KEY) or []
    if table_mode is TableMode.SINGLE:
        if len(records) == 0:
            return None
        if len(records) > 1:
            raise RunFunctionErrorCode.RF_11.exception(count=len(records))
        return _build_input(records[0], info=info)
    if entry.get(inputs_manifest.MANIFEST_IS_EMPTY_KEY):
        return []
    return [_build_input(record, info=info) for record in records]


def _invoke_user_function(func, call_args: list, output_tables: list[str]) -> tuple:
    """Call the user function and validate its return shape.

    Expected return: a tuple with exactly `len(output_tables)` elements.
    Each element must be a `TableFrame` or `None`.

    Single-output convenience: when the function declares exactly one
    output slot, the lone value may be returned bare, without the
    enclosing 1-tuple -- it is wrapped here so the downstream length /
    element checks see the canonical tuple shape. This mirrors the
    signature relaxation in ``tabsdatak._api.check_callable_signature``.
    """
    from tabsdatak.tableframe import TableFrame

    try:
        result = func(*call_args)
    except Exception as exc:
        raise RunFunctionErrorCode.RF_12.exception(cause=exc, detail=str(exc))

    if not isinstance(result, tuple):
        if len(output_tables) == 1:
            result = (result,)
        else:
            raise RunFunctionErrorCode.RF_13.exception(
                actual_type=type(result).__name__
            )
    if len(result) != len(output_tables):
        raise RunFunctionErrorCode.RF_14.exception(
            returned=len(result),
            expected=len(output_tables),
            tables=list(output_tables),
        )
    for i, value in enumerate(result):
        if value is not None and not isinstance(value, TableFrame):
            raise RunFunctionErrorCode.RF_15.exception(
                slot=i,
                table=output_tables[i],
                actual_type=type(value).__name__,
            )
    return result


def _sink_outputs(
    result: tuple,
    output_tables: list[str],
    out_dir: Path,
    *,
    public: bool,
) -> list[dict]:
    """Sink each non-None output to `<out_dir>/slot<i>/<table>.parquet`
    and build the outputs-manifest entries.

    Publisher / transformer outputs become stored tables, so they sink
    natively with the framework's persisted system columns. Subscriber
    outputs feed a destination plugin and leave the system, so they sink
    publicly: stripped of every `$td.` system and `@td.` connector
    column, leaving only the user's own columns for the plugin to write.
    """
    entries: list[dict] = []
    for i, (table_name, value) in enumerate(zip(output_tables, result)):
        if value is None:
            entries.append({
                outputs_manifest.MANIFEST_NAME_KEY: table_name,
                outputs_manifest.MANIFEST_FILE_KEY: None,
            })
            continue
        slot_dir = out_dir / f"slot{i}"
        dest = slot_dir / f"{table_name}.parquet"
        try:
            slot_dir.mkdir(parents=True, exist_ok=True)
            # [TODO: tabsdatak.tableframe scan and sink migration] revisit when
            # tabsdatak.tableframe ships scan and sink methods.
            if public:
                value._sink_public(dest)
            else:
                value._sink_native(dest)
        except Exception as exc:
            raise RunFunctionErrorCode.RF_16.exception(
                cause=exc, table=table_name, path=dest, detail=str(exc)
            )
        entries.append({
            outputs_manifest.MANIFEST_NAME_KEY: table_name,
            outputs_manifest.MANIFEST_FILE_KEY: str(dest.absolute()),
        })
    return entries


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    inputs_manifest_path = Path(args.inputs_manifest)
    out_dir = Path(args.out_dir)
    outputs_manifest_path = Path(args.outputs_manifest)
    state_dir = Path(args.state_dir)

    logger.info("Running user function")
    request = load_request_or_raise(request_path, RunFunctionErrorCode.RF_1)
    info = _load_function_def(bundle_dir)
    logger.debug(
        "Loaded function '%s' (kind=%s) from bundle %s",
        request.info.function,
        type(info).__name__,
        bundle_dir,
    )
    entries = inputs_manifest.load_inputs_manifest_or_raise(
        inputs_manifest_path, RunFunctionErrorCode.RF_5
    )
    logger.debug(
        "Inputs manifest has %d slot(s): %s; manifest path %s",
        len(entries),
        entries,
        inputs_manifest_path,
    )

    func = _unwrap_info(info.func)
    table_modes = _infer_table_modes(func)
    if len(entries) != len(table_modes):
        raise RunFunctionErrorCode.RF_10.exception(
            entries=len(entries), slots=len(table_modes)
        )

    # Make the function's output directory available BEFORE invoking
    # the user function: the FunctionCtx surfaces it as ``work_dir``
    # (per the PluginCtx contract), so a user function that drops a
    # sidecar file there mustn't fail with FileNotFoundError. Also
    # required up-front for ``_sink_outputs`` after the call.
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build the positional args for the user function from the
    # inputs manifest. Manifest order mirrors function parameter
    # order; each entry carries the per-table records (file, provenance
    # idx and version metadata) the materialization needs, so no
    # correlation with the request is required.
    call_args: list[Any] = []
    for entry, table_mode in zip(entries, table_modes):
        call_args.append(
            _materialize_input_slot(entry, table_mode=table_mode, info=info)
        )
    logger.debug(
        "Materialised %d input slot(s) with modes %s",
        len(call_args),
        [m.name if hasattr(m, "name") else str(m) for m in table_modes],
    )

    # If the user function declares a trailing ``ctx: TrxCtx``
    # parameter, construct a FunctionCtx and hydrate it from the
    # state-dir before invocation. Preferring the state-out file
    # (written by run-source-plugin's drain on the same run) over
    # the state-in file handles publisher chains; falling back to
    # state-in handles transformer / subscriber chains where
    # run-source-plugin does not run.
    ctx: _FunctionContext | None = None
    if has_trx_ctx_param(func):
        ctx = _FunctionContext(
            work_dir=out_dir,
            exec_plan_id=str(request.info.execution_id),
            trx_id=str(request.info.transaction_id),
            exec_plan_ts=request.info.triggered_on_iso,
            collection_id=str(request.info.collection_id),
            collection_name=request.info.collection,
            function_id=str(request.info.function_run_id),
            function_name=request.info.function,
        )
        hydrate_ctx_from_state_or_raise(
            ctx,
            request,
            state_dir,
            error_code=RunFunctionErrorCode.RF_18,
            prefer_output=True,
        )
        call_args.append(ctx)
        logger.debug("Hydrated ctx from %s (work_dir=%s)", state_dir, out_dir)

    output_tables = _output_table_names(info)
    logger.info(
        "Invoking user function with %d positional arg(s); expecting %d output slot(s)",
        len(call_args),
        len(output_tables),
    )
    result = _invoke_user_function(func, call_args, output_tables)

    if ctx is not None:
        drain_ctx_to_state_or_raise(
            ctx, request, state_dir, error_code=RunFunctionErrorCode.RF_19
        )
        logger.debug("Drained ctx scratch to %s", state_dir)

    entries_out = _sink_outputs(
        result,
        output_tables,
        out_dir,
        public=isinstance(info, SubscriberInfo),
    )
    written = sum(
        1 for e in entries_out if e.get(outputs_manifest.MANIFEST_FILE_KEY) is not None
    )
    logger.info(
        "User function returned %d slot(s) for tables %s; %d written, %d returned None",
        len(entries_out),
        output_tables,
        written,
        len(entries_out) - written,
    )
    logger.debug("Outputs manifest entries: %s", entries_out)

    try:
        outputs_manifest.write_outputs_manifest(entries_out, outputs_manifest_path)
    except OSError as exc:
        raise RunFunctionErrorCode.RF_17.exception(
            cause=exc, path=outputs_manifest_path, detail=str(exc)
        )
    logger.debug("Wrote outputs manifest to %s", outputs_manifest_path)

    return 0
