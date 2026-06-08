#
# Copyright 2026 Tabs Data Inc.
#

"""run-function command.

The penultimate step of the function-runner chain. Reads the inputs
manifest, materialises each input slot as a polars `LazyFrame` (or a
list of LazyFrames for ARRAY-mode slots), invokes the user-decorated
function, and sinks each returned `LazyFrame` to a parquet under
`--out-dir`. Emits an outputs manifest at `--outputs-manifest` mapping
each declared output table to the parquet it produced (or `null` if
the function returned `None` for that table — "not modified this run").

Kind-agnostic: handles `publisherDef`, `transformerDef`, and
`subscriberDef`. The input slot mode (SINGLE vs ARRAY) is inferred
per-parameter from the user function's type annotations.

For publishers and transformers, output table names come from
`info.output_tables`. Subscribers do not declare output table names
— the count is `dest.cardinality(info.destination)` and the names
are synthesised as `["out_0", "out_1", ...]`. Those synthetic names
are only used internally (parquet filenames + outputs-manifest
entry keys); the destination plugin downstream consumes the
parquets positionally.
"""

from __future__ import annotations

import argparse
import inspect
import typing
from pathlib import Path
from typing import Any, Callable

import polars as pl

from tabsdatak._api import _is_array_slot, _is_single_slot, has_trx_ctx_param
from tabsdatak._connectors import get_dest
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_SUBSCRIBER_DEF,
    KIND_TRANSFORMER_DEF,
    PublisherInfo,
    SubscriberInfo,
    TransformerInfo,
    _unwrap_info,
)
from tabsdatak.server._tdrunner.function._lib.cli import build_parser, command
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.initial_values import (
    drain_ctx_to_state_or_raise,
    hydrate_ctx_from_state_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    load_inputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    write_outputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.run_function.error import (
    RunFunctionErrorCode,
)
from tabsdatak.spi import FunctionCtx, TableMode

# The Info classes this command knows how to invoke. The runner is
# agnostic of *what* the function does — it just needs the call shape
# (input modes, output names/count) which it derives uniformly from
# each Info shape via `_output_table_names`.
FunctionInfo = PublisherInfo | TransformerInfo | SubscriberInfo

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "0.1.0"
INFO = (
    "run-function — invoke the decorated function on the prepared inputs "
    "and sink the returned tables to parquet"
)
MAN = (
    "run-function --request <PATH> --bundle-dir <PATH> "
    "--inputs-manifest <PATH> --out-dir <PATH> "
    "--outputs-manifest <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir, materialises each "
    "input slot from --inputs-manifest as a polars LazyFrame "
    "(single-mode slots → one LazyFrame; array-mode slots → list of "
    "LazyFrames), invokes the user function, and sinks each returned "
    "table to <out-dir>/slot<i>/<table_name>.parquet. Writes an "
    "outputs manifest at --outputs-manifest mapping each output "
    "table to its parquet (or null if the function returned None). "
    "Handles publisherDef, transformerDef, and subscriberDef bundles."
)


def _build_parser():
    parser = build_parser(
        prog="run-function",
        description="Invoke the publisher's user-defined function.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir", required=True, help="Extracted bundle directory"
    )
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help="Path to the YAML inputs manifest produced by stamp-inputs",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory where the function's output parquets are written",
    )
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        help=(
            "Path where this command writes the YAML outputs manifest "
            "mapping each output table to its parquet file (or null if "
            "the function returned None). Consumed by prep-upload-state."
        ),
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help=(
            "Local directory mirroring the one used by "
            "prep-localize-state / run-source-plugin / "
            "prep-upload-state. When the user function declares a "
            "trailing ``ctx: TrxCtx`` parameter, the runtime hydrates "
            "the ctx scratch from <state-dir>/<system_output[0].name>."
            "parquet (falling back to <system_input[0].name>.parquet) "
            "before invoking the function, and drains it back to "
            "<state-dir>/<system_output[0].name>.parquet afterwards. "
            "Ignored when the user function has no ``ctx`` parameter."
        ),
    )
    return parser


class _FunctionContext(FromRequestCtx, FunctionCtx):
    """Concrete :class:`FunctionCtx` for the run-function command.

    All behaviour comes from :class:`FromRequestCtx`; this class
    only fixes the abstract base so ``isinstance(ctx, FunctionCtx)``
    holds at the call boundary.
    """


_INFO_BY_KIND: dict[str, type[FunctionInfo]] = {
    KIND_PUBLISHER_DEF: PublisherInfo,
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
        lf = pl.scan_parquet(path)
        lf.collect_schema()
    except Exception as exc:
        raise RunFunctionErrorCode.RF_9.exception(cause=exc, path=path, detail=str(exc))
    return lf


def _materialise_input_slot(files: list[str], *, table_mode: TableMode):
    """Turn one inputs-manifest entry into the value the user function
    expects for that input parameter.

    SINGLE mode:
      * empty `files` → `None`
      * one file     → `LazyFrame`
      * many files   → contract violation (plugin should have returned
        exactly one for a SINGLE slot)

    ARRAY mode:
      * any files (incl. empty) → `list[LazyFrame]`

    !!! WARNING — INTERIM STUB !!!
    The user-function contract (see `tabsdatak._api`) declares input
    parameters as `TableFrame | None` / `list[TableFrame | None] | None`,
    but this helper currently hands back raw `polars.LazyFrame` because
    `tabsdatak.tableframe.TableFrame` is still an empty placeholder class
    (no real wrapper implementation yet). When the real TableFrame lands
    in tabsdatak, every `LazyFrame` produced here must be wrapped — e.g.
    `return TableFrame(scans[0])` in SINGLE mode and
    `return [TableFrame(s) for s in scans]` in ARRAY mode — and the
    `LazyFrame` mentions above should be replaced with `TableFrame`.

    TODO(T2-migration): wrap LazyFrame outputs in TableFrame once the
    real TableFrame implementation is available in tabsdatak.
    """
    scans = [_scan_parquet_or_fail(Path(f)) for f in files]
    if table_mode is TableMode.SINGLE:
        if len(scans) == 0:
            return None
        if len(scans) > 1:
            raise RunFunctionErrorCode.RF_11.exception(count=len(scans))
        return scans[0]
    # ARRAY mode
    return scans


def _invoke_user_function(func, call_args: list, output_tables: list[str]) -> tuple:
    """Call the user function and validate its return shape.

    Expected return: a tuple with exactly `len(output_tables)` elements.
    Each element must be a `LazyFrame` or `None`.
    """
    try:
        result = func(*call_args)
    except Exception as exc:
        raise RunFunctionErrorCode.RF_12.exception(cause=exc, detail=str(exc))

    if not isinstance(result, tuple):
        raise RunFunctionErrorCode.RF_13.exception(actual_type=type(result).__name__)
    if len(result) != len(output_tables):
        raise RunFunctionErrorCode.RF_14.exception(
            returned=len(result),
            expected=len(output_tables),
            tables=list(output_tables),
        )
    for i, value in enumerate(result):
        if value is not None and not isinstance(value, pl.LazyFrame):
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
) -> list[dict]:
    """Sink each non-None output to `<out_dir>/slot<i>/<table>.parquet`
    and build the outputs-manifest entries."""
    entries: list[dict] = []
    for i, (table_name, value) in enumerate(zip(output_tables, result)):
        if value is None:
            entries.append({
                MANIFEST_NAME_KEY: table_name,
                MANIFEST_FILE_KEY: None,
            })
            continue
        slot_dir = out_dir / f"slot{i}"
        dest = slot_dir / f"{table_name}.parquet"
        try:
            slot_dir.mkdir(parents=True, exist_ok=True)
            value.sink_parquet(dest)
        except Exception as exc:
            raise RunFunctionErrorCode.RF_16.exception(
                cause=exc, table=table_name, path=dest, detail=str(exc)
            )
        entries.append({
            MANIFEST_NAME_KEY: table_name,
            MANIFEST_FILE_KEY: str(dest.absolute()),
        })
    return entries


@command(name="td_run_function", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    inputs_manifest_path = Path(args.inputs_manifest)
    out_dir = Path(args.out_dir)
    outputs_manifest_path = Path(args.outputs_manifest)
    state_dir = Path(args.state_dir)

    request = load_request_or_raise(request_path, RunFunctionErrorCode.RF_1)
    info = _load_function_def(bundle_dir)
    entries = load_inputs_manifest_or_raise(
        inputs_manifest_path, RunFunctionErrorCode.RF_5
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
    # order.
    call_args: list[Any] = []
    for entry, table_mode in zip(entries, table_modes):
        files = entry.get(MANIFEST_FILES_KEY) or []
        call_args.append(_materialise_input_slot(files, table_mode=table_mode))

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

    output_tables = _output_table_names(info)
    result = _invoke_user_function(func, call_args, output_tables)

    if ctx is not None:
        drain_ctx_to_state_or_raise(
            ctx, request, state_dir, error_code=RunFunctionErrorCode.RF_19
        )

    entries_out = _sink_outputs(result, output_tables, out_dir)

    try:
        write_outputs_manifest(entries_out, outputs_manifest_path)
    except OSError as exc:
        raise RunFunctionErrorCode.RF_17.exception(
            cause=exc, path=outputs_manifest_path, detail=str(exc)
        )

    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
