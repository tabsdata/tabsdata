#
# Copyright 2026 Tabsdata Inc.
#

"""run-function runner.

The command's logic, callable without argparse. The package
``__init__`` wraps :func:`run` in the ``@command`` entrypoint for the
console script; the compound imports :func:`run` directly and supplies
pre-built ``RunFunctionArguments``.
"""

from __future__ import annotations

import inspect
import logging
import typing
from pathlib import Path
from typing import Any, Callable, Self

import polars as pl

from tabsdatak._api import _is_array_slot, _is_single_slot, has_transaction_ctx_param
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
from tabsdatak.api import FunctionCtx
from tabsdatak.common._actions.dataquality.record import Check
from tabsdatak.server._tdrunner import dataquality
from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
    TableStats,
)
from tabsdatak.server._tdrunner.function._commons.solver.secrets import (
    ProjectSecret,
    StorageSecrets,
    take_project_secret,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.state import State, StateOutput
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.runners.function.arguments import (
    RunFunctionArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.function.error import (
    RunFunctionErrorCode,
)
from tabsdatak.spi import TableMode

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

    Behaviour comes from :class:`FromRequestCtx`, plus the project
    secret the runner took from the environment.
    """

    _project_secret = ProjectSecret({})

    def with_project_secret(self, project_secret: ProjectSecret) -> Self:
        self._project_secret = project_secret
        return self

    @property
    def secret_names(self) -> list[str]:
        return self._project_secret.names

    def secret_keys(self, name: str) -> list[str]:
        return self._project_secret.keys(name)

    def secret(self, name: str, key: str | None = None) -> str:
        return self._project_secret.value(name, key)


_INFO_BY_KIND: dict[str, type[FunctionInfo]] = {
    KIND_PUBLISHER_DEF: PublisherInfo,
    KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
    KIND_TRANSFORMER_DEF: TransformerInfo,
    KIND_SUBSCRIBER_DEF: SubscriberInfo,
}


def _load_function_def(bundle_folder: Path) -> FunctionInfo:
    """Load the bundle's ``function_def.json`` -- thin wrapper over
    :func:`load_function_def_or_raise` accepting the three kinds
    run-function dispatches on.

    Both bundle-shape failures and unknown-kind failures land on
    ``BadBundleError`` (same exit code) to preserve the pre-refactor
    mapping (the test suite expects that)."""
    return load_function_def_or_raise(
        bundle_folder,
        allowed_kinds=_INFO_BY_KIND,
        bad_bundle_code=RunFunctionErrorCode.RF_2,
        bad_user_code_code=RunFunctionErrorCode.RF_3,
        wrong_kind_code=RunFunctionErrorCode.RF_4,
    )


def _infer_table_modes(func: Callable) -> list[TableMode]:
    """Derive the table-mode (SINGLE / ARRAY) for each input slot
    parameter of the user function from its type annotations.

    The count of returned modes equals the number of *slot*
    parameters: the trailing ``ctx: TransactionCtx`` parameter (if declared)
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
    # The trailing ``ctx: TransactionCtx`` parameter (if declared) is not an
    # input slot -- skip it when inferring slot modes.
    if has_transaction_ctx_param(func) and params:
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


def _scan_input(source: TableFile) -> pl.LazyFrame:
    try:
        return source.scan()
    except Exception as error:
        raise RunFunctionErrorCode.RF_9.exception(
            cause=error, path=source.uri, detail=str(error)
        )


def _build_input(record: InputRecord, *, info: FunctionInfo):
    """Wrap one manifest table record into a `TableFrame`.

    A record with neither `file` nor `location` (a version member with no
    physical data) yields `None`, preserving its position in a
    multi-version slot.
    Publisher inputs are the enriched parquets stamp-inputs already wrote, so
    they keep their system columns as-is (TAB). The transformer / subscriber inputs
    are stored tables whose pristine columns are regenerated for a fresh
    per-function view (MUT), seeded with the record's version metadata and its
    provenance idx (`input_idx`).
    """
    from tabsdatak.tableframe import TableFrame
    from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

    source = TableFile.of(record)
    if source is None:
        return None
    lf = _scan_input(source)
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
        idx=record.input_idx,
        version=record.version(),
    )


def _materialize_input_slot(
    entry: InputEntry,
    *,
    table_mode: TableMode,
    info: FunctionInfo,
):
    """Turn one inputs-manifest entry into the value the user function
    expects for that input parameter.

    SINGLE mode:
      * no records      → `None`
      * one record      → `TableFrame` (or `None` for a record without data)
      * many records    → for a publisher, the files a source plugin
        wrote for one table (a query read in chunks), concatenated into
        one `TableFrame`; for a transformer or subscriber they would be
        several versions of a stored table, a contract violation (a
        SINGLE slot resolves to exactly one table)

    ARRAY mode:
      * `is_empty` entry → `[]`
      * any records      → `list[TableFrame | None]`, one element per
        record in order (members without data stay as `None`)
    """
    records = entry.tables
    if table_mode is TableMode.SINGLE:
        if len(records) == 0:
            return None
        if len(records) == 1:
            return _build_input(records[0], info=info)
        if isinstance(info, (PublisherInfo, StreamPublisherInfo)):
            return _build_chunked_input(records)
        raise RunFunctionErrorCode.RF_11.exception(count=len(records))
    if entry.is_empty:
        return []
    return [_build_input(record, info=info) for record in records]


def _build_chunked_input(records: tuple[InputRecord, ...]):
    """One `TableFrame` over the files a source plugin wrote for one
    publisher input: the chunks are scanned lazily and concatenated,
    widening a column whose type differs between chunks."""
    from tabsdatak.tableframe import TableFrame
    from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

    sources = [
        source for source in (TableFile.of(record) for record in records) if source
    ]
    if not sources:
        return None
    scans = [_scan_input(source) for source in sources]
    return TableFrame._build(pl.concat(scans, how="vertical_relaxed"), CreationMode.TAB)


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
        with StorageSecrets().managed():
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


def _output_targets(
    request: Request,
    info: FunctionInfo,
    output_tables: list[str],
) -> list[TableFile]:
    """Where each output table is written.

    Publisher / transformer outputs are stored tables, written straight
    to the location the request assigns them. Subscriber outputs feed a
    destination plugin, so they land in the run's landing area, one file
    per output position, for the plugin step to read.
    """
    if isinstance(info, SubscriberInfo):
        return [
            TableFile.stored(request.info.landing_file(position))
            for position in range(len(output_tables))
        ]
    locations = {table.table: table.location for table in request.output}
    targets = []
    for table_name in output_tables:
        if table_name not in locations:
            raise RunFunctionErrorCode.RF_20.exception(table=table_name)
        targets.append(TableFile.stored(locations[table_name]))
    return targets


def _sink_outputs(
    result: tuple,
    output_tables: list[str],
    targets: list[TableFile],
    policy: ColumnPolicy,
    *,
    checks: list[Check] | None = None,
    state: StateOutput | None = None,
) -> OutputManifest:
    """Sink each non-None output to its target, copy the run's state to
    its storage location, settle the data-quality checks, and build the
    outputs-manifest entries with the stats read from each written
    file's footer.

    `policy` decides which framework columns the outputs keep: stored
    tables for publishers / transformers, landing files for subscribers,
    which the destination plugin step reshapes before its handoff.

    Every output and the state are written, and every check evaluated,
    in one streaming pass, so the plan the outputs share runs once.
    """
    plans: list[pl.LazyFrame] = []
    written: list[tuple[str, TableFile]] = []
    for table_name, value, target in zip(output_tables, result, targets):
        if value is None:
            continue
        try:
            plans.append(policy.sink(value, target))
        except Exception as error:
            raise RunFunctionErrorCode.RF_16.exception(
                cause=error,
                table=table_name,
                path=target.uri,
                detail=str(error),
            )
        written.append((table_name, target))
    if state is not None:
        try:
            plans.append(state.source.copy(state.target))
        except Exception as error:
            raise RunFunctionErrorCode.RF_16.exception(
                cause=error,
                table=state.name,
                path=state.target.uri,
                detail=str(error),
            )
        written.append((state.name, state.target))
    pending = checks or []
    if plans or pending:
        try:
            results = pl.collect_all(
                [*plans, *(check.verdict for check in pending)],
                engine="streaming",
            )
        except Exception as error:
            raise RunFunctionErrorCode.RF_16.exception(
                cause=error,
                table=", ".join(table_name for table_name, _ in written),
                path=", ".join(target.uri for _, target in written),
                detail=str(error),
            )
        for check, verdict in zip(
            pending,
            results[len(plans) :],
        ):
            check.enforce(verdict)
    entries = {
        table_name: OutputEntry(
            name=table_name, uri=target.uri, stats=_read_stats(table_name, target)
        )
        for table_name, target in written
    }
    return OutputManifest(
        outputs=tuple(
            entries.get(table_name, OutputEntry(name=table_name))
            for table_name in output_tables
        ),
        state=None if state is None else entries[state.name],
    )


def _read_stats(table_name: str, target: TableFile) -> TableStats:
    try:
        return target.stats()
    except Exception as error:
        raise RunFunctionErrorCode.RF_21.exception(
            cause=error,
            table=table_name,
            path=target.uri,
            detail=str(error),
        )


def run(arguments: RunFunctionArguments) -> int:
    project_secret = take_project_secret()
    request_path = arguments.request
    bundle_folder = arguments.bundle_folder
    inputs_manifest_path = arguments.inputs_manifest
    output_folder = arguments.output_folder
    outputs_manifest_path = arguments.outputs_manifest
    state_folder = arguments.state_folder

    logger.info("Running user function")
    request = Request.load(request_path, RunFunctionErrorCode.RF_1)
    info = _load_function_def(bundle_folder)
    logger.debug(
        "Loaded function '%s' (kind=%s) from bundle %s",
        request.info.function,
        type(info).__name__,
        bundle_folder,
    )
    try:
        entries = InputManifest.load(inputs_manifest_path).inputs
    except ManifestError as error:
        raise RunFunctionErrorCode.RF_5.exception(
            cause=error, path=inputs_manifest_path, detail=str(error)
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

    # Make the function's output folder available BEFORE invoking
    # the user function: the FunctionCtx surfaces it as ``work_dir``
    # (per the PluginCtx contract), so a user function that drops a
    # sidecar file there mustn't fail with FileNotFoundError. Also
    # required up-front for ``_sink_outputs`` after the call.
    output_folder.mkdir(parents=True, exist_ok=True)

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

    # If the user function declares a trailing ``ctx: TransactionCtx``
    # parameter, construct a FunctionCtx and hydrate it before
    # invocation. The state run-source-plugin drained on this run wins
    # (publisher chains); otherwise the previous run's state is read
    # where it is stored (transformer / subscriber chains).
    state = State(request, state_folder)
    ctx: _FunctionContext | None = None
    if has_transaction_ctx_param(func):
        ctx = _FunctionContext.from_info(
            output_folder, request.info
        ).with_project_secret(project_secret)
        state.hydrate(ctx, error_code=RunFunctionErrorCode.RF_18, prefer_local=True)
        call_args.append(ctx)
        logger.debug("Hydrated ctx (work_folder=%s)", output_folder)

    output_tables = _output_table_names(info)
    logger.info(
        "Invoking user function with %d positional arg(s); expecting %d output slot(s)",
        len(call_args),
        len(output_tables),
    )
    result = _invoke_user_function(func, call_args, output_tables)
    # Data quality and the sinks below all read the frames the function
    # returned; caching them makes the single streaming pass that writes the
    # outputs compute each frame once, however many tables and checks read it.
    result = tuple(None if frame is None else frame._cached() for frame in result)

    if ctx is not None:
        state.drain(ctx, error_code=RunFunctionErrorCode.RF_19)
        logger.debug("Drained ctx scratch to %s", state.file)

    # Apply the function's data-quality actions over its output tables. In-place
    # actions rewrite an output frame; producing actions yield extra derived
    # tables. Both sink as normal outputs; their catalog type is set at
    # registration. Skipped entirely when no data-quality action is declared, so
    # ordinary runs are unaffected. Derived tables register under their kind's
    # TableType at registration time, not here.
    checks: list[Check] = []
    if dataquality.driver.has_data_quality(getattr(info, "on_tables", None)):
        dq_result = dataquality.driver.run_data_quality(
            collection=request.info.collection,
            output_tables=output_tables,
            output_frames=list(result),
            on_tables=info.on_tables,
        )
        result = list(dq_result.output_frames)
        checks = dq_result.checks
        for derived in dq_result.derived:
            output_tables.append(derived.name)
            result.append(derived.frame)
        logger.info(
            "Data quality produced %d derived table(s): %s",
            len(dq_result.derived),
            [derived.name for derived in dq_result.derived],
        )

    outputs_manifest = _sink_outputs(
        result,
        output_tables,
        _output_targets(request, info, output_tables),
        ColumnPolicy(Zone.LAND if isinstance(info, SubscriberInfo) else Zone.HOST),
        checks=checks,
        state=state.output(),
    )
    outputs = outputs_manifest.outputs
    written = sum(1 for entry in outputs if entry.stats is not None)
    logger.info(
        "User function returned %d slot(s) for tables %s; %d written, %d returned None",
        len(outputs),
        output_tables,
        written,
        len(outputs) - written,
    )
    logger.debug("Outputs manifest entries: %s", outputs)

    try:
        outputs_manifest.write(outputs_manifest_path)
    except OSError as exc:
        raise RunFunctionErrorCode.RF_17.exception(
            cause=exc, path=outputs_manifest_path, detail=str(exc)
        )
    logger.debug("Wrote outputs manifest to %s", outputs_manifest_path)

    return 0
