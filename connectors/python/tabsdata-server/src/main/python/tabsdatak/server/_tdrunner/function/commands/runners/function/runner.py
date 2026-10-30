#
# Copyright 2026 Tabsdata Inc.
#

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
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionDefinitionError,
    FunctionInfo,
    InvalidDefinitionError,
    UnexpectedKindError,
    load_definition,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.landing import LandedInputs
from tabsdatak.server._tdrunner.function._handlers.state import State, StateOutput
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.runners.function.error import (
    RunFunctionErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.function.options import (
    RunFunctionArguments,
)
from tabsdatak.spi import TableMode
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

logger = logging.getLogger(__name__)

SUBSCRIBER_OUTPUT_PREFIX = "out_"

DEFINITION_BY_KIND: dict[str, type[FunctionInfo]] = {
    KIND_PUBLISHER_DEF: PublisherInfo,
    KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
    KIND_TRANSFORMER_DEF: TransformerInfo,
    KIND_SUBSCRIBER_DEF: SubscriberInfo,
}

Input = TableFrame | None
InputSlot = Input | list[Input]


class FunctionContext(FromRequestCtx, FunctionCtx):
    _project_secret = ProjectSecret({})

    def with_project_secret(self, project_secret: ProjectSecret) -> Self:
        self._project_secret = project_secret
        return self

    @property
    def secret_names(self) -> list[str]:
        return self._project_secret.names

    def secret_keys(self, name: str) -> list[str]:
        return self._project_secret.keys(name)

    def secret(
        self,
        name: str,
        key: str | None = None,
    ) -> str:
        return self._project_secret.value(name, key)


def load_function_definition(bundle_folder: Path) -> FunctionInfo:
    try:
        return load_definition(bundle_folder, DEFINITION_BY_KIND)
    except UnexpectedKindError as error:
        raise RunFunctionErrorCode.RF_4.exception(
            cause=error,
            kind=error.kind,
            manifest=error.manifest,
            allowed=error.allowed,
        )
    except InvalidDefinitionError as error:
        raise RunFunctionErrorCode.RF_3.exception(
            cause=error,
            bundle_folder=bundle_folder,
            detail=error.detail,
        )
    except FunctionDefinitionError as error:
        raise RunFunctionErrorCode.RF_2.exception(
            cause=error,
            bundle_folder=bundle_folder,
            detail=error.detail,
        )


def load_inputs(path: Path) -> tuple[InputEntry, ...]:
    try:
        return InputManifest.load(path).inputs
    except ManifestError as error:
        raise RunFunctionErrorCode.RF_5.exception(
            cause=error,
            path=path,
            detail=str(error),
        )


def infer_table_modes(function: Callable) -> list[TableMode]:
    try:
        hints = typing.get_type_hints(function)
    except Exception as error:
        raise RunFunctionErrorCode.RF_6.exception(cause=error, detail=str(error))
    parameters = list(inspect.signature(function).parameters.values())
    if has_transaction_ctx_param(function) and parameters:
        parameters = parameters[:-1]
    modes: list[TableMode] = []
    for parameter in parameters:
        hint = hints.get(parameter.name)
        if hint is not None and _is_single_slot(hint):
            modes.append(TableMode.SINGLE)
        elif hint is not None and _is_array_slot(hint):
            modes.append(TableMode.ARRAY)
        else:
            raise RunFunctionErrorCode.RF_7.exception(parameter=parameter.name)
    return modes


def output_table_names(definition: FunctionInfo) -> list[str]:
    if isinstance(definition, SubscriberInfo):
        try:
            destination = get_dest(type(definition.destination))
        except KeyError as error:
            raise RunFunctionErrorCode.RF_8.exception(
                cause=error,
                destination_type=type(definition.destination).__name__,
                detail=str(error),
            )
        count = destination.cardinality(definition.destination)
        return [f"{SUBSCRIBER_OUTPUT_PREFIX}{position}" for position in range(count)]
    return list(definition.output_tables)


def is_publisher(definition: FunctionInfo) -> bool:
    return isinstance(definition, (PublisherInfo, StreamPublisherInfo))


def scan_input(source: TableFile) -> pl.LazyFrame:
    try:
        return source.scan()
    except Exception as error:
        raise RunFunctionErrorCode.RF_9.exception(
            cause=error,
            path=source.uri,
            detail=str(error),
        )


def scan_record(
    record: InputRecord,
    landed: LandedInputs,
) -> pl.LazyFrame | None:
    if record.schema is not None:
        try:
            return landed.scan(record)
        except Exception as error:
            raise RunFunctionErrorCode.RF_9.exception(
                cause=error,
                path=record.location.uri if record.location else record.schema.uri,
                detail=str(error),
            )
    source = TableFile.of(record)
    if source is None:
        return None
    return scan_input(source)


def build_input(
    record: InputRecord,
    *,
    definition: FunctionInfo,
    landed: LandedInputs,
) -> Input:
    frame = scan_record(record, landed)
    if frame is None:
        return None
    if is_publisher(definition):
        return TableFrame._build(frame, CreationMode.TAB)
    return TableFrame._build(
        frame,
        CreationMode.MUT,
        idx=record.input_idx,
        version=record.version(),
    )


def build_chunked_input(
    records: tuple[InputRecord, ...],
    landed: LandedInputs,
) -> Input:
    scans = [
        frame
        for frame in (scan_record(record, landed) for record in records)
        if frame is not None
    ]
    if not scans:
        return None
    return TableFrame._build(pl.concat(scans, how="vertical_relaxed"), CreationMode.TAB)


def materialize_input_slot(
    entry: InputEntry,
    *,
    table_mode: TableMode,
    definition: FunctionInfo,
    landed: LandedInputs,
) -> InputSlot:
    records = entry.tables
    if table_mode is TableMode.SINGLE:
        if len(records) == 0:
            return None
        if len(records) == 1:
            return build_input(
                records[0],
                definition=definition,
                landed=landed,
            )
        if is_publisher(definition):
            return build_chunked_input(records, landed)
        raise RunFunctionErrorCode.RF_11.exception(count=len(records))
    if entry.is_empty:
        return []
    return [
        build_input(
            record,
            definition=definition,
            landed=landed,
        )
        for record in records
    ]


def invoke_user_function(
    function: Callable,
    call_arguments: list[Any],
    output_tables: list[str],
) -> list[Input]:
    try:
        with StorageSecrets().managed():
            result = function(*call_arguments)
    except Exception as error:
        raise RunFunctionErrorCode.RF_12.exception(cause=error, detail=str(error))
    if not isinstance(result, tuple):
        if len(output_tables) != 1:
            raise RunFunctionErrorCode.RF_13.exception(
                actual_type=type(result).__name__
            )
        result = (result,)
    if len(result) != len(output_tables):
        raise RunFunctionErrorCode.RF_14.exception(
            returned=len(result),
            expected=len(output_tables),
            tables=list(output_tables),
        )
    for position, value in enumerate(result):
        if value is not None and not isinstance(value, TableFrame):
            raise RunFunctionErrorCode.RF_15.exception(
                slot=position,
                table=output_tables[position],
                actual_type=type(value).__name__,
            )
    return list(result)


def output_targets(
    request: Request,
    definition: FunctionInfo,
    output_tables: list[str],
) -> list[TableFile]:
    if isinstance(definition, SubscriberInfo):
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


def read_stats(table_name: str, target: TableFile) -> TableStats:
    try:
        return target.stats()
    except Exception as error:
        raise RunFunctionErrorCode.RF_21.exception(
            cause=error,
            table=table_name,
            path=target.uri,
            detail=str(error),
        )


def sink_outputs(
    result: list[Input],
    output_tables: list[str],
    targets: list[TableFile],
    policy: ColumnPolicy,
    *,
    checks: list[Check] | None = None,
    state: StateOutput | None = None,
) -> OutputManifest:
    plans: list[pl.LazyFrame] = []
    written: list[tuple[str, TableFile]] = []
    for table_name, value, target in zip(
        output_tables,
        result,
        targets,
    ):
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
        for check, verdict in zip(pending, results[len(plans) :]):
            check.enforce(verdict)
    entries = {
        table_name: OutputEntry(
            name=table_name,
            uri=target.uri,
            stats=read_stats(table_name, target),
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


def write_outputs_manifest(manifest: OutputManifest, path: Path) -> None:
    try:
        manifest.write(path)
    except OSError as error:
        raise RunFunctionErrorCode.RF_17.exception(
            cause=error,
            path=path,
            detail=str(error),
        )


def run(arguments: RunFunctionArguments) -> int:
    project_secret = take_project_secret()

    logger.info("Running the user function")
    request = Request.load(arguments.request, RunFunctionErrorCode.RF_1)
    definition = load_function_definition(arguments.bundle_folder)
    logger.debug(
        f"Function '{request.info.function}' ({type(definition).__name__}) "
        f"loaded from {arguments.bundle_folder}"
    )
    entries = load_inputs(arguments.inputs_manifest)
    logger.debug(f"Inputs manifest {arguments.inputs_manifest} lists {entries}")

    function = _unwrap_info(definition.func)
    table_modes = infer_table_modes(function)
    if len(entries) != len(table_modes):
        raise RunFunctionErrorCode.RF_10.exception(
            entries=len(entries),
            slots=len(table_modes),
        )

    arguments.output_folder.mkdir(parents=True, exist_ok=True)

    landed = LandedInputs(request.info)
    call_arguments: list[Any] = [
        materialize_input_slot(
            entry,
            table_mode=table_mode,
            definition=definition,
            landed=landed,
        )
        for entry, table_mode in zip(entries, table_modes)
    ]
    logger.debug(
        f"{len(call_arguments)} input slot(s) built, with modes "
        f"{[mode.name for mode in table_modes]}"
    )

    state = State(request, arguments.state_folder)
    context = (
        FunctionContext.from_info(
            arguments.output_folder,
            request.info,
        ).with_project_secret(project_secret)
        if has_transaction_ctx_param(function)
        else None
    )
    if context is not None:
        state.hydrate(
            context,
            error_code=RunFunctionErrorCode.RF_18,
            prefer_local=True,
        )
        call_arguments.append(context)
        logger.debug(f"Function context ready on {arguments.output_folder}")

    output_tables = output_table_names(definition)
    logger.info(
        f"Calling the user function with {len(call_arguments)} argument(s), "
        f"for {len(output_tables)} output(s)"
    )
    result = invoke_user_function(
        function,
        call_arguments,
        output_tables,
    )
    result = [None if frame is None else frame._cached() for frame in result]

    if context is not None:
        state.drain(context, error_code=RunFunctionErrorCode.RF_19)
        logger.debug(f"State drained to {state.file}")

    checks: list[Check] = []
    on_tables = None if isinstance(definition, SubscriberInfo) else definition.on_tables
    if dataquality.driver.has_data_quality(on_tables):
        quality = dataquality.driver.run_data_quality(
            collection=request.info.collection,
            output_tables=output_tables,
            output_frames=result,
            on_tables=on_tables,
        )
        result = list(quality.output_frames)
        checks = quality.checks
        for derived in quality.derived:
            output_tables.append(derived.name)
            result.append(derived.frame)
        logger.info(
            f"Data quality derived {len(quality.derived)} table(s): "
            f"{[derived.name for derived in quality.derived]}"
        )

    outputs_manifest = sink_outputs(
        result,
        output_tables,
        output_targets(
            request,
            definition,
            output_tables,
        ),
        ColumnPolicy(
            Zone.LAND if isinstance(definition, SubscriberInfo) else Zone.HOST
        ),
        checks=checks,
        state=state.output(),
    )
    outputs = outputs_manifest.outputs
    written = sum(1 for entry in outputs if entry.stats is not None)
    logger.info(
        f"User function returned {len(outputs)} output(s) for {output_tables}: "
        f"{written} written, {len(outputs) - written} None"
    )
    logger.debug(f"Outputs manifest entries: {outputs}")

    write_outputs_manifest(outputs_manifest, arguments.outputs_manifest)
    logger.debug(f"Outputs manifest written to {arguments.outputs_manifest}")
    return 0
