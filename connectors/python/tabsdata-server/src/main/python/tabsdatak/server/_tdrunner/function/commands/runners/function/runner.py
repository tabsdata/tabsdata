#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    KIND_SUBSCRIBER_DEF,
    KIND_TRANSFORMER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.common._actions.dataquality.record import Check
from tabsdatak.server._tdrunner import dataquality
from tabsdatak.server._tdrunner.function._commons.context.function import (
    FunctionContext,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._commons.solver.secrets import (
    ProjectSecret,
    take_project_secret,
)
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    DefinitionErrors,
    FunctionBundle,
    FunctionInfo,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.function import (
    FunctionFailedError,
    InputModeError,
    NotATupleError,
    Output,
    ResultCountError,
    ResultTypeError,
    TypeHintsError,
    UnknownDestinationError,
    UserFunction,
)
from tabsdatak.server._tdrunner.function._handlers.inputs import (
    Inputs,
    InputScanError,
    InputSlot,
    SingleSlotError,
)
from tabsdatak.server._tdrunner.function._handlers.outputs import (
    Outputs,
    SinkError,
    StatsError,
)
from tabsdatak.server._tdrunner.function._handlers.state import (
    State,
    StateReadError,
    StateWriteError,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function.commands.runners.function.error import (
    RunFunctionErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.function.options import (
    RunFunctionArguments,
)
from tabsdatak.spi import TableMode

logger = logging.getLogger(__name__)

KINDS: dict[str, type[FunctionInfo]] = {
    KIND_PUBLISHER_DEF: PublisherInfo,
    KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
    KIND_TRANSFORMER_DEF: TransformerInfo,
    KIND_SUBSCRIBER_DEF: SubscriberInfo,
}
DEFINITION_ERRORS = DefinitionErrors(
    bundle=RunFunctionErrorCode.RF_2,
    user_code=RunFunctionErrorCode.RF_3,
    kind=RunFunctionErrorCode.RF_4,
)


class Function(Command[RunFunctionArguments]):
    request_file: Path
    bundle_folder: Path
    inputs_manifest_file: Path
    output_folder: Path
    outputs_manifest_file: Path
    state_folder: Path
    project_secret: ProjectSecret
    request: Request
    user_function: UserFunction
    entries: tuple[InputEntry, ...]
    table_modes: list[TableMode]
    inputs: Inputs
    state: State
    context: FunctionContext | None
    call_arguments: list[Any]
    output_tables: list[str]
    results: list[Output]
    checks: list[Check]
    outputs_manifest: OutputManifest

    def __init__(self, arguments: RunFunctionArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.bundle_folder = arguments.bundle_folder
        self.inputs_manifest_file = arguments.inputs_manifest
        self.output_folder = arguments.output_folder
        self.outputs_manifest_file = arguments.outputs_manifest
        self.state_folder = arguments.state_folder

    def run(self) -> int:
        self.project_secret = take_project_secret()
        logger.info("Running the user function")
        self.load()
        self.prepare_inputs()
        self.prepare_context()
        self.invoke()
        self.drain()
        self.check_quality()
        self.sink()
        self.write_manifest()
        return 0

    def load(self) -> None:
        self.request = Request.load(self.request_file, RunFunctionErrorCode.RF_1)
        definition = FunctionBundle(self.bundle_folder).definition(
            KINDS,
            DEFINITION_ERRORS,
        )
        logger.debug(
            f"Function '{self.request.info.function}' ({type(definition).__name__}) "
            f"loaded from {self.bundle_folder}"
        )
        self.user_function = UserFunction(definition)
        self.entries = self.load_inputs()
        logger.debug(
            f"Inputs manifest {self.inputs_manifest_file} lists {self.entries}"
        )
        self.table_modes = self.infer_table_modes()
        if len(self.entries) != len(self.table_modes):
            raise RunFunctionErrorCode.RF_10.exception(
                entries=len(self.entries),
                slots=len(self.table_modes),
            )
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def load_inputs(self) -> tuple[InputEntry, ...]:
        path = self.inputs_manifest_file
        try:
            return InputManifest.load(path).inputs
        except ManifestError as error:
            raise RunFunctionErrorCode.RF_5.exception(
                cause=error,
                path=path,
                detail=str(error),
            )

    def infer_table_modes(self) -> list[TableMode]:
        try:
            return self.user_function.table_modes()
        except TypeHintsError as error:
            raise RunFunctionErrorCode.RF_6.exception(
                cause=error,
                detail=error.detail,
            )
        except InputModeError as error:
            raise RunFunctionErrorCode.RF_7.exception(
                cause=error,
                parameter=error.parameter,
            )

    def prepare_inputs(self) -> None:
        self.inputs = Inputs(self.request.info, publisher=self.user_function.publisher)
        self.call_arguments = [
            self.input_slot(entry, mode)
            for entry, mode in zip(self.entries, self.table_modes)
        ]
        logger.debug(
            f"{len(self.call_arguments)} input slot(s) built, with modes "
            f"{[mode.name for mode in self.table_modes]}"
        )

    def input_slot(
        self,
        entry: InputEntry,
        mode: TableMode,
    ) -> InputSlot:
        try:
            return self.inputs.slot(entry, mode)
        except InputScanError as error:
            raise RunFunctionErrorCode.RF_9.exception(
                cause=error,
                path=error.path,
                detail=error.detail,
            )
        except SingleSlotError as error:
            raise RunFunctionErrorCode.RF_11.exception(
                cause=error,
                count=error.count,
            )

    def prepare_context(self) -> None:
        self.state = State(self.request, self.state_folder)
        if not self.user_function.takes_context:
            self.context = None
            return
        context = FunctionContext.from_info(
            self.output_folder,
            self.request.info,
        ).with_project_secret(self.project_secret)
        try:
            self.state.hydrate(context, prefer_local=True)
        except StateReadError as error:
            raise RunFunctionErrorCode.RF_18.exception(
                cause=error,
                state_file=error.uri,
                detail=error.detail,
            )
        self.context = context
        self.call_arguments.append(context)
        logger.debug(f"Function context ready on {self.output_folder}")

    def invoke(self) -> None:
        self.output_tables = self.resolve_output_tables()
        logger.info(
            f"Calling the user function with {len(self.call_arguments)} "
            f"argument(s), for {len(self.output_tables)} output(s)"
        )
        results = self.call()
        self.results = [None if frame is None else frame._cached() for frame in results]

    def resolve_output_tables(self) -> list[str]:
        try:
            return self.user_function.output_tables()
        except UnknownDestinationError as error:
            raise RunFunctionErrorCode.RF_8.exception(
                cause=error,
                destination_type=error.destination_type,
                detail=error.detail,
            )

    def call(self) -> list[Output]:
        try:
            return self.user_function.call(self.call_arguments, self.output_tables)
        except FunctionFailedError as error:
            raise RunFunctionErrorCode.RF_12.exception(
                cause=error,
                detail=error.detail,
            )
        except NotATupleError as error:
            raise RunFunctionErrorCode.RF_13.exception(
                cause=error,
                actual_type=error.actual_type,
            )
        except ResultCountError as error:
            raise RunFunctionErrorCode.RF_14.exception(
                cause=error,
                returned=error.returned,
                expected=len(error.tables),
                tables=error.tables,
            )
        except ResultTypeError as error:
            raise RunFunctionErrorCode.RF_15.exception(
                cause=error,
                slot=error.slot,
                table=error.table,
                actual_type=error.actual_type,
            )

    def drain(self) -> None:
        if self.context is None:
            return
        try:
            self.state.drain(self.context)
        except StateWriteError as error:
            raise RunFunctionErrorCode.RF_19.exception(
                cause=error,
                state_file=error.file,
                detail=error.detail,
            )
        logger.debug(f"State drained to {self.state.file}")

    def check_quality(self) -> None:
        self.checks = []
        on_tables = self.user_function.on_tables
        if not dataquality.driver.has_data_quality(on_tables):
            return
        quality = dataquality.driver.run_data_quality(
            collection=self.request.info.collection,
            output_tables=self.output_tables,
            output_frames=self.results,
            on_tables=on_tables,
        )
        self.results = list(quality.output_frames)
        self.checks = quality.checks
        for derived in quality.derived:
            self.output_tables.append(derived.name)
            self.results.append(derived.frame)
        logger.info(
            f"Data quality derived {len(quality.derived)} table(s): "
            f"{[derived.name for derived in quality.derived]}"
        )

    def sink(self) -> None:
        if self.user_function.subscriber:
            outputs = Outputs(ColumnPolicy(Zone.LAND))
        else:
            outputs = Outputs(ColumnPolicy(Zone.HOST))
        targets = self.targets()
        try:
            self.outputs_manifest = outputs.sink(
                self.results,
                self.output_tables,
                targets,
                checks=self.checks,
                state=None if self.user_function.subscriber else self.state.output(),
            )
        except SinkError as error:
            raise RunFunctionErrorCode.RF_16.exception(
                cause=error,
                table=error.table,
                path=error.path,
                detail=error.detail,
            )
        except StatsError as error:
            raise RunFunctionErrorCode.RF_21.exception(
                cause=error,
                table=error.table,
                path=error.path,
                detail=error.detail,
            )
        entries = self.outputs_manifest.outputs
        written = sum(1 for entry in entries if entry.stats is not None)
        logger.info(
            f"User function returned {len(entries)} output(s) for "
            f"{self.output_tables}: {written} written, "
            f"{len(entries) - written} None"
        )
        logger.debug(f"Outputs manifest entries: {entries}")

    def targets(self) -> list[TableFile]:
        if self.user_function.subscriber:
            return [
                TableFile.stored(self.request.info.landing_file(position))
                for position in range(len(self.output_tables))
            ]
        locations = {table.table: table.location for table in self.request.output}
        targets = []
        for table in self.output_tables:
            if table not in locations:
                raise RunFunctionErrorCode.RF_20.exception(table=table)
            targets.append(TableFile.stored(locations[table]))
        return targets

    def write_manifest(self) -> None:
        path = self.outputs_manifest_file
        try:
            self.outputs_manifest.write(path)
        except OSError as error:
            raise RunFunctionErrorCode.RF_17.exception(
                cause=error,
                path=path,
                detail=str(error),
            )
        logger.debug(f"Outputs manifest written to {path}")
