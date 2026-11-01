#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak._api import warn_legacy_src
from tabsdatak._handoff import SourceContract
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
)
from tabsdatak.server._tdrunner.function._commons.context.source import SourceContext
from tabsdatak.server._tdrunner.function._commons.document.connection import (
    InvalidConnectionError,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    DefinitionErrors,
    FunctionBundle,
)
from tabsdatak.server._tdrunner.function._handlers.binding import (
    Publisher,
    SourceBinding,
    UnknownSourceError,
    UnsupportedStageError,
)
from tabsdatak.server._tdrunner.function._handlers.handoff import (
    Handoff,
    InputCountError,
    SourceHandoff,
)
from tabsdatak.server._tdrunner.function._handlers.landing import LandedFile
from tabsdatak.server._tdrunner.function._handlers.state import (
    State,
    StateReadError,
    StateWriteError,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.error import (
    RunSourcePluginErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.options import (  # noqa: E501
    RunSourcePluginArguments,
)

logger = logging.getLogger(__name__)

KINDS = {
    KIND_PUBLISHER_DEF: PublisherInfo,
    KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
}
DEFINITION_ERRORS = DefinitionErrors(
    bundle=RunSourcePluginErrorCode.RSP_2,
    user_code=RunSourcePluginErrorCode.RSP_3,
    kind=RunSourcePluginErrorCode.RSP_4,
)


class SourcePlugin(Command[RunSourcePluginArguments]):
    request_file: Path
    bundle_folder: Path
    connection_file: Path
    output_folder: Path
    state_folder: Path
    inputs_manifest_file: Path
    request: Request
    publisher: Publisher
    binding: SourceBinding
    context: SourceContext
    state: State
    handoff: SourceHandoff
    names: list[str]
    landed: list[list[LandedFile]]

    def __init__(self, arguments: RunSourcePluginArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.bundle_folder = arguments.bundle_folder
        self.connection_file = arguments.connection_file
        self.output_folder = arguments.output_folder
        self.state_folder = arguments.state_folder
        self.inputs_manifest_file = arguments.inputs_manifest

    def run(self) -> int:
        logger.info("Running the source plugin")
        self.load()
        self.hydrate()
        self.land()
        self.drain()
        self.write_manifest()
        return 0

    def load(self) -> None:
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.request = Request.load(self.request_file, RunSourcePluginErrorCode.RSP_1)
        self.publisher = FunctionBundle(self.bundle_folder).definition(
            KINDS,
            DEFINITION_ERRORS,
        )
        self.binding = self.bind()
        logger.debug(
            f"Source plugin {type(self.binding.plugin).__name__} "
            f"for connection {type(self.binding.connection).__name__} "
            f"({type(self.publisher).__name__}, "
            f"cardinality {self.binding.cardinality})"
        )
        self.context = SourceContext.from_info(self.output_folder, self.request.info)
        self.state = State(self.request, self.state_folder)

    def bind(self) -> SourceBinding:
        try:
            return SourceBinding.of(
                self.publisher,
                self.request.info.function_stage,
                self.connection_file,
            )
        except UnknownSourceError as error:
            raise RunSourcePluginErrorCode.RSP_5.exception(
                cause=error,
                source_type=error.source_type,
                detail=error.detail,
            )
        except UnsupportedStageError as error:
            raise RunSourcePluginErrorCode.RSP_11.exception(
                cause=error,
                uri=error.uri,
                scheme=error.scheme,
            )
        except InvalidConnectionError as error:
            raise RunSourcePluginErrorCode.RSP_12.exception(
                cause=error,
                path=error.path,
                detail=error.detail,
            )

    def hydrate(self) -> None:
        try:
            self.state.hydrate(self.context)
        except StateReadError as error:
            raise RunSourcePluginErrorCode.RSP_9.exception(
                cause=error,
                state_file=error.uri,
                detail=error.detail,
            )
        logger.debug(f"Source context ready on {self.output_folder}")

    def land(self) -> None:
        plugin = self.binding.plugin
        self.handoff = Handoff.of(plugin, self.output_folder)
        if self.handoff.legacy and SourceContract(type(plugin)).deprecated():
            logger.warning(warn_legacy_src(type(plugin)))
        self.names = self.binding.input_names(self.publisher)
        slots = self.handoff.seed(
            self.request.info,
            self.names,
            variable=self.binding.variable,
        )
        logger.info(f"Landing {len(slots)} input(s)")
        self.landed = self.write_in()
        logger.info(
            f"Source plugin landed {sum(len(files) for files in self.landed)} file(s)"
        )

    def write_in(self) -> list[list[LandedFile]]:
        try:
            self.handoff.grow(
                self.binding.plugin,
                self.context,
                self.binding.connection,
                self.binding.source,
            )
        except InputCountError as error:
            raise RunSourcePluginErrorCode.RSP_7.exception(
                cause=error,
                returned=error.returned,
                expected=error.expected,
            )
        except Exception as error:
            raise RunSourcePluginErrorCode.RSP_6.exception(
                cause=error,
                detail=str(error),
            )
        return self.handoff.reap()

    def drain(self) -> None:
        try:
            self.state.drain(self.context)
        except StateWriteError as error:
            raise RunSourcePluginErrorCode.RSP_10.exception(
                cause=error,
                state_file=error.file,
                detail=error.detail,
            )
        logger.debug(f"State drained to {self.state.file}")

    def write_manifest(self) -> None:
        manifest = InputManifest.of(
            self.names,
            [[file.record() for file in files] for files in self.landed],
        )
        path = self.inputs_manifest_file
        try:
            manifest.write(path)
        except OSError as error:
            raise RunSourcePluginErrorCode.RSP_8.exception(
                cause=error,
                path=path,
                detail=str(error),
            )
        logger.info(f"Inputs manifest written to {path}")
