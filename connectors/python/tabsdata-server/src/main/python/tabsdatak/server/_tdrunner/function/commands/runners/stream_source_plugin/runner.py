#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from tabsdatak._connectors import get_stream_src
from tabsdatak._info import KIND_STREAM_PUBLISHER_DEF, StreamPublisherInfo, _unwrap_info
from tabsdatak.common._storage.transfer import TransferError
from tabsdatak.extensions.variant.version import version as tabsdata_version
from tabsdatak.server._tdrunner.function._commons.context.stream import (
    StreamSourceContext,
)
from tabsdatak.server._tdrunner.function._commons.document.connection import (
    ConnectionDocument,
    InvalidConnectionError,
)
from tabsdatak.server._tdrunner.function._commons.document.journal import Journal
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    StreamerRequest,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import is_retriable
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._commons.run.shutdown import (
    is_shutdown_requested,
    listen_for_shutdown,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import Stage
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    DefinitionErrors,
    FunctionBundle,
)
from tabsdatak.server._tdrunner.function._handlers.batch import StageBatch
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.error import (  # noqa: E501
    RunStreamSourcePluginErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.stream_source_plugin.options import (  # noqa: E501
    RunStreamSourcePluginArguments,
)
from tabsdatak.spi import Conn, StreamSrcDef, StreamSrcPlugin

logger = logging.getLogger(__name__)

KINDS = {KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo}
DEFINITION_ERRORS = DefinitionErrors(
    bundle=RunStreamSourcePluginErrorCode.RSSP_2,
    user_code=RunStreamSourcePluginErrorCode.RSSP_3,
    kind=RunStreamSourcePluginErrorCode.RSSP_4,
)


class StreamSourcePlugin(Command[RunStreamSourcePluginArguments]):
    request_file: Path
    bundle_folder: Path
    context_folder: Path
    connection_file: Path
    journal_file: Path
    request: StreamerRequest
    publisher: StreamPublisherInfo
    definition: StreamSrcDef
    plugin: StreamSrcPlugin
    connection: Conn
    context: StreamSourceContext

    def __init__(self, arguments: RunStreamSourcePluginArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.bundle_folder = arguments.bundle_folder
        self.context_folder = arguments.context_folder
        self.connection_file = arguments.connection_file
        self.journal_file = arguments.journal_file

    def run(self) -> int:
        listen_for_shutdown(self.shutdown)
        logger.info("Starting the stream source plugin")
        self.load()
        if is_shutdown_requested():
            logger.info(
                "Not starting the plugin, as a stop was requested while loading"
            )
        else:
            self.stream()
        self.record_end()
        self.report_completion()
        return 0

    def load(self) -> None:
        self.context_folder.mkdir(parents=True, exist_ok=True)
        self.request = StreamerRequest.load(
            self.request_file,
            RunStreamSourcePluginErrorCode.RSSP_1,
        )
        self.publisher = FunctionBundle(self.bundle_folder).definition(
            KINDS,
            DEFINITION_ERRORS,
        )
        self.definition = self.resolve_definition()
        self.plugin = self.definition.plugin()
        self.connection = self.load_connection()
        logger.debug(
            f"Stream publisher {self.function_name} reads "
            f"{type(self.publisher.source).__name__} through "
            f"{type(self.plugin).__name__}"
        )
        staging = self.open_stage()
        self.context = StreamSourceContext.from_info(
            self.context_folder,
            self.request.info,
            staging=staging,
            batch=StageBatch(
                staging, self.definition.cardinality(self.publisher.source)
            ),
            plugin=type(self.plugin),
            thresholds=self.publisher.thresholds,
            tabsdata_version=tabsdata_version(),
        )

    @property
    def function_name(self) -> str:
        function = _unwrap_info(self.publisher.func)
        return getattr(function, "__name__", repr(function))

    def resolve_definition(self) -> StreamSrcDef:
        source_type = type(self.publisher.source)
        try:
            return get_stream_src(source_type)
        except KeyError as error:
            raise RunStreamSourcePluginErrorCode.RSSP_5.exception(
                cause=error,
                src_type=source_type.__name__,
                detail=str(error),
            )

    def load_connection(self) -> Conn:
        try:
            return ConnectionDocument(self.connection_file).load()
        except InvalidConnectionError as error:
            raise RunStreamSourcePluginErrorCode.RSSP_9.exception(
                cause=error,
                path=error.path,
                detail=error.detail,
            )

    def open_stage(self) -> Stage:
        function_stage = self.request.info.function_stage
        try:
            return Stage(function_stage)
        except TransferError as error:
            raise RunStreamSourcePluginErrorCode.RSSP_7.exception(
                cause=error,
                uri=function_stage.uri,
                scheme=urlparse(function_stage.uri).scheme,
            )

    def shutdown(self) -> None:
        if hasattr(self, "context"):
            self.context.deactivate()

    def stream(self) -> None:
        thresholds = self.publisher.thresholds
        if self.publisher.flush_interval_mins is not None:
            logger.warning(
                f"The stream publisher {self.function_name} names "
                f"flush_interval_mins={self.publisher.flush_interval_mins}, which "
                f"is deprecated and taken as batch_max_secs="
                f"{thresholds.batch.max_secs}; set batch_max_secs instead"
            )
        logger.info(
            f"Running {type(self.plugin).__name__} for {self.function_name} on "
            f"{self.context_folder}, staging to {self.context.staging.uri} with "
            f"{thresholds}"
        )
        try:
            self.plugin.run(self.context, self.connection, self.publisher.source)
        except Exception as error:
            raise RunStreamSourcePluginErrorCode.RSSP_6.exception(
                cause=error, detail=str(error)
            )
        finally:
            self.unstage()
        logger.info("The stream source plugin returned")

    def unstage(self) -> None:
        pending = sum(len(files) for files in self.context.pending)
        if pending == 0:
            return
        logger.warning(
            f"The plugin left {pending} table file(s) sent and not published; "
            f"unstaging them"
        )
        try:
            self.context.unstage()
        except Exception as error:
            logger.warning(f"Cannot unstage what the plugin left: {error}")

    def record_end(self) -> None:
        end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        journal = Journal.load(self.journal_file, strict=True)
        journal.end = end
        try:
            journal.save(self.journal_file)
        except OSError as error:
            raise RunStreamSourcePluginErrorCode.RSSP_8.exception(
                cause=error,
                path=self.journal_file,
                detail=str(error),
            )
        logger.info(f"Recorded the end at {end}")

    def report_completion(self) -> None:
        try:
            self.responder.done()
        except Exception as error:
            if not (is_shutdown_requested() and is_retriable(error)):
                raise
            logger.warning(f"Cannot report completion while shutting down: {error}")
            return
        logger.info("Reported completion")
