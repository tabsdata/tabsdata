#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from tabsdatak._api import warn_legacy_dest
from tabsdatak._connectors import get_dest
from tabsdatak._handoff import DestinationContract
from tabsdatak._info import KIND_SUBSCRIBER_DEF, SubscriberInfo
from tabsdatak.server._tdrunner.function._commons.context.destination import (
    DestinationContext,
)
from tabsdatak.server._tdrunner.function._commons.document.connection import (
    ConnectionDocument,
    InvalidConnectionError,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    DefinitionErrors,
    FunctionBundle,
)
from tabsdatak.server._tdrunner.function._handlers.handoff import (
    DestinationHandoff,
    Handoff,
    MissingOutputError,
    OutputCopyError,
    OutputReadError,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.error import (  # noqa: E501
    RunDestinationPluginErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.options import (  # noqa: E501
    RunDestinationPluginArguments,
)
from tabsdatak.spi import Conn, DestPlugin

logger = logging.getLogger(__name__)

KINDS = {KIND_SUBSCRIBER_DEF: SubscriberInfo}
DEFINITION_ERRORS = DefinitionErrors(
    bundle=RunDestinationPluginErrorCode.RDP_2,
    user_code=RunDestinationPluginErrorCode.RDP_3,
    kind=RunDestinationPluginErrorCode.RDP_4,
)


class DestinationPlugin(Command[RunDestinationPluginArguments]):
    request_file: Path
    bundle_folder: Path
    outputs_manifest_file: Path
    connection_file: Path
    work_folder: Path
    request: Request
    subscriber: SubscriberInfo
    plugin: DestPlugin
    connection: Conn
    manifest: OutputManifest
    handoff: DestinationHandoff

    def __init__(self, arguments: RunDestinationPluginArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.bundle_folder = arguments.bundle_folder
        self.outputs_manifest_file = arguments.outputs_manifest
        self.connection_file = arguments.connection_file
        self.work_folder = arguments.work_folder or Path(tempfile.gettempdir())

    def run(self) -> int:
        logger.info("Running the destination plugin")
        self.load()
        if self.empty:
            logger.info(
                f"The subscriber returned no data for its "
                f"{len(self.manifest.outputs)} output(s); "
                f"{type(self.plugin).__name__} is not called"
            )
            return 0
        self.handoff = Handoff.of(self.plugin, self.work_folder)
        try:
            self.read_tables()
            self.responder.destination_write()
            self.write_tables()
        finally:
            self.handoff.reap()
        logger.info(f"Destination plugin wrote {len(self.handoff.slots)} table(s)")
        return 0

    def load(self) -> None:
        self.request = Request.load(
            self.request_file,
            RunDestinationPluginErrorCode.RDP_1,
        )
        self.subscriber = FunctionBundle(self.bundle_folder).definition(
            KINDS,
            DEFINITION_ERRORS,
        )
        self.plugin = self.resolve_plugin()
        if not DestinationContract(type(self.plugin)).frames():
            logger.warning(warn_legacy_dest(type(self.plugin)))
        self.connection = self.load_connection()
        logger.debug(
            f"Destination plugin {type(self.plugin).__name__} "
            f"for connection {type(self.connection).__name__}"
        )
        self.manifest = self.load_manifest()

    @property
    def empty(self) -> bool:
        outputs = self.manifest.outputs
        return bool(outputs) and all(output.uri is None for output in outputs)

    def load_connection(self) -> Conn:
        try:
            return ConnectionDocument(self.connection_file).load()
        except InvalidConnectionError as error:
            raise RunDestinationPluginErrorCode.RDP_11.exception(
                cause=error,
                path=error.path,
                detail=error.detail,
            )

    def resolve_plugin(self) -> DestPlugin:
        destination = self.subscriber.destination
        try:
            definition = get_dest(type(destination))
        except KeyError as error:
            raise RunDestinationPluginErrorCode.RDP_5.exception(
                cause=error,
                destination_type=type(destination).__name__,
                detail=str(error),
            )
        return definition.plugin()

    def load_manifest(self) -> OutputManifest:
        path = self.outputs_manifest_file
        try:
            return OutputManifest.load(path)
        except ManifestError as error:
            raise RunDestinationPluginErrorCode.RDP_7.exception(
                cause=error,
                path=path,
                detail=str(error),
            )

    def read_tables(self) -> None:
        try:
            slots = self.handoff.seed(self.request.info, self.manifest)
        except MissingOutputError as error:
            raise RunDestinationPluginErrorCode.RDP_8.exception(
                cause=error,
                slot=error.slot,
                name=error.name,
            )
        except OutputReadError as error:
            raise RunDestinationPluginErrorCode.RDP_10.exception(
                cause=error,
                slot=error.slot,
                path=error.path,
                detail=error.detail,
            )
        except OutputCopyError as error:
            raise RunDestinationPluginErrorCode.RDP_9.exception(
                cause=error,
                slot=error.slot,
                path=error.path,
                detail=error.detail,
            )
        logger.debug(f"Outputs {self.manifest.outputs} handed over as {slots}")

    def write_tables(self) -> None:
        context = DestinationContext.from_info(
            self.bundle_folder,
            self.request.info,
        )
        logger.info(
            f"Writing {len(self.handoff.slots)} table(s) through "
            f"{type(self.plugin).__name__}"
        )
        try:
            self.handoff.grow(
                self.plugin,
                context,
                self.connection,
                self.subscriber.destination,
            )
        except Exception as error:
            raise RunDestinationPluginErrorCode.RDP_6.exception(
                cause=error,
                detail=str(error),
            )
