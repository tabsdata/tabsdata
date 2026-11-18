#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import tempfile
from dataclasses import replace
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
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy, Zone
from tabsdatak.server._tdrunner.function._handlers.handoff import (
    DestinationHandoff,
    Handoff,
    OutputCopyError,
    OutputReadError,
)
from tabsdatak.server._tdrunner.function._handlers.outputs import (
    OutputError,
    Outputs,
)
from tabsdatak.server._tdrunner.function._handlers.state import (
    State,
    StateReadError,
    StateWriteError,
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
    state_folder: Path
    request: Request
    subscriber: SubscriberInfo
    plugin: DestPlugin
    connection: Conn
    manifest: OutputManifest
    context: DestinationContext
    state: State
    handoff: DestinationHandoff

    def __init__(self, arguments: RunDestinationPluginArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.bundle_folder = arguments.bundle_folder
        self.outputs_manifest_file = arguments.outputs_manifest
        self.connection_file = arguments.connection_file
        self.work_folder = arguments.work_folder or Path(tempfile.gettempdir())
        self.state_folder = arguments.state_folder

    def run(self) -> int:
        logger.info("Running the destination plugin")
        self.load()
        self.hydrate()
        self.write()
        self.drain()
        self.store_state()
        return 0

    def write(self) -> None:
        self.handoff = Handoff.of(self.plugin, self.work_folder)
        try:
            self.read_tables()
            self.responder.destination_write()
            self.write_tables()
        finally:
            self.handoff.reap()
        logger.info(f"Destination plugin took {len(self.handoff.slots)} output(s)")

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
        self.context = DestinationContext.from_info(
            self.work_folder,
            self.request.info,
        )
        self.state = State(self.request, self.state_folder)

    def hydrate(self) -> None:
        try:
            self.state.hydrate(self.context, prefer_local=True)
        except StateReadError as error:
            raise RunDestinationPluginErrorCode.RDP_12.exception(
                cause=error,
                state_file=error.uri,
                detail=error.detail,
            )

    def drain(self) -> None:
        try:
            self.state.drain(self.context)
        except StateWriteError as error:
            raise RunDestinationPluginErrorCode.RDP_13.exception(
                cause=error,
                state_file=error.file,
                detail=error.detail,
            )
        logger.debug(f"State drained to {self.state.file}")

    def store_state(self) -> None:
        state = self.state.output()
        if state is None:
            return
        try:
            stored = Outputs(ColumnPolicy(Zone.LAND)).sink([], [], [], state=state)
        except OutputError as error:
            raise RunDestinationPluginErrorCode.RDP_14.exception(
                cause=error,
                state_file=self.state.file,
                path=state.target.uri,
                detail=str(error),
            )
        self.manifest = replace(self.manifest, state=stored.state)
        path = self.outputs_manifest_file
        try:
            self.manifest.write(path)
        except OSError as error:
            raise RunDestinationPluginErrorCode.RDP_15.exception(
                cause=error,
                path=path,
                detail=str(error),
            )
        logger.debug(f"State stored at {state.target.uri}")

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
        slots = self.handoff.slots
        logger.info(
            f"Handing {len(slots)} output(s) to {type(self.plugin).__name__}, "
            f"{sum(slot.table is None for slot in slots)} of them None"
        )
        try:
            self.handoff.grow(
                self.plugin,
                self.context,
                self.connection,
                self.subscriber.destination,
            )
        except Exception as error:
            raise RunDestinationPluginErrorCode.RDP_6.exception(
                cause=error,
                detail=str(error),
            )
