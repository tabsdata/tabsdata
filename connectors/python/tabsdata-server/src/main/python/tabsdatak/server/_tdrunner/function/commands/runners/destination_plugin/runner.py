#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from tabsdatak._connectors import get_dest
from tabsdatak._info import KIND_SUBSCRIBER_DEF, SubscriberInfo
from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import (
    notify_step_destination_write,
)
from tabsdatak.server._tdrunner.function._handlers.handoff import (
    DestinationHandoff,
    Handoff,
    MissingOutputError,
    OutputCopyError,
)
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.error import (  # noqa: E501
    RunDestinationPluginErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.options import (  # noqa: E501
    RunDestinationPluginArguments,
)
from tabsdatak.spi import Conn, Dest, DestContext, DestPlugin

logger = logging.getLogger(__name__)


class DestinationContext(FromRequestCtx, DestContext):
    pass


def load_function_definition(bundle_folder: Path) -> SubscriberInfo:
    return load_function_def_or_raise(
        bundle_folder,
        allowed_kinds={KIND_SUBSCRIBER_DEF: SubscriberInfo},
        bad_bundle_code=RunDestinationPluginErrorCode.RDP_2,
        bad_user_code_code=RunDestinationPluginErrorCode.RDP_3,
        wrong_kind_code=RunDestinationPluginErrorCode.RDP_4,
    )


def resolve_plugin(subscriber: SubscriberInfo) -> DestPlugin:
    try:
        definition = get_dest(type(subscriber.destination))
    except KeyError as error:
        raise RunDestinationPluginErrorCode.RDP_5.exception(
            cause=error,
            destination_type=type(subscriber.destination).__name__,
            detail=str(error),
        )
    return definition.plugin()


def load_manifest(path: Path) -> OutputManifest:
    try:
        return OutputManifest.load(path)
    except ManifestError as error:
        raise RunDestinationPluginErrorCode.RDP_7.exception(
            cause=error, path=path, detail=str(error)
        )


def read_tables(
    handoff: DestinationHandoff, request: Request, manifest: OutputManifest
) -> list[Path]:
    try:
        return handoff.seed(request.info, manifest)
    except MissingOutputError as error:
        raise RunDestinationPluginErrorCode.RDP_8.exception(
            cause=error, slot=error.slot, name=error.name
        )
    except OutputCopyError as error:
        raise RunDestinationPluginErrorCode.RDP_9.exception(
            cause=error, slot=error.slot, path=error.path, detail=error.detail
        )


def write_tables(
    handoff: DestinationHandoff,
    plugin: DestPlugin,
    context: DestContext,
    connection: Conn,
    destination: Dest,
    tables: list[Path],
) -> None:
    try:
        handoff.grow(plugin, context, connection, destination, tables)
    except Exception as error:
        raise RunDestinationPluginErrorCode.RDP_6.exception(
            cause=error, detail=str(error)
        )


def run(arguments: RunDestinationPluginArguments) -> int:
    logger.info("Running the destination plugin")
    request = Request.load(arguments.request, RunDestinationPluginErrorCode.RDP_1)
    subscriber = load_function_definition(arguments.bundle_folder)
    plugin = resolve_plugin(subscriber)
    connection = load_connection(arguments.connection_file)
    logger.debug(
        f"Destination plugin {type(plugin).__name__} "
        f"for connection {type(connection).__name__}"
    )
    manifest = load_manifest(arguments.outputs_manifest)
    outputs = manifest.outputs
    if outputs and all(output.uri is None for output in outputs):
        logger.info(
            f"The subscriber returned no data for its {len(outputs)} output(s); "
            f"{type(plugin).__name__} is not called"
        )
        return 0
    work_folder = arguments.work_folder or Path(tempfile.gettempdir())
    handoff = Handoff.of(plugin, work_folder)
    try:
        tables = read_tables(handoff, request, manifest)
        logger.debug(f"Outputs {outputs} handed over as {tables}")
        context = DestinationContext.from_info(arguments.bundle_folder, request.info)
        notify_step_destination_write(arguments)
        logger.info(f"Writing {len(tables)} table(s) through {type(plugin).__name__}")
        write_tables(
            handoff, plugin, context, connection, subscriber.destination, tables
        )
    finally:
        handoff.harvest()
    logger.info(f"Destination plugin wrote {len(tables)} table(s)")
    return 0
