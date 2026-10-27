#
# Copyright 2026 Tabsdata Inc.
#

"""run-destination-plugin runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak._connectors import get_dest
from tabsdatak._info import KIND_SUBSCRIBER_DEF, SubscriberInfo
from tabsdatak.common._storage.transfer import local_path
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
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.run_destination_plugin.error import (
    RunDestinationPluginErrorCode,
)
from tabsdatak.spi import DestContext

logger = logging.getLogger(__name__)


def _load_function_def(bundle_dir: Path) -> SubscriberInfo:
    """Load the subscriber's ``function_def.json`` -- thin wrapper over
    :func:`load_function_def_or_raise` pinning the accepted kind set
    and the command's per-site error codes."""
    return load_function_def_or_raise(
        bundle_dir,
        allowed_kinds={KIND_SUBSCRIBER_DEF: SubscriberInfo},
        bad_bundle_code=RunDestinationPluginErrorCode.RDP_2,
        bad_user_code_code=RunDestinationPluginErrorCode.RDP_3,
        wrong_kind_code=RunDestinationPluginErrorCode.RDP_4,
    )


class _DestContext(FromRequestCtx, DestContext):
    """Concrete :class:`DestContext` for the run-destination-plugin command.

    All behaviour comes from :class:`FromRequestCtx`; this class
    only fixes the abstract base so ``isinstance(ctx, DestContext)``
    holds at the call boundary.
    """


def _resolve_plugin(info: SubscriberInfo):
    try:
        dest_def = get_dest(type(info.destination))
    except KeyError as exc:
        raise RunDestinationPluginErrorCode.RDP_5.exception(
            cause=exc,
            dest_type=type(info.destination).__name__,
            detail=str(exc),
        )
    return dest_def.plugin()


def _tables_from_manifest(manifest: OutputManifest) -> list[Path]:
    tables: list[Path] = []
    for slot, entry in enumerate(manifest.outputs):
        if entry.uri is None:
            raise RunDestinationPluginErrorCode.RDP_8.exception(
                slot=slot, name=entry.name
            )
        tables.append(local_path(entry.uri))
    return tables


def _invoke_write_out(plugin, ctx: DestContext, conn, dest, tables) -> None:
    try:
        plugin.write_out(ctx, conn, dest, tables)
    except Exception as exc:
        raise RunDestinationPluginErrorCode.RDP_6.exception(cause=exc, detail=str(exc))


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    outputs_manifest_path = Path(args.outputs_manifest)
    connection_file = Path(args.connection_file)

    logger.info("Running destination plugin")
    request = Request.load(request_path, RunDestinationPluginErrorCode.RDP_1)
    info = _load_function_def(bundle_dir)
    plugin = _resolve_plugin(info)
    conn = load_connection(connection_file)
    logger.debug(
        "Resolved Dest plugin %s for connector %s",
        type(plugin).__name__,
        type(conn).__name__,
    )

    try:
        manifest = OutputManifest.load(outputs_manifest_path)
    except ManifestError as error:
        raise RunDestinationPluginErrorCode.RDP_7.exception(
            cause=error, path=outputs_manifest_path, detail=str(error)
        )
    entries = manifest.outputs
    # Contract: a subscriber that returns None for every output slot produced no
    # new data this run, so the destination plugin must NOT be invoked (skip the
    # external write) rather than erroring on the null slots.
    if entries and all(entry.uri is None for entry in entries):
        logger.info(
            "Subscriber produced no data (all %d output slot(s) null); "
            "skipping %s.write_out",
            len(entries),
            type(plugin).__name__,
        )
        return 0
    tables = _tables_from_manifest(manifest)
    logger.debug(
        "Outputs manifest has %d entries: %s; "
        "passing %d table frame(s) to the plugin: %s",
        len(entries),
        entries,
        len(tables),
        tables,
    )

    ctx = _DestContext.from_info(bundle_dir, request.info)
    notify_step_destination_write(args)
    logger.info(
        "Invoking %s.write_out: %d table frame(s) -> %s",
        type(plugin).__name__,
        len(tables),
        type(conn).__name__,
    )
    _invoke_write_out(plugin, ctx, conn, info.destination, tables)
    logger.info(
        "Destination plugin completed: %d table frame(s) written to %s",
        len(tables),
        type(conn).__name__,
    )
    return 0
