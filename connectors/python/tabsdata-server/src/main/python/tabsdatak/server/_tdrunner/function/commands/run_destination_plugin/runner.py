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
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    load_outputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
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


def _tables_from_manifest(entries: list[dict]) -> list[Path]:
    """Convert outputs-manifest entries to a positional list of
    `TableFileSpec` (parquet) paths for `DestPlugin.write_out`.

    Subscribers must produce a concrete frame for every slot; an
    entry with `file: null` is a contract violation here and surfaces
    as `BadManifestError`. (`DestPlugin.write_out`'s signature does
    not allow None entries, so the framework cannot pass them
    through.)
    """
    tables: list[Path] = []
    for i, entry in enumerate(entries):
        file = entry.get(MANIFEST_FILE_KEY)
        name = entry.get(MANIFEST_NAME_KEY, f"slot{i}")
        if file is None:
            raise RunDestinationPluginErrorCode.RDP_8.exception(slot=i, name=name)
        tables.append(Path(file))
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
    request = load_request_or_raise(request_path, RunDestinationPluginErrorCode.RDP_1)
    info = _load_function_def(bundle_dir)
    plugin = _resolve_plugin(info)
    conn = load_connection(connection_file)
    logger.debug(
        "Resolved Dest plugin %s for connector %s",
        type(plugin).__name__,
        type(conn).__name__,
    )

    entries = load_outputs_manifest_or_raise(
        outputs_manifest_path, RunDestinationPluginErrorCode.RDP_7
    )
    # Contract: a subscriber that returns None for every output slot produced no
    # new data this run, so the destination plugin must NOT be invoked (skip the
    # external write) rather than erroring on the null slots.
    if entries and all(e.get(MANIFEST_FILE_KEY) is None for e in entries):
        logger.info(
            "Subscriber produced no data (all %d output slot(s) null); "
            "skipping %s.write_out",
            len(entries),
            type(plugin).__name__,
        )
        return 0
    tables = _tables_from_manifest(entries)
    logger.debug(
        "Outputs manifest has %d entries: %s; "
        "passing %d table frame(s) to the plugin: %s",
        len(entries),
        entries,
        len(tables),
        tables,
    )

    ctx = _DestContext(
        work_dir=bundle_dir,
        exec_plan_id=str(request.info.execution_id),
        trx_id=str(request.info.transaction_id),
        exec_plan_ts=request.info.triggered_on_iso,
        collection_id=str(request.info.collection_id),
        collection_name=request.info.collection,
        function_id=str(request.info.function_run_id),
        function_name=request.info.function,
    )
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
