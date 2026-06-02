#
# Copyright 2026 Tabs Data Inc.
#

"""run-destination-plugin command.

The subscriber chain's counterpart to `run-source-plugin`. Reads the
function definition from the extracted bundle (must be a
`SubscriberInfo`), reads the outputs manifest produced by
`run-function`, resolves the matching `DestPlugin` via the connector
registry, and calls `write_out()` with the list of parquet paths the
function produced.

The destination plugin consumes the parquets positionally — one entry
per slot, in `info.destination.cardinality(info.destination)` order.
Null entries (the user function returned `None` for that slot)
surface as `EXIT_BAD_MANIFEST` since the `DestPlugin.write_out` API
takes `list[TableFileSpec]` (no `None`s); subscribers must return
concrete frames for every output slot.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from tabsdatak._connectors import get_dest
from tabsdatak._info import KIND_SUBSCRIBER_DEF, SubscriberInfo
from tabsdatak.server._tdrunner.function._lib.cli import build_parser, command
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function._lib.function_def import load_function_def
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    load_outputs_manifest_or_fail,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_fail
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.spi import DestContext

VERSION = "0.1.0"
INFO = (
    "run-destination-plugin — hand the function's output parquets to the "
    "subscriber's destination plugin"
)
MAN = (
    "run-destination-plugin --request <PATH> --bundle-dir <PATH> "
    "--outputs-manifest <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir (must be a "
    "subscriberDef), reads --outputs-manifest, resolves the destination "
    "plugin via the tabsdatak connectors registry, and invokes "
    "write_out() with the list of parquet paths the function produced."
)

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_BAD_REQUEST = 10
EXIT_BAD_BUNDLE = 11
EXIT_BAD_USER_CODE = 12
EXIT_WRONG_KIND = 13
EXIT_NO_PLUGIN = 14
EXIT_PLUGIN_FAILED = 15
EXIT_BAD_MANIFEST = 16


def _build_parser():
    parser = build_parser(
        prog="run-destination-plugin",
        description="Invoke the subscriber's destination plugin.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir", required=True, help="Extracted bundle directory"
    )
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        dest="outputs_manifest",
        help=(
            "Path to the YAML outputs manifest produced by run-function. "
            "Entries are consumed positionally — one parquet per slot in "
            "declaration order. Entries with `file: null` are rejected "
            "(subscribers must return concrete frames for every slot)."
        ),
    )
    return parser


def _load_function_def(bundle_dir: Path) -> SubscriberInfo:
    """Load the subscriber's ``function_def.json`` -- thin wrapper over
    :func:`load_function_def` pinning the accepted kind set and the
    command's exit codes."""
    return load_function_def(
        bundle_dir,
        allowed_kinds={KIND_SUBSCRIBER_DEF: SubscriberInfo},
        exit_bad_bundle=EXIT_BAD_BUNDLE,
        exit_bad_user_code=EXIT_BAD_USER_CODE,
        exit_wrong_kind=EXIT_WRONG_KIND,
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
        raise CommandSpecificError(EXIT_NO_PLUGIN, str(exc)) from exc
    return dest_def.plugin()


def _tables_from_manifest(entries: list[dict]) -> list[Path]:
    """Convert outputs-manifest entries to a positional list of
    `TableFileSpec` (parquet) paths for `DestPlugin.write_out`.

    Subscribers must produce a concrete frame for every slot; an
    entry with `file: null` is a contract violation here and surfaces
    as `EXIT_BAD_MANIFEST`. (`DestPlugin.write_out`'s signature does
    not allow None entries, so the framework cannot pass them
    through.)
    """
    tables: list[Path] = []
    for i, entry in enumerate(entries):
        file = entry.get(MANIFEST_FILE_KEY)
        name = entry.get(MANIFEST_NAME_KEY, f"slot{i}")
        if file is None:
            raise CommandSpecificError(
                EXIT_BAD_MANIFEST,
                f"output slot {i} ({name!r}) is null — subscribers "
                f"must return a concrete frame for every output slot",
            )
        tables.append(Path(file))
    return tables


def _invoke_write_out(plugin, ctx: DestContext, dest, tables) -> None:
    try:
        plugin.write_out(ctx, dest, tables)
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_PLUGIN_FAILED, f"plugin write_out failed: {exc}"
        ) from exc


@command(name="td_run_destination_plugin", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    outputs_manifest_path = Path(args.outputs_manifest)

    request = load_request_or_fail(request_path, EXIT_BAD_REQUEST)
    info = _load_function_def(bundle_dir)
    plugin = _resolve_plugin(info)

    entries = load_outputs_manifest_or_fail(outputs_manifest_path, EXIT_BAD_MANIFEST)
    tables = _tables_from_manifest(entries)

    ctx = _DestContext(
        work_dir=bundle_dir,
        exec_plan_id=str(request.info.execution_id),
        trx_id=str(request.info.transaction_id),
        collection_id=str(request.info.collection_id),
        collection_name=request.info.collection,
        function_id=str(request.info.function_run_id),
    )
    _invoke_write_out(plugin, ctx, info.destination, tables)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
