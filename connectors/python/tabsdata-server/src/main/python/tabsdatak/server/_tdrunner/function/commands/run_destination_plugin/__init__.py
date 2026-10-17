#
# Copyright 2026 Tabsdata Inc.
#

"""run-destination-plugin command.

The subscriber chain's counterpart to ``run-source-plugin``. Reads the
function definition from the extracted bundle (must be a
``SubscriberInfo``), reads the outputs manifest produced by
``run-function``, resolves the matching ``DestPlugin`` via the connector
registry, and calls ``write_out()`` with the list of parquet paths the
function produced.

The destination plugin consumes the parquets positionally — one entry
per slot, in ``info.destination.cardinality(info.destination)`` order.
Null entries (the user function returned ``None`` for that slot)
surface as ``BadManifestError`` since the ``DestPlugin.write_out`` API
takes ``list[TableFileSpec]`` (no ``None`` values); subscribers must
return concrete frames for every output slot.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function.commands.run_destination_plugin.runner import (
    _DestContext,
    _invoke_write_out,
    _load_function_def,
    _resolve_plugin,
    _tables_from_manifest,
    run,
)

__all__ = [
    "_DestContext",
    "_invoke_write_out",
    "_load_function_def",
    "_resolve_plugin",
    "_tables_from_manifest",
    "run",
    "main",
]

VERSION = "2.2.1"
INFO = (
    "run-destination-plugin — hand the function's output parquets to the "
    "subscriber's destination plugin"
)
MAN = (
    "run-destination-plugin --request <PATH> --bundle-dir <PATH> "
    "--outputs-manifest <PATH> --connection-file <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir (must be a "
    "subscriberDef), reads --outputs-manifest, resolves the destination "
    "plugin via the tabsdatak connectors registry, loads the connection "
    "from --connection-file, and invokes write_out() with the list of "
    "parquet paths the function produced."
)


def _build_parser():
    parser = build_parser(
        program="run-destination-plugin",
        description="Invoke the subscriber's destination plugin.",
        version=VERSION,
        info=INFO,
        manual=MAN,
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
    parser.add_argument(
        "--connection-file",
        required=True,
        dest="connection_file",
        help="Path to the connection definition file.",
    )
    return parser


@command(name="td_run_destination_plugin", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
