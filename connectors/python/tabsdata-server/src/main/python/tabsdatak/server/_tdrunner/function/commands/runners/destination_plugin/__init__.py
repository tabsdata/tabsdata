#
# Copyright 2026 Tabsdata Inc.
#

"""run-destination-plugin command.

The subscriber chain's counterpart to ``run-source-plugin``. Reads the
function definition from the extracted bundle (must be a
``SubscriberInfo``), reads the outputs manifest produced by
``run-function``, resolves the matching ``DestPlugin`` via the connector
registry, and hands the outputs the function landed in the run's landing
area to the plugin through its ``Handoff``: with a file handoff,
``write_out()`` gets local parquet copies holding only the user's columns.

The destination plugin consumes the outputs positionally — one entry
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
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.destination_plugin.runner import (  # noqa: E501
    _DestContext,
    _load_function_def,
    _read_tables,
    _resolve_plugin,
    _write_tables,
    run,
)

__all__ = [
    "_DestContext",
    "_load_function_def",
    "_read_tables",
    "_resolve_plugin",
    "_write_tables",
    "run",
    "main",
]

VERSION = version()
GENERATION = 1
INFO = (
    "run-destination-plugin — hand the function's landed outputs to the "
    "subscriber's destination plugin"
)
MAN = (
    "run-destination-plugin --request <PATH> --bundle-dir <PATH> "
    "--outputs-manifest <PATH> --connection-file <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir (must be a "
    "subscriberDef), reads --outputs-manifest, resolves the destination "
    "plugin via the tabsdatak connectors registry, loads the connection "
    "from --connection-file, and hands the outputs the function landed "
    "to the plugin: with a file handoff, write_out() gets local parquet "
    "copies holding only the user's columns."
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
            "declaration order. Entries without a `uri` are rejected "
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
