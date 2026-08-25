#
# Copyright 2026 Tabsdata Inc.
#

"""run-streamer command.

Long-running entrypoint of the streamer pod's main container. Loads
the ``StreamPublisherInfo`` from the extracted bundle, builds a
:class:`StreamerPluginCtx`, and invokes the
:class:`StreamSrcPlugin`'s ``run(ctx, conn, stream_src)`` until the
pod receives SIGTERM / SIGINT or the plugin returns of its own accord.

The plugin's responsibility is to honour ``ctx.active`` (poll loop
exits when it flips to ``False``) and to call ``ctx.stage(...)`` once
per rollover. Everything else -- writing the manifest under
``<stage>/manifests/``, moving parquets into ``<stage>/files/``,
persisting the envelope at ``<stage>/stream-state.json`` -- is the
ctx's job (see :mod:`stage_layout` for the layout convention and
:class:`StreamerPluginCtx` for the IO).

Pod chain (see ``stream-publisher_kminus.yaml``):

  * ``0010-begin-execution`` -- initialise journal + notify_running
  * ``0020-fetch-request`` -- pull request.yaml from apiserver
  * ``0030-localize-fn-bundle`` -- download + extract the user bundle
  * ``0900-manifest-trigger`` -- sidecar (NU7); scans
    ``<stage>/manifests/`` and POSTs ``fn_trigger`` per manifest
  * ``1000-run-streamer`` (this command) -- long-running, exits 0 on
    clean shutdown via SIGTERM

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.run_streamer.runner import (
    _load_function_def,
    _resolve_plugin,
    run,
)

__all__ = ["_load_function_def", "_resolve_plugin", "run", "main"]

VERSION = "2.0.2"
INFO = (
    "run-streamer — long-running streamer pod entrypoint; runs the "
    "@stream_publisher's StreamSrcPlugin until SIGTERM"
)
MAN = (
    "run-streamer\n"
    "    --request <PATH>\n"
    "    --bundle-dir <PATH>\n"
    "    --out-dir <PATH>\n"
    "    --connection-file <PATH>\n"
    "    --shared-dir <PATH>\n"
    "    --work-dir <PATH>\n"
    "    --log-config <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --journal-file <PATH>\n\n"
    "Loads the StreamPublisherInfo from --bundle-dir, loads the connection "
    "from --connection-file, builds the StreamerPluginCtx from "
    "request.info, installs SIGTERM/SIGINT handlers that flip ctx.active "
    "to False, then invokes plugin.run(ctx, conn, stream_src) and blocks "
    "until it returns. Stamps journal.end + posts Done on clean exit."
)


def _build_parser():
    parser = build_parser(
        prog="run-streamer",
        description="Long-running streamer pod entrypoint.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help="Extracted bundle directory (function_def.json + user code)",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help=(
            "Local scratch directory the StreamSrcPlugin writes parquets "
            "into; ctx.stage() then moves them to <stage>/files/."
        ),
    )
    parser.add_argument(
        "--connection-file",
        required=True,
        dest="connection_file",
        help="Path to the connection definition file.",
    )
    return parser


@command(name="td_run_streamer", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
