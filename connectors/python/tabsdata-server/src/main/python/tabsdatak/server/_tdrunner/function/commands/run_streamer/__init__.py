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

  * ``0010-begin-execution`` -- initialise journal + notify_status_running
  * ``0020-fetch-request`` -- pull request.yaml from apiserver
  * ``0030-get-bundle`` -- download + extract the user bundle
  * ``0900-manifest-trigger`` -- sidecar (NU7); scans
    ``<stage>/manifests/`` and POSTs ``fn_trigger`` per manifest
  * ``1000-run-streamer`` (this command) -- long-running, exits 0 on
    clean shutdown via SIGTERM

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.run_streamer.options import (
    RunStreamerArguments,
)
from tabsdatak.server._tdrunner.function.commands.run_streamer.runner import (
    _load_function_def,
    _resolve_plugin,
    run,
)

__all__ = ["_load_function_def", "_resolve_plugin", "run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = (
    "run-streamer — long-running streamer pod entrypoint; runs the "
    "@stream_publisher's StreamSrcPlugin until SIGTERM"
)
MANUAL = (
    "run-streamer\n"
    "    --request <PATH>\n"
    "    --bundle-folder <PATH>\n"
    "    --output-folder <PATH>\n"
    "    --connection-file <PATH>\n"
    "    --shared-folder <PATH>\n"
    "    --work-folder <PATH>\n"
    "    --logging-configuration <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --journal-file <PATH>\n\n"
    "Loads the StreamPublisherInfo from --bundle-folder, loads the connection "
    "from --connection-file, builds the StreamerPluginCtx from "
    "request.info, installs SIGTERM/SIGINT handlers that flip ctx.active "
    "to False, then invokes plugin.run(ctx, conn, stream_src) and blocks "
    "until it returns. Stamps journal.end + posts Done on clean exit."
)


def build_command_parser() -> ArgumentsParser:
    return RunStreamerArguments.parser(
        program="run-streamer",
        description="Long-running streamer pod entrypoint.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_streamer", parser=build_command_parser)
def main(arguments: RunStreamerArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
