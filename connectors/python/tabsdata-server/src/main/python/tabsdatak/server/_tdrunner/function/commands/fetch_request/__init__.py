#
# Copyright 2026 Tabsdata Inc.
#

"""fetch-request command.

Performs `<resolver-method> <resolver-url>` with the given Authorization
header, extracts the ``data`` field from the JSON envelope returned by
the apiserver, and writes it as YAML to `--request-file`. Used to
materialize the function-run request envelope before the rest of the
chain runs.

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
from tabsdatak.server._tdrunner.function.commands.fetch_request.arguments import (
    FetchRequestArguments,
)
from tabsdatak.server._tdrunner.function.commands.fetch_request.runner import (
    _fetch,
    run,
)

__all__ = ["_fetch", "run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = (
    "fetch-request — fetch a function-run definition (request.yaml) from the API server"
)
MANUAL = (
    "fetch-request\n"
    "    --shared-folder <PATH>\n"
    "    --work-folder <PATH>\n"
    "    --logging-configuration <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --resolver-url <URL>\n"
    "    --resolver-method <METHOD>\n"
    "    --resolver-authorization <AUTH>\n"
    "    --request-file <PATH>\n\n"
    "Performs the resolver request with the given Authorization header, "
    "extracts the 'data' field from the JSON envelope, and writes it as "
    "YAML to --request-file."
)


def build_command_parser() -> ArgumentsParser:
    return FetchRequestArguments.parser(
        program="fetch-request",
        description="Fetch a function-run definition (request.yaml) from a URL.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_fetch_request", parser=build_command_parser)
def main(arguments: FetchRequestArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
