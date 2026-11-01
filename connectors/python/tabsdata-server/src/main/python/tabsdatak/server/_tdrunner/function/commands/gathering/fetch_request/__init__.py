#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_request.options import (  # noqa: E501
    FetchRequestArguments,
)
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_request.runner import (  # noqa: E501
    FetchRequest,
)

VERSION = version()
GENERATION = 2
INFORMATION = "fetch-request — fetch the function-run request from the API server"
MANUAL = (
    "fetch-request --resolver-url <URL> --resolver-method <METHOD> "
    "--resolver-authorization <AUTH> --request-file <PATH> --liveness <PATH>\n\n"
    "Calls the resolver with its Authorization header, takes the 'data' field "
    "of the JSON envelope it answers and writes it as YAML to --request-file."
)


def build_command_parser() -> ArgumentsParser:
    return FetchRequestArguments.parser(
        program="fetch-request",
        description="Fetch the function-run request from the API server.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-fetch-request", parser=build_command_parser)
def main(arguments: FetchRequestArguments) -> int:
    return FetchRequest(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
