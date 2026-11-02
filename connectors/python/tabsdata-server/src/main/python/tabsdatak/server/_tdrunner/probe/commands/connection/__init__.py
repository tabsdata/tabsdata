#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.probe.commands.connection.options import (
    ConnectionProbeArguments,
)
from tabsdatak.server._tdrunner.probe.commands.connection.runner import (
    ConnectionProbe,
)

VERSION = version()
GENERATION = 2
INFORMATION = "probe-connection — run one connector Explorer call against a connection"
MANUAL = (
    "probe-connection --request <PATH> --connection-file <PATH> --liveness <PATH> "
    "--journal-file <PATH> --responder-url <URL> --responder-method <METHOD> "
    "--responder-authorization <AUTH>\n\n"
    "Loads the connection from --connection-file, with the secret values the pod "
    "receives as SECRET_CONN__* variables, runs the check, metadata or sample "
    "call --request asks of the connection's Explorer under the kind's timeout, "
    "and posts the outcome to the responder as the Done report's output. Error "
    "messages leave the pod with those secret values scrubbed."
)


def build_command_parser() -> ArgumentsParser:
    return ConnectionProbeArguments.parser(
        program="probe-connection",
        description="Run one connector Explorer call against a connection.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-probe-connection", parser=build_command_parser)
def main(arguments: ConnectionProbeArguments) -> int:
    return ConnectionProbe(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
