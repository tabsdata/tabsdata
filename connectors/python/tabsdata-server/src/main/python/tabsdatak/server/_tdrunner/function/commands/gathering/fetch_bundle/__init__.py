#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_bundle.options import (  # noqa: E501
    FetchBundleArguments,
)
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_bundle.runner import (
    FetchBundle,
)

VERSION = version()
GENERATION = 2
INFORMATION = "fetch-bundle — download the function bundle and extract it"
MANUAL = (
    "fetch-bundle --request <PATH> --bundle-folder <PATH> --bundle-archive <PATH> "
    "--liveness <PATH>\n\n"
    "Reads the bundle location from --request, downloads the bundle to "
    "--bundle-archive with the options of its storage mount, read from the "
    "SECRETS__STORAGE__MOUNTS__<MOUNT>__OPTIONS__* environment variables, and "
    "extracts the archive into --bundle-folder. The caller owns the lifecycle of "
    "every path."
)


def build_command_parser() -> ArgumentsParser:
    return FetchBundleArguments.parser(
        program="fetch-bundle",
        description="Download and extract the function bundle.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-fetch-bundle", parser=build_command_parser)
def main(arguments: FetchBundleArguments) -> int:
    return FetchBundle(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
