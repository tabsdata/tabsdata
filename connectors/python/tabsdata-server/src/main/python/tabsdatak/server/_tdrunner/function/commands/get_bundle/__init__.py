#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function.commands.get_bundle.runner import run

VERSION = "2.3.0"
INFO = "get-bundle — download the function bundle and extract it"
MAN = (
    "get-bundle --request <PATH> --bundle-dir <PATH> --bundle-archive <PATH> "
    "--storage-config <PATH> --liveness <PATH>\n\n"
    "Reads the bundle location from --request, downloads the bundle to "
    "--bundle-archive with the credentials of its storage mount in "
    "--storage-config, and extracts the archive into --bundle-dir. The "
    "caller owns the lifecycle of every path."
)


def build_command_parser() -> argparse.ArgumentParser:
    parser = build_parser(
        program="get-bundle",
        description="Download and extract the function bundle.",
        version=VERSION,
        info=INFO,
        manual=MAN,
    )
    parser.add_argument(
        "--request",
        required=True,
        help="Path to the request file.",
    )
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help="Folder to extract the bundle into.",
    )
    parser.add_argument(
        "--bundle-archive",
        required=True,
        help="Path to download the bundle archive to.",
    )
    parser.add_argument(
        "--storage-config",
        required=True,
        dest="storage_config",
        help="Path to the storage configuration file.",
    )
    return parser


@command(name="td_get_bundle", parser=build_command_parser)
def main(arguments: argparse.Namespace) -> int:
    return run(arguments)


if __name__ == "__main__":
    import sys

    sys.exit(main())
