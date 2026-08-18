#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function.commands import tabsdata_command
from tabsdatak.server._tdrunner.function.commands.inspect_instance.runner import run

__all__ = ["main"]

VERSION = "2.0.1"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="inspect-instance",
        description="Dump mounted instance config and pod folders for troubleshooting.",
    )
    parser.add_argument("--version", "-v", action="version", version=VERSION)
    return parser


def command(*, name: str):
    def wrap(function):
        return tabsdata_command(name=name)(function)

    return wrap


@command(name="td_inspector")
def main() -> int:
    args = _build_parser().parse_args()
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
