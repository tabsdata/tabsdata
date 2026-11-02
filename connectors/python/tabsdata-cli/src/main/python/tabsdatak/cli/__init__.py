#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys


def main() -> None:
    from tabsdatak.cli.td.cli import cli
    from tabsdatak.common._compatibility import (
        PackageVersionError,
        check_sticky_version_packages,
    )

    try:
        check_sticky_version_packages()
    except PackageVersionError as error:
        print(error, file=sys.stderr)
    cli(prog_name="tdk")
