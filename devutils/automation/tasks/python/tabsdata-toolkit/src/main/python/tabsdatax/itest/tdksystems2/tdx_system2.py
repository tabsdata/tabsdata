#
# Copyright 2026 Tabsdata Inc.
#

"""Console entry point for ``tdx-system``."""

from __future__ import annotations

from tabsdatax.itest.tdksystems2 import _venv, constants, tdx


def main() -> None:
    _venv.bootstrap(constants.SYS_TOOL, "tabsdatax.itest.tdksystems2.tdx_system2")
    tdx.main_system()


if __name__ == "__main__":
    main()
