#
# Copyright 2026 Tabsdata Inc.
#

"""Console entry point for ``tdx-domain``."""

from __future__ import annotations

from tabsdatax.itest.tdksystems2 import _venv, constants, tdx


def main() -> None:
    _venv.bootstrap(constants.DOM_TOOL, "tabsdatax.itest.tdksystems2.tdx_domain2")
    tdx.main_domain()


if __name__ == "__main__":
    main()
