#
#  Copyright 2026 Tabsdata Inc.
#

import os
import pathlib
import sys
import sysconfig


def main() -> int:
    tdd_dir = pathlib.Path(os.environ.get("TDD_DIR") or pathlib.Path.home() / ".tdd")
    venv = tdd_dir / "tdd-venvs" / "tdd"
    scripts = sysconfig.get_paths(
        scheme="venv",
        vars={"base": str(venv), "platbase": str(venv)},
    )["scripts"]
    print(pathlib.Path(scripts) / ("tdd-exe" + (sysconfig.get_config_var("EXE") or "")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
