#
# Copyright 2026 Tabsdata Inc.
#

import textwrap
from datetime import date
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.emission import HEADER
from tabsdatax.cmake.kupgrader.common.templates import run_upgrader
from tabsdatax.cmake.kupgrader.common.workflow import Context

DEFAULT_RULES = textwrap.dedent(
    """\
    schema: 1
    rules: []
    """
)
RULES_COMMAND = "rules"
KUPGRADE_OPTION = "--kupgrade"
PROBLEMS_KEY = "problems"


def rules_file(context: Context, version: str) -> Path:
    return context.rules_folder / f"{version}.yaml"


def ensure_default_rules(context: Context, version: str) -> Path | None:
    path = rules_file(context, version)
    if path.exists():
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER.format(year=date.today().year) + DEFAULT_RULES)
    return path


def rules_problems(binary: Path, kupgrade_folder: Path) -> list[str]:
    report = run_upgrader(
        binary, [RULES_COMMAND, KUPGRADE_OPTION, str(kupgrade_folder)]
    )
    return list(report.get(PROBLEMS_KEY, []))
