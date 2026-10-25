#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from tabsdatak.common._storage import mounts, transfer

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

ALLOWED = {
    transfer: {"obstore"},
    mounts: {"yaml", transfer.__name__},
}


def imports(path: Path) -> set[str]:
    names = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add("." * node.level + (node.module or ""))
    return names


def allowed(name: str, module) -> bool:
    top = name.partition(".")[0]
    return (
        top in sys.stdlib_module_names
        or name in ALLOWED[module]
        or (top in ALLOWED[module])
    )


@pytest.mark.parametrize("module", list(ALLOWED), ids=["transfer", "mounts"])
def test_module_runs_without_tabsdata(module):
    foreign = sorted(
        name for name in imports(Path(module.__file__)) if not allowed(name, module)
    )
    assert not foreign, f"Module {module.__name__} imports {foreign}"
