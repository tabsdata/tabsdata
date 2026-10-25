#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import ast
import importlib.util
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

FUNCTION = "tabsdatak.server._tdrunner.function"
COMMONS = f"{FUNCTION}._commons"
HANDLERS = f"{FUNCTION}._handlers"
COMMANDS = f"{FUNCTION}.commands"

UPPER_LAYERS = {
    COMMONS: (HANDLERS, COMMANDS),
    HANDLERS: (COMMANDS,),
}


def modules(package: str) -> Iterator[tuple[str, Path]]:
    for location in importlib.util.find_spec(package).submodule_search_locations:
        for path in sorted(Path(location).rglob("*.py")):
            parts = path.relative_to(location).with_suffix("").parts
            if parts[-1] == "__init__":
                parts = parts[:-1]
            yield ".".join((package, *parts)), path


def imports(module: str, path: Path) -> set[str]:
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    names = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = importlib.util.resolve_name(
                "." * node.level + (node.module or ""),
                package,
            )
            names.add(base)
            names.update(f"{base}.{alias.name}" for alias in node.names)
    return names


def within(name: str, package: str) -> bool:
    return name == package or name.startswith(f"{package}.")


@pytest.mark.parametrize("layer", sorted(UPPER_LAYERS))
def test_layer_imports_only_from_lower_layers(layer: str):
    found = list(modules(layer))
    assert found, f"Layer {layer} has no modules to check"
    violations = sorted(
        f"{module} imports {name}"
        for module, path in found
        for name in imports(module, path)
        for upper in UPPER_LAYERS[layer]
        if within(name, upper)
    )
    assert not violations, "Some modules import from an upper layer: " + "; ".join(
        violations,
    )
