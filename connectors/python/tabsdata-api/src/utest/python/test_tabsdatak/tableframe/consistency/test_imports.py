#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

import tabsdatak.tableframe as package

pytestmark = pytest.mark.tableframe

ROOT = package.__name__


def _modules() -> list[str]:
    names = {ROOT}
    for base in package.__path__:
        root = Path(base)
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            relative = path.relative_to(root).with_suffix("")
            parts = [p for p in relative.parts if p != "__init__"]
            names.add(".".join([ROOT, *parts]) if parts else ROOT)
    return sorted(names)


def _module_file(name: str) -> Path | None:
    spec = importlib.util.find_spec(name)
    if spec is None or spec.origin in (None, "namespace", "built-in"):
        return None
    return Path(spec.origin)


def _imported_targets(name: str, source: str) -> set[str]:
    tree = ast.parse(source, filename=name)
    module_file = _module_file(name)
    is_package = module_file is not None and module_file.name == "__init__.py"
    package_of = name if is_package else name.rsplit(".", 1)[0]
    targets: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                targets.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package_of.rsplit(".", node.level - 1)[0] if node.level > 1 else package_of
                module = f"{base}.{node.module}" if node.module else base
            else:
                module = node.module or ""
            targets.add(module)
            for alias in node.names:
                targets.add(f"{module}.{alias.name}")
    return {t for t in targets if t == ROOT or t.startswith(f"{ROOT}.")}


def _graph() -> dict[str, set[str]]:
    known = set(_modules())
    graph: dict[str, set[str]] = {}
    for name in known:
        path = _module_file(name)
        if path is None:
            continue
        edges = set()
        for target in _imported_targets(name, path.read_text()):
            owner = target
            while owner and owner not in known:
                owner = owner.rsplit(".", 1)[0] if "." in owner else ""
            if owner and owner != name:
                edges.add(owner)
        graph[name] = edges
    return graph


def _cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    index = {}
    low = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    counter = [0]
    found: list[list[str]] = []

    def strong_connect(vertex: str) -> None:
        index[vertex] = low[vertex] = counter[0]
        counter[0] += 1
        stack.append(vertex)
        on_stack.add(vertex)
        for neighbor in graph.get(vertex, ()):
            if neighbor not in index:
                strong_connect(neighbor)
                low[vertex] = min(low[vertex], low[neighbor])
            elif neighbor in on_stack:
                low[vertex] = min(low[vertex], index[neighbor])
        if low[vertex] == index[vertex]:
            component = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member == vertex:
                    break
            if len(component) > 1 or vertex in graph.get(vertex, ()):
                found.append(sorted(component))

    for node in list(graph):
        if node not in index:
            strong_connect(node)
    return found


def test_module_inventory_is_non_empty():
    modules = _modules()
    assert ROOT in modules
    assert len(modules) > 1, "package walk found no submodules"


def test_no_circular_imports_static():
    graph = _graph()
    cycles = _cycles(graph)
    assert not cycles, "circular module-level imports detected:\n" + "\n".join(
        " -> ".join(cycle) for cycle in cycles
    )


@pytest.mark.parametrize("name", _modules())
def test_each_module_imports_in_isolation(name):
    def tableframe_modules():
        return [
            module
            for module in list(sys.modules)
            if module == ROOT or module.startswith(f"{ROOT}.")
        ]

    saved = {module: sys.modules[module] for module in tableframe_modules()}
    for loaded in saved:
        del sys.modules[loaded]
    try:
        importlib.import_module(name)
    except ImportError as error:
        message = str(error).lower()
        if "circular" in message or "partially initialized" in message:
            pytest.fail(f"{name} fails with a circular import: {error}")
        raise
    finally:
        for loaded in tableframe_modules():
            del sys.modules[loaded]
        sys.modules.update(saved)
