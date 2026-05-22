#
#  Copyright 2026 Tabs Data Inc.
#

"""ComponentTracker -- component-level test progress tracking.

At collection time, derives the component name from each test item's
file path and builds a component -> total tests mapping.  As test
results arrive, counters are updated and pushed to the writer.

Component derivation: find ``utest`` in the path, go two directories
up (grandparent of ``utest``).  Known names are mapped:
``platform`` -> ``tabsdata``, ``tabsdata-ag`` -> ``tabsdata-agent``.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tabsdatax.pytest.plugins.harvester.internal.helpers import normalize_nodeid
from tabsdatax.pytest.plugins.harvester.internal.schema import ComponentState
from tabsdatax.pytest.plugins.harvester.internal.writer import MetaMessage, MetaWriter

if TYPE_CHECKING:
    import pytest

COMPONENT_UNKNOWN = "unknown"

# Grandparent-of-utest name -> display component name
COMPONENT_MAP = {
    "platform": "tabsdata",
    "tabsdata-ag": "tabsdata-agent",
}


def extract_component(fspath: str) -> str:
    """Derive the component name from a test file's absolute path.

    Finds ``utest`` in the path parts and returns the directory name
    two levels above it.  Special-case mappings are applied for known
    components that don't follow the naming convention.
    """
    parts = Path(fspath).parts
    for i, part in enumerate(parts):
        if part == "utest" and i >= 2:
            raw = parts[i - 2]
            # Skip generic directory names (e.g. <component>/python/src/utest)
            if raw == "python" and i >= 3:
                raw = parts[i - 3]
            return COMPONENT_MAP.get(raw, raw)
    return COMPONENT_UNKNOWN


class ComponentTracker:
    """Tracks per-component test progress.

    Every test belongs to exactly one component.  Counters are
    maintained in-memory and sent as absolute values to the writer
    to avoid race conditions with the async queue.
    """

    def __init__(self, writer: MetaWriter) -> None:
        self._writer = writer
        # nodeid -> component name
        self._test_components: dict[str, str] = {}
        # In-memory counters: component -> {total, passed, failed, skipped, pending}
        self._counts: dict[str, dict[str, int]] = {}

    def get_component(self, nodeid: str) -> str:
        """Return the component name for a test nodeid."""
        return self._test_components.get(normalize_nodeid(nodeid), COMPONENT_UNKNOWN)

    def register_all(self, items: list[pytest.Item]) -> None:
        """Build component-to-test mapping from collected items."""
        component_counts: dict[str, int] = {}

        for item in items:
            component = extract_component(str(item.fspath))
            self._test_components[item.nodeid] = component
            component_counts[component] = component_counts.get(component, 0) + 1

        for name, count in component_counts.items():
            self._counts[name] = {
                "total": count,
                "passed": 0,
                "failed": 0,
                "skipped": 0,
                "pending": count,
            }
            self._writer.enqueue(
                MetaMessage(
                    kind="component",
                    key=name,
                    payload=ComponentState(
                        total=count,
                        pending=count,
                    ).to_dict(),
                )
            )

    def on_deselected(self, items: list[pytest.Item]) -> None:
        """Subtract deselected items from component counts."""
        for item in items:
            component = self._test_components.pop(item.nodeid, None)
            if component is None:
                continue
            c = self._counts.get(component)
            if c is None:
                continue
            c["total"] = max(0, c["total"] - 1)
            c["pending"] = max(0, c["pending"] - 1)
            self._writer.enqueue(
                MetaMessage(kind="component", key=component, payload=dict(c))
            )

    def on_test_result(self, nodeid: str, outcome: str) -> None:
        """Update component counters for a completed test.

        *outcome* should be one of ``"passed"``, ``"failed"``,
        ``"skipped"``.
        """
        component = self._test_components.get(normalize_nodeid(nodeid))
        if component is None:
            return
        c = self._counts.get(component)
        if c is None:
            return
        c["pending"] = max(0, c["pending"] - 1)
        if outcome in ("passed", "failed", "skipped"):
            c[outcome] = c[outcome] + 1
        self._writer.enqueue(
            MetaMessage(kind="component", key=component, payload=dict(c))
        )
