#
#  Copyright 2026 Tabsdata Inc.
#

"""FixtureTracker — fixture scope, lifecycle state, and test countdown.

At collection time, inspects every test item's ``fixturenames`` to build
a map of fixture → {scope, total tests using it}.  During execution,
hooks update lifecycle state (setup/active/teardown/done) and decrement
the remaining-test counter as results come in.

Fixtures are sorted: session → package → module → class → function,
and within each scope alphabetically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    FIXTURE_SCOPE_ORDER,
    FixtureLifecycle,
)
from tabsdatax.pytesting.plugins.harvester.internal.helpers import normalize_nodeid
from tabsdatax.pytesting.plugins.harvester.internal.schema import FixtureState
from tabsdatax.pytesting.plugins.harvester.internal.writer import (
    MetaMessage,
    MetaWriter,
)

if TYPE_CHECKING:
    import pytest

# Internal pytest fixtures that add noise without actionable insight.
_BUILTIN_FIXTURES = frozenset({
    "request",
    "tmp_path",
    "tmp_path_factory",
    "capfd",
    "capfdbinary",
    "caplog",
    "capsys",
    "capsysbinary",
    "cache",
    "doctest_namespace",
    "monkeypatch",
    "pytestconfig",
    "record_property",
    "record_testsuite_property",
    "record_xml_attribute",
    "recwarn",
    "tmpdir",
    "tmpdir_factory",
    "worker_id",
})


FIXTURE_NONE = "(none)"


class FixtureTracker:
    """Tracks fixture lifecycle and per-fixture test countdown.

    Every test contributes to at least one fixture entry.  Tests with no
    tracked fixtures are counted under the ``"(none)"`` pseudo-fixture.
    """

    def __init__(self, writer: MetaWriter) -> None:
        self._writer = writer
        # nodeid → set of fixture names (includes FIXTURE_NONE for tests
        # that don't use any tracked fixture)
        self._test_fixtures: dict[str, set[str]] = {}
        # Known fixture names
        self._known: set[str] = set()
        # In-memory counters: name → {remaining, passed, failed, skipped}
        self._counts: dict[str, dict[str, int]] = {}

    def register_all(self, items: list[pytest.Item]) -> None:
        """Build fixture-to-test mapping from collected items."""
        fixture_counts: dict[str, int] = {}
        fixture_scopes: dict[str, str] = {}

        autouse_fixtures: set[str] = set()

        for item in items:
            if not isinstance(item, pytest.Function):
                continue
            item_fixtures = set()
            for fname in item.fixturenames:
                if fname.startswith("_") or fname in _BUILTIN_FIXTURES:
                    continue

                # Resolve fixturedef once per fixture name
                if fname not in fixture_scopes:
                    try:
                        fixturedefs = item.session._fixturemanager.getfixturedefs(
                            fname, item
                        )
                        if fixturedefs:
                            fdef = fixturedefs[-1]
                            scope = fdef.scope or "function"
                            fixture_scopes[fname] = str(scope)
                            if getattr(fdef, "_autouse", False):
                                autouse_fixtures.add(fname)
                    except Exception:
                        fixture_scopes[fname] = "function"

                item_fixtures.add(fname)
                fixture_counts[fname] = fixture_counts.get(fname, 0) + 1

            if not item_fixtures:
                item_fixtures.add(FIXTURE_NONE)
                fixture_counts[FIXTURE_NONE] = fixture_counts.get(FIXTURE_NONE, 0) + 1

            self._test_fixtures[item.nodeid] = item_fixtures

        # Emit fixture registrations to writer and init in-memory counters
        for fname, count in fixture_counts.items():
            fixture_scope = fixture_scopes.get(fname, "function")
            if fname == FIXTURE_NONE:
                fixture_scope = "function"
            self._known.add(fname)
            self._counts[fname] = {
                "scope": fixture_scope,  # type: ignore[dict-item]
                "total": count,
                "remaining": count,
                "passed": 0,
                "failed": 0,
                "skipped": 0,
                "setup_count": 0,
                "teardown_count": 0,
                "in_setup": 0,
                "in_teardown": 0,
            }
            is_autouse = fname in autouse_fixtures
            self._writer.enqueue(
                MetaMessage(
                    kind="fixture",
                    key=fname,
                    payload=FixtureState(
                        scope=scope,
                        lifecycle=FixtureLifecycle.PENDING,
                        autouse=is_autouse,
                        total=count,
                        remaining=count,
                    ).to_dict(),
                )
            )

    def on_deselected(self, items: list[pytest.Item]) -> None:
        """Subtract deselected items from fixture counts."""
        for item in items:
            fixtures = self._test_fixtures.pop(item.nodeid, set())
            for fname in fixtures:
                c = self._counts.get(fname)
                if c is None:
                    continue
                c["total"] = max(0, c["total"] - 1)
                c["remaining"] = max(0, c["remaining"] - 1)
                self._writer.enqueue(
                    MetaMessage(kind="fixture", key=fname, payload=dict(c))
                )

    def on_setup(self, fixturedef, request) -> None:
        """Called from ``pytest_fixture_setup`` — before setup runs."""
        fname = fixturedef.argname
        if fname not in self._known:
            return
        c = self._counts.get(fname)
        if c is None:
            return
        c["setup_count"] = c.get("setup_count", 0) + 1
        c["in_setup"] = c.get("in_setup", 0) + 1
        self._writer.enqueue(MetaMessage(kind="fixture", key=fname, payload=dict(c)))

    def on_setup_complete(self, fixturedef, request) -> None:
        """Called after fixture setup completes.  Decrements the active
        setup counter."""
        fname = fixturedef.argname
        if fname not in self._known:
            return
        c = self._counts.get(fname)
        if c is None:
            return
        c["in_setup"] = max(0, c.get("in_setup", 0) - 1)
        self._writer.enqueue(MetaMessage(kind="fixture", key=fname, payload=dict(c)))

    def on_teardown(self, fixturedef, request) -> None:
        """Called from ``pytest_fixture_post_finalizer`` — after teardown."""
        fname = fixturedef.argname
        if fname not in self._known:
            return
        c = self._counts.get(fname)
        if c is None:
            return
        c["teardown_count"] = c.get("teardown_count", 0) + 1
        self._writer.enqueue(MetaMessage(kind="fixture", key=fname, payload=dict(c)))

    def on_test_complete(self, nodeid: str, outcome: str) -> None:
        """Decrement remaining count for all fixtures used by this test.

        Uses in-memory counters to avoid race conditions with the async
        writer queue.
        """
        fixtures = self._test_fixtures.get(normalize_nodeid(nodeid), set())
        for fname in fixtures:
            c = self._counts.get(fname)
            if c is None:
                continue
            c["remaining"] = max(0, c["remaining"] - 1)
            if outcome in ("passed", "failed", "skipped"):
                c[outcome] = c[outcome] + 1
            # Each completed test implies one setup + one teardown for
            # that fixture (accurate for function-scoped; approximate for
            # session/module where it's shared across tests).
            c["setup_count"] = c.get("setup_count", 0) + 1
            c["teardown_count"] = c.get("teardown_count", 0) + 1
            self._writer.enqueue(
                MetaMessage(kind="fixture", key=fname, payload=dict(c))
            )

    @staticmethod
    def sort_key(name: str, state: FixtureState) -> tuple:
        """Sort key: scope order, then lifecycle fixtures first, then name."""
        scope_idx = FIXTURE_SCOPE_ORDER.get(state.scope, 99)
        has_lifecycle = 0 if state.scope in ("session", "package", "module") else 1
        return (scope_idx, has_lifecycle, name)
