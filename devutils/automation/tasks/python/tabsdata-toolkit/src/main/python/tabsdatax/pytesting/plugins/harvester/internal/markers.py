#
#  Copyright 2026 Tabsdata Inc.
#

"""MarkerTracker — marker-to-test progress tracking.

At collection time, reads each collected test's markers to build a map
of marker → total tests.  As test results arrive, counters are
updated and pushed to the writer.
"""

from __future__ import annotations

from tabsdatax.pytesting.plugins.harvester.internal.collection import CollectedTest
from tabsdatax.pytesting.plugins.harvester.internal.helpers import normalize_nodeid
from tabsdatax.pytesting.plugins.harvester.internal.schema import MarkerState
from tabsdatax.pytesting.plugins.harvester.internal.writer import (
    MetaMessage,
    MetaWriter,
)

MARKER_NONE = "(none)"


class MarkerTracker:
    """Tracks per-marker test progress.

    Every test contributes to at least one marker entry.  Tests with no
    markers are counted under the ``"(none)"`` pseudo-marker.  A test
    with *N* markers contributes 1 to each of those *N* markers.

    Counters are maintained in-memory and sent as absolute values to
    the writer to avoid race conditions with the async queue.
    """

    def __init__(self, writer: MetaWriter) -> None:
        self._writer = writer
        # nodeid → set of marker names (includes MARKER_NONE for unmarked)
        self._test_markers: dict[str, set[str]] = {}
        # In-memory counters: name → {total, passed, failed, skipped, pending}
        self._counts: dict[str, dict[str, int]] = {}

    def register_all(self, tests: list[CollectedTest]) -> None:
        """Build marker-to-test mapping from the collected tests."""
        marker_counts: dict[str, int] = {}

        for test in tests:
            item_markers = set(test.markers)
            if not item_markers:
                item_markers.add(MARKER_NONE)
            for name in item_markers:
                marker_counts[name] = marker_counts.get(name, 0) + 1
            self._test_markers[test.nodeid] = item_markers

        for name, count in marker_counts.items():
            self._counts[name] = {
                "total": count,
                "passed": 0,
                "failed": 0,
                "skipped": 0,
                "pending": count,
            }
            self._writer.enqueue(
                MetaMessage(
                    kind="marker",
                    key=name,
                    payload=MarkerState(
                        total=count,
                        pending=count,
                    ).to_dict(),
                )
            )

    def on_test_result(self, nodeid: str, outcome: str) -> None:
        """Update marker counters for a completed test.

        *outcome* should be one of ``"passed"``, ``"failed"``,
        ``"skipped"``.
        """
        markers = self._test_markers.get(normalize_nodeid(nodeid), set())
        for name in markers:
            c = self._counts.get(name)
            if c is None:
                continue
            c["pending"] = max(0, c["pending"] - 1)
            if outcome in ("passed", "failed", "skipped"):
                c[outcome] = c[outcome] + 1
            self._writer.enqueue(MetaMessage(kind="marker", key=name, payload=dict(c)))
