#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from types import SimpleNamespace

from tabsdatak.cli.td.execution_utils import worker_status_label


def _worker(worker_type: str, status: str, **kwargs):
    return SimpleNamespace(type=worker_type, status=status, kwargs=kwargs)


def test_a_failed_stream_with_a_retry_says_when_it_restarts():
    label = worker_status_label(
        _worker("ST", "F", retry_at=1_800_000_000_000, failures=3)
    )
    assert label.startswith("F (retrying at ")
    assert label.endswith(", failure 3 in a row)")


def test_a_failed_stream_without_a_retry_needs_action():
    assert worker_status_label(_worker("ST", "F", retry_at=None)) == (
        "F (needs action)"
    )


def test_other_workers_show_their_status():
    assert worker_status_label(_worker("ST", "R", retry_at=None)) == "R"
    assert worker_status_label(_worker("F", "F", retry_at=None)) == "F"
