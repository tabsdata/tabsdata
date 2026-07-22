#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for
:class:`~tabsdatak.conn.common.sql.cdc._sentinel.disk.DiskSentinel`
covering the ``should_flush`` threshold arms (row / byte / time / force /
empty), the positional ``build_manifest`` contract, and ``clear``.
Parquet writing (``process_section``) is not exercised here."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from tabsdatak.conn.common.sql.cdc._sentinel.disk import DiskSentinel
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


def _disk(*, max_rows=1_000, max_bytes=None, max_secs=10_000) -> DiskSentinel:
    return DiskSentinel(
        landing=Path("/tmp"),
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_secs=max_secs,
        tables={"s.a": None, "s.b": None},
        cdc_format=CdcFormat(),
    )


def test_should_flush_false_when_empty_even_with_force():
    disk = _disk()
    assert disk.should_flush(force=True) is False


def test_should_flush_on_row_threshold():
    disk = _disk(max_rows=5)
    disk._total_rows = 5
    assert disk.should_flush() is True


def test_should_flush_on_byte_threshold():
    disk = _disk(max_rows=1_000, max_bytes=100)
    disk._total_rows = 1
    disk._total_bytes = 100
    assert disk.should_flush() is True


def test_should_flush_on_time_threshold():
    disk = _disk(max_secs=60)
    disk._total_rows = 1
    disk._last_flushed = time.time() - 61
    assert disk.should_flush() is True


def test_should_flush_on_force_with_data():
    disk = _disk()
    disk._total_rows = 1
    assert disk.should_flush(force=True) is True


def test_should_flush_false_below_all_thresholds():
    disk = _disk(max_rows=1_000, max_bytes=1_000, max_secs=10_000)
    disk._total_rows = 1
    disk._total_bytes = 1
    assert disk.should_flush(force=False) is False


def test_build_manifest_is_positional_with_empty_slots():
    disk = _disk()
    disk._files["s.b"].append(Path("b0.parquet"))
    assert disk.build_manifest() == [[], [Path("b0.parquet")]]


def test_clear_resets_files_and_counters():
    disk = _disk()
    disk._files["s.a"].append(Path("a0.parquet"))
    disk._total_rows = 3
    disk._total_bytes = 99

    disk.clear()

    assert disk.build_manifest() == [[], []]
    assert disk.is_empty is True
    assert disk._total_bytes == 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
