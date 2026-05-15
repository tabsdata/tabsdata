#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import time

import pytest

from tabsdatak._tdrunner.function._lib import liveness as liveness_module
from tabsdatak._tdrunner.function._lib.liveness import Liveness, touch

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- touch ---


def test_touch_creates_file(tmp_path):
    p = tmp_path / "live"
    assert not p.exists()
    touch(p)
    assert p.exists()


def test_touch_existing_file_preserves_content(tmp_path):
    p = tmp_path / "live"
    p.write_text("hello")
    touch(p)
    assert p.read_text() == "hello"


def test_touch_updates_mtime(tmp_path):
    p = tmp_path / "live"
    p.touch()
    initial_mtime = p.stat().st_mtime
    # Force the mtime backwards so the next touch is observably newer.
    import os

    os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
    touch(p)
    assert p.stat().st_mtime > initial_mtime - 100.0


def test_touch_missing_parent_raises(tmp_path):
    p = tmp_path / "no" / "such" / "live"
    with pytest.raises(FileNotFoundError):
        touch(p)


# --- Liveness ---


def test_liveness_context_manager_creates_file(tmp_path):
    p = tmp_path / "live"
    with Liveness(p, interval=10.0):
        pass
    assert p.exists()


def test_liveness_explicit_start_stop(tmp_path):
    p = tmp_path / "live"
    live = Liveness(p, interval=10.0)
    live.start()
    try:
        assert p.exists()
    finally:
        live.stop()


def test_liveness_double_start_is_idempotent(tmp_path):
    p = tmp_path / "live"
    live = Liveness(p, interval=10.0)
    live.start()
    try:
        first_thread = live._thread
        live.start()  # no-op
        assert live._thread is first_thread
    finally:
        live.stop()


def test_liveness_stop_without_start_is_noop(tmp_path):
    p = tmp_path / "live"
    Liveness(p, interval=10.0).stop()  # must not raise


def test_liveness_double_stop_is_idempotent(tmp_path):
    p = tmp_path / "live"
    live = Liveness(p, interval=10.0)
    live.start()
    live.stop()
    live.stop()  # second stop is no-op
    assert live._thread is None


def test_liveness_stops_on_exception(tmp_path):
    p = tmp_path / "live"
    live = Liveness(p, interval=10.0)
    with pytest.raises(RuntimeError):
        with live:
            raise RuntimeError("boom")
    assert live._thread is None


def test_liveness_stop_exits_loop_promptly(tmp_path):
    p = tmp_path / "live"
    live = Liveness(p, interval=60.0)
    live.start()
    start = time.monotonic()
    live.stop()
    elapsed = time.monotonic() - start
    # If stop didn't signal the Event, the thread would sleep ~60s.
    assert elapsed < 1.0


def test_liveness_periodic_touches(tmp_path, monkeypatch):
    """The daemon thread runs `touch` more than once when intervals elapse."""
    counter = {"n": 0}
    event = __import__("threading").Event()

    def counting_touch(path):
        counter["n"] += 1
        if counter["n"] >= 3:
            event.set()

    monkeypatch.setattr(liveness_module, "touch", counting_touch)

    p = tmp_path / "live"
    with Liveness(p, interval=0.005):
        assert event.wait(timeout=2.0)
    # Initial touch + at least 2 periodic ticks
    assert counter["n"] >= 3


def test_liveness_start_raises_if_parent_missing(tmp_path):
    p = tmp_path / "no" / "such" / "live"
    live = Liveness(p, interval=10.0)
    with pytest.raises(FileNotFoundError):
        live.start()
    assert live._thread is None
