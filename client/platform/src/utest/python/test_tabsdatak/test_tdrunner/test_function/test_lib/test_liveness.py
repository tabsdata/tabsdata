#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import time

import pytest

from tabsdatak._tdrunner.function._lib import liveness as liveness_module
from tabsdatak._tdrunner.function._lib.liveness import (
    Liveness,
    LivenessFailureError,
    _max_failed_beats_from_env,
    beat,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- beat ---


def test_beat_creates_file(tmp_path):
    p = tmp_path / "live"
    assert not p.exists()
    beat(p)
    assert p.exists()


def test_beat_existing_file_preserves_content(tmp_path):
    p = tmp_path / "live"
    p.write_text("hello")
    beat(p)
    assert p.read_text() == "hello"


def test_beat_updates_mtime(tmp_path):
    p = tmp_path / "live"
    p.touch()
    initial_mtime = p.stat().st_mtime
    # Force the mtime backwards so the next beat is observably newer.
    import os

    os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
    beat(p)
    assert p.stat().st_mtime > initial_mtime - 100.0


def test_beat_missing_parent_raises(tmp_path):
    p = tmp_path / "no" / "such" / "live"
    with pytest.raises(FileNotFoundError):
        beat(p)


# --- Liveness (context-manager only) ---


def test_liveness_context_manager_creates_file(tmp_path):
    p = tmp_path / "live"
    with Liveness(p, interval=10.0):
        pass
    assert p.exists()


def test_liveness_stops_on_exception(tmp_path):
    p = tmp_path / "live"
    live = Liveness(p, interval=10.0)
    with pytest.raises(RuntimeError):
        with live:
            raise RuntimeError("boom")
    assert live._thread is None


def test_liveness_exit_exits_loop_promptly(tmp_path):
    p = tmp_path / "live"
    start = time.monotonic()
    with Liveness(p, interval=60.0):
        pass
    elapsed = time.monotonic() - start
    # If __exit__ didn't signal the Event, the daemon would sleep ~60s.
    assert elapsed < 1.0


def test_liveness_periodic_beats(tmp_path, monkeypatch):
    """The daemon thread emits a beat more than once when intervals elapse."""
    counter = {"n": 0}
    event = __import__("threading").Event()

    def counting_beat(path):
        counter["n"] += 1
        if counter["n"] >= 3:
            event.set()

    monkeypatch.setattr(liveness_module, "beat", counting_beat)

    p = tmp_path / "live"
    with Liveness(p, interval=0.005):
        assert event.wait(timeout=2.0)
    # Initial beat + at least 2 periodic beats
    assert counter["n"] >= 3


def test_liveness_enter_raises_if_parent_missing(tmp_path):
    p = tmp_path / "no" / "such" / "live"
    with pytest.raises(FileNotFoundError):
        with Liveness(p, interval=10.0):
            pytest.fail("body should not execute when __enter__ raises")


def test_liveness_beat_failure_inside_loop_does_not_kill_thread(
    tmp_path, monkeypatch, caplog
):
    """A `beat` failure below the threshold is logged and the loop continues."""
    import threading as _threading

    calls = {"n": 0}
    saw_failure = _threading.Event()
    recovered = _threading.Event()

    def flaky_beat(path):
        calls["n"] += 1
        # Initial beat from __enter__ succeeds (n=1). Next beat fails
        # (n=2). Subsequent beats succeed; we want to observe at least
        # one post-failure success to prove the thread survived.
        if calls["n"] == 2:
            saw_failure.set()
            raise OSError("transient")
        if calls["n"] >= 3:
            recovered.set()

    monkeypatch.setattr(liveness_module, "beat", flaky_beat)

    p = tmp_path / "live"
    with caplog.at_level("WARNING", logger="tabsdatak._tdrunner.function._lib.liveness"):
        with Liveness(p, interval=0.005, max_failed_beats=99):
            assert saw_failure.wait(timeout=2.0)
            assert recovered.wait(timeout=2.0)

    assert calls["n"] >= 3  # at least one recovered beat after failure
    assert any("liveness beat failed" in r.getMessage() for r in caplog.records)


# --- max_failed_beats: env var & escalation ---


def test_max_failed_beats_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("TD_LIVENESS_MAX_FAILED_BEATS", raising=False)
    assert _max_failed_beats_from_env() == 5


def test_max_failed_beats_from_env(monkeypatch):
    monkeypatch.setenv("TD_LIVENESS_MAX_FAILED_BEATS", "12")
    assert _max_failed_beats_from_env() == 12


def test_max_failed_beats_non_integer_falls_back(monkeypatch, caplog):
    monkeypatch.setenv("TD_LIVENESS_MAX_FAILED_BEATS", "not-an-int")
    with caplog.at_level("WARNING", logger="tabsdatak._tdrunner.function._lib.liveness"):
        assert _max_failed_beats_from_env() == 5
    assert any("not an integer" in r.getMessage() for r in caplog.records)


def test_max_failed_beats_below_one_falls_back(monkeypatch, caplog):
    monkeypatch.setenv("TD_LIVENESS_MAX_FAILED_BEATS", "0")
    with caplog.at_level("WARNING", logger="tabsdatak._tdrunner.function._lib.liveness"):
        assert _max_failed_beats_from_env() == 5
    assert any("below 1" in r.getMessage() for r in caplog.records)


def test_liveness_constructor_overrides_env(monkeypatch, tmp_path):
    monkeypatch.setenv("TD_LIVENESS_MAX_FAILED_BEATS", "3")
    live = Liveness(tmp_path / "live", interval=10.0, max_failed_beats=7)
    assert live._max_failed_beats == 7


def test_liveness_threshold_raises_from_exit(tmp_path, monkeypatch, caplog):
    """After max_failed_beats consecutive failures, __exit__ raises."""
    import threading as _threading

    threshold_hit = _threading.Event()

    def always_fail_beat(path, _counter={"n": 0}):
        _counter["n"] += 1
        if _counter["n"] == 1:
            # initial beat in __enter__ — succeed so the thread starts.
            return
        if _counter["n"] >= 4:
            threshold_hit.set()
        raise OSError("persistent disk error")

    monkeypatch.setattr(liveness_module, "beat", always_fail_beat)

    p = tmp_path / "live"
    with caplog.at_level("ERROR", logger="tabsdatak._tdrunner.function._lib.liveness"):
        with pytest.raises(LivenessFailureError, match="3 consecutive times"):
            with Liveness(p, interval=0.005, max_failed_beats=3):
                assert threshold_hit.wait(timeout=2.0)
    assert any("marking stale" in r.getMessage() for r in caplog.records)


def test_liveness_recovery_resets_counter(tmp_path, monkeypatch):
    """Successful beats between failures reset the consecutive counter."""
    import threading as _threading

    # Fail, fail, succeed, fail, fail — max consecutive = 2, threshold = 3.
    # No raise expected.
    pattern = iter([True, False, False, True, False, False])
    done = _threading.Event()
    seen = {"count": 0}

    def patterned_beat(path):
        seen["count"] += 1
        if seen["count"] == 1:
            return  # initial beat in __enter__ always succeeds
        try:
            ok = next(pattern)
        except StopIteration:
            done.set()
            return
        if seen["count"] >= 6:
            done.set()
        if not ok:
            raise OSError("transient")

    monkeypatch.setattr(liveness_module, "beat", patterned_beat)

    p = tmp_path / "live"
    with Liveness(p, interval=0.005, max_failed_beats=3):
        assert done.wait(timeout=2.0)
    # Exited cleanly — counter never reached 3 consecutive failures.


def test_liveness_exit_suppresses_liveness_failure_when_body_raised(
    tmp_path, monkeypatch, caplog
):
    """If the with-body raises, __exit__ doesn't shadow it with LivenessFailureError."""
    import threading as _threading

    threshold_hit = _threading.Event()

    def always_fail(path, _counter={"n": 0}):
        _counter["n"] += 1
        if _counter["n"] == 1:
            return  # initial beat
        if _counter["n"] >= 3:
            threshold_hit.set()
        raise OSError("disk")

    monkeypatch.setattr(liveness_module, "beat", always_fail)

    p = tmp_path / "live"
    with caplog.at_level("ERROR", logger="tabsdatak._tdrunner.function._lib.liveness"):
        with pytest.raises(RuntimeError, match="body error") as exc_info:
            with Liveness(p, interval=0.005, max_failed_beats=2):
                assert threshold_hit.wait(timeout=2.0)
                raise RuntimeError("body error")
    # The body's RuntimeError propagated, NOT LivenessFailureError.
    assert not isinstance(exc_info.value, LivenessFailureError)
    # The liveness failure was logged at ERROR so it's still visible.
    assert any("liveness failure suppressed" in r.getMessage() for r in caplog.records)
