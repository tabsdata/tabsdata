#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
import threading

import pytest

from tabsdatak.server._tdrunner.function._lib.liveness import (
    LivenessLogHandler,
    beat,
    install_liveness_handler,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- beat ----------------------------------------------------------------


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
    # Force mtime backwards so the next beat is observably newer.
    os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
    beat(p)
    assert p.stat().st_mtime > initial_mtime - 100.0


def test_beat_missing_parent_raises(tmp_path):
    p = tmp_path / "no" / "such" / "live"
    with pytest.raises(FileNotFoundError):
        beat(p)


# --- LivenessLogHandler --------------------------------------------------


def _make_record(level: int = logging.INFO, msg: str = "x") -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=level,
        pathname=__file__,
        lineno=0,
        msg=msg,
        args=None,
        exc_info=None,
    )


def test_handler_touches_on_construction(tmp_path):
    p = tmp_path / "live"
    assert not p.exists()
    LivenessLogHandler(p)
    assert p.exists()


def test_handler_emit_touches_file(tmp_path):
    p = tmp_path / "live"
    h = LivenessLogHandler(p)
    initial_mtime = p.stat().st_mtime
    # Force mtime backwards so the post-emit touch is observably newer.
    os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
    h.emit(_make_record())
    assert p.stat().st_mtime > initial_mtime - 100.0


def test_handler_construction_swallows_missing_parent(tmp_path):
    """Construction with a missing parent dir doesn't raise; emit
    surfaces the failure via handleError instead."""
    p = tmp_path / "no" / "such" / "live"
    h = LivenessLogHandler(p)
    # No exception during construction. emit will fail and go through
    # handleError. We monkey the handler to capture it.
    captured: list[logging.LogRecord] = []
    h.handleError = lambda record: captured.append(record)  # type: ignore[method-assign]
    h.emit(_make_record())
    assert len(captured) == 1


def test_handler_emit_routes_touch_errors_through_handleError(tmp_path, monkeypatch):
    p = tmp_path / "live"
    h = LivenessLogHandler(p)

    captured: list[logging.LogRecord] = []
    h.handleError = lambda record: captured.append(record)  # type: ignore[method-assign]

    def boom(_self):
        raise OSError("disk full")

    monkeypatch.setattr("pathlib.Path.touch", boom)
    h.emit(_make_record())
    assert len(captured) == 1


def test_handler_survives_concurrent_emits(tmp_path):
    """Many threads emitting simultaneously must not crash. Each touch
    is idempotent — a few extra syscalls under contention is fine."""
    p = tmp_path / "live"
    h = LivenessLogHandler(p)

    def hammer():
        for _ in range(100):
            h.emit(_make_record())

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert p.exists()


# --- install_liveness_handler -------------------------------------------


def _cleanup_handler(handler: LivenessLogHandler):
    logging.getLogger().removeHandler(handler)


def test_install_attaches_handler_to_root_logger(tmp_path):
    p = tmp_path / "live"
    h = install_liveness_handler(p)
    try:
        assert h in logging.getLogger().handlers
    finally:
        _cleanup_handler(h)


def test_install_returns_the_installed_handler(tmp_path):
    p = tmp_path / "live"
    h = install_liveness_handler(p)
    try:
        assert isinstance(h, LivenessLogHandler)
    finally:
        _cleanup_handler(h)


def test_install_creates_file_immediately(tmp_path):
    p = tmp_path / "live"
    h = install_liveness_handler(p)
    try:
        assert p.exists()
    finally:
        _cleanup_handler(h)


def test_install_is_idempotent_removes_prior_liveness_handler(tmp_path):
    """A second install replaces the first — never accumulates."""
    p1 = tmp_path / "live1"
    p2 = tmp_path / "live2"
    h1 = install_liveness_handler(p1)
    h2 = install_liveness_handler(p2)
    try:
        handlers = [
            h for h in logging.getLogger().handlers if isinstance(h, LivenessLogHandler)
        ]
        assert handlers == [h2]
        assert h1 not in handlers
    finally:
        _cleanup_handler(h2)


def test_install_does_not_remove_unrelated_handlers(tmp_path):
    """Only previous LivenessLogHandlers are removed; stream handlers etc. stay."""
    p = tmp_path / "live"
    other = logging.StreamHandler()
    logging.getLogger().addHandler(other)
    try:
        h = install_liveness_handler(p)
        try:
            assert other in logging.getLogger().handlers
            assert h in logging.getLogger().handlers
        finally:
            _cleanup_handler(h)
    finally:
        logging.getLogger().removeHandler(other)


def test_install_bumps_root_level_so_info_records_reach_handler(tmp_path):
    """After install, an INFO record from any child logger reaches the
    handler — i.e. the root logger isn't filtering it out at level."""
    root = logging.getLogger()
    original_level = root.level
    try:
        root.setLevel(logging.WARNING)  # would suppress INFO
        p = tmp_path / "live"
        h = install_liveness_handler(p)
        try:
            initial_mtime = p.stat().st_mtime
            os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
            logging.getLogger("tabsdatak.child").info("hello")
            assert p.stat().st_mtime > initial_mtime - 100.0
        finally:
            _cleanup_handler(h)
    finally:
        root.setLevel(original_level)


def test_install_catches_records_from_third_party_loggers(tmp_path):
    """Records from any top-level logger (sqlalchemy, polars, boto3,
    …) propagate to root and tick the handler. This is the whole
    point of attaching at root rather than at ``tabsdatak``.

    We use a synthetic logger name rather than a real library name —
    libraries actually loaded in the test process (e.g. sqlalchemy)
    configure their own loggers' levels and can suppress an INFO
    record at the emitting logger before it ever propagates to
    root, which would have nothing to do with our install path.
    """
    p = tmp_path / "live"
    h = install_liveness_handler(p)
    try:
        initial_mtime = p.stat().st_mtime
        os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
        logging.getLogger("td_test.fake_third_party_lib").info("hello")
        assert p.stat().st_mtime > initial_mtime - 100.0
    finally:
        _cleanup_handler(h)


def test_install_logger_emit_propagates_to_handler(tmp_path):
    """End-to-end: a logger.info() call from a sub-logger ticks the
    file via the installed handler. Covers the path tdrunner code
    relies on."""
    p = tmp_path / "live"
    h = install_liveness_handler(p)
    try:
        initial_mtime = p.stat().st_mtime
        os.utime(p, (initial_mtime - 100.0, initial_mtime - 100.0))
        logging.getLogger("tabsdatak.server._tdrunner.function.commands.x").info("x")
        assert p.stat().st_mtime > initial_mtime - 100.0
    finally:
        _cleanup_handler(h)
