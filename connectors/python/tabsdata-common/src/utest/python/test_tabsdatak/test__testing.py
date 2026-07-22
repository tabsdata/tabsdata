#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the shared pytest helpers in
``test_tabsdatak._testing``: ``setup_tests_logging`` and
``NewlineOnFirstLog``."""

from __future__ import annotations

import logging

import pytest

# noinspection PyProtectedMember
from test_tabsdatak._testing import NewlineOnFirstLog, setup_tests_logging


def _make_record(msg: str, level: int = logging.ERROR) -> logging.LogRecord:
    return logging.LogRecord(
        name="t", level=level, pathname="", lineno=0, msg=msg, args=None, exc_info=None
    )


def test_newline_on_first_log_prepends_newline_on_first_record():
    f = NewlineOnFirstLog()
    r = _make_record("hello")
    f.filter(r)
    assert r.msg == "\nhello"


def test_newline_on_first_log_does_not_prepend_on_subsequent_records():
    f = NewlineOnFirstLog()
    r1 = _make_record("first")
    r2 = _make_record("second")
    f.filter(r1)
    f.filter(r2)
    assert r1.msg == "\nfirst"
    assert r2.msg == "second"


def test_newline_on_first_log_filter_returns_critical_or_above_on_unix(monkeypatch):
    monkeypatch.setattr("platform.system", lambda: "Linux")
    f = NewlineOnFirstLog()
    # The first record always passes through the mutation, so use a
    # second record to test the level gate without the mutation noise.
    f.filter(_make_record("seed"))
    assert f.filter(_make_record("low", level=logging.INFO)) is False
    assert f.filter(_make_record("high", level=logging.CRITICAL)) is True


def test_newline_on_first_log_filter_drops_everything_on_windows(monkeypatch):
    monkeypatch.setattr("platform.system", lambda: "Windows")
    f = NewlineOnFirstLog()
    f.filter(_make_record("seed"))
    assert f.filter(_make_record("any", level=logging.CRITICAL)) is False


@pytest.fixture
def _restore_root_logger():
    """Snapshot/restore the root logger so tests don't leak state."""
    root = logging.getLogger(None)
    saved_level = root.level
    saved_handlers = list(root.handlers)
    saved_propagate = root.propagate
    saved_filelock = logging.getLogger("filelock").level
    yield
    root.setLevel(saved_level)
    root.handlers[:] = saved_handlers
    root.propagate = saved_propagate
    logging.getLogger("filelock").setLevel(saved_filelock)


def test_setup_tests_logging_configures_root_at_error_with_one_rich_handler(
    _restore_root_logger,
):
    from rich.logging import RichHandler

    logger = setup_tests_logging()
    assert logger is logging.getLogger(None)
    assert logger.level == logging.ERROR
    assert logger.propagate is True
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], RichHandler)


def test_setup_tests_logging_bumps_filelock_to_info(_restore_root_logger):
    setup_tests_logging()
    assert logging.getLogger("filelock").level == logging.INFO


def test_setup_tests_logging_handler_has_newline_on_first_log_filter(
    _restore_root_logger,
):
    logger = setup_tests_logging()
    handler = logger.handlers[0]
    assert any(isinstance(f, NewlineOnFirstLog) for f in handler.filters)
