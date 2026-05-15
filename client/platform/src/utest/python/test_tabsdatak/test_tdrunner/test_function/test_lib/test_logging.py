#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

import pytest

from tabsdatak._tdrunner.function._lib.logging import TRACE, setup_logging

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_trace_level_registered():
    assert logging.getLevelName(TRACE) == "TRACE"
    assert TRACE < logging.DEBUG


def test_setup_logging_returns_named_logger():
    logger = setup_logging("test.tdrunner.return")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test.tdrunner.return"


def test_setup_logging_does_not_propagate():
    logger = setup_logging("test.tdrunner.no_propagate")
    assert logger.propagate is False


def test_info_goes_to_stdout(capsys):
    logger = setup_logging("test.tdrunner.info")
    logger.info("hello info")
    captured = capsys.readouterr()
    assert "hello info" in captured.out
    assert "hello info" not in captured.err


def test_debug_goes_to_stdout_when_enabled(capsys):
    logger = setup_logging("test.tdrunner.debug", level=logging.DEBUG)
    logger.debug("hello debug")
    captured = capsys.readouterr()
    assert "hello debug" in captured.out
    assert "hello debug" not in captured.err


def test_trace_goes_to_stdout_when_enabled(capsys):
    logger = setup_logging("test.tdrunner.trace", level=TRACE)
    logger.log(TRACE, "hello trace")
    captured = capsys.readouterr()
    assert "hello trace" in captured.out
    assert "hello trace" not in captured.err


def test_logger_trace_method_emits_at_trace_level(capsys):
    """logger.trace(...) is the ergonomic equivalent of logger.log(TRACE, ...)."""
    logger = setup_logging("test.tdrunner.trace_method", level=TRACE)
    logger.trace("hello via .trace()")
    captured = capsys.readouterr()
    assert "hello via .trace()" in captured.out
    assert "hello via .trace()" not in captured.err


def test_logger_trace_method_respects_logger_level(capsys):
    """When the logger is at INFO, .trace() is filtered."""
    logger = setup_logging("test.tdrunner.trace_filtered")
    logger.trace("should not appear")
    captured = capsys.readouterr()
    assert "should not appear" not in captured.out
    assert "should not appear" not in captured.err


def test_logger_trace_method_available_on_module_loggers():
    """The patch applies to every Logger, not just ones from setup_logging."""
    fresh = logging.getLogger("test.tdrunner.fresh_logger")
    assert callable(fresh.trace)


def test_warning_goes_to_stderr(capsys):
    logger = setup_logging("test.tdrunner.warning")
    logger.warning("hello warning")
    captured = capsys.readouterr()
    assert "hello warning" in captured.err
    assert "hello warning" not in captured.out


def test_error_goes_to_stderr(capsys):
    logger = setup_logging("test.tdrunner.error")
    logger.error("hello error")
    captured = capsys.readouterr()
    assert "hello error" in captured.err
    assert "hello error" not in captured.out


def test_critical_goes_to_stderr(capsys):
    logger = setup_logging("test.tdrunner.critical")
    logger.critical("hello critical")
    captured = capsys.readouterr()
    assert "hello critical" in captured.err
    assert "hello critical" not in captured.out


def test_setup_logging_idempotent_no_duplicate_handlers(capsys):
    """Calling setup_logging twice on the same name replaces handlers
    rather than stacking them."""
    logger1 = setup_logging("test.tdrunner.idempotent")
    logger2 = setup_logging("test.tdrunner.idempotent")
    assert logger1 is logger2
    logger1.info("once")
    captured = capsys.readouterr()
    assert captured.out.count("once") == 1


def test_default_level_silences_debug(capsys):
    logger = setup_logging("test.tdrunner.default_level")
    logger.debug("should not appear")
    captured = capsys.readouterr()
    assert "should not appear" not in captured.out
    assert "should not appear" not in captured.err
