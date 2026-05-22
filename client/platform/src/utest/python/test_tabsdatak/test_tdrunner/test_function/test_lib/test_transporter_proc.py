#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import subprocess
import sys

import pytest

from tabsdatak._tdrunner.function._lib import transporter_proc
from tabsdatak._tdrunner.function._lib.transporter_proc import (
    TRANSPORTER_BINARY,
    TransporterNotFoundError,
    _binary_name,
    run_transporter,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- helpers --------------------------------------------------------------


class _FakeStream:
    """Iterates over the lines we hand it, then closes."""

    def __init__(self, lines):
        self._lines = list(lines)
        self.closed = False

    def __iter__(self):
        yield from self._lines

    def close(self):
        self.closed = True


class _FakePopen:
    """Stand-in for subprocess.Popen captured by tests.

    Records the argv and kwargs it was constructed with, hands the
    reader threads pre-baked stdout/stderr line lists, and returns a
    configurable exit code from wait().
    """

    instances: list["_FakePopen"] = []

    def __init__(
        self, cmd, *, stdout_lines=(), stderr_lines=(), returncode=0, **kwargs
    ):
        self.cmd = list(cmd)
        self.kwargs = kwargs
        self.stdout = _FakeStream(stdout_lines)
        self.stderr = _FakeStream(stderr_lines)
        self._returncode = returncode
        _FakePopen.instances.append(self)

    def wait(self):
        return self._returncode


def _install_fake_popen(monkeypatch, *, stdout_lines=(), stderr_lines=(), returncode=0):
    _FakePopen.instances.clear()

    def _factory(cmd, **kwargs):
        return _FakePopen(
            cmd,
            stdout_lines=stdout_lines,
            stderr_lines=stderr_lines,
            returncode=returncode,
            **kwargs,
        )

    monkeypatch.setattr(subprocess, "Popen", _factory)


# --- platform-aware binary name ------------------------------------------


def test_binary_name_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    assert _binary_name() == "td_transporter.exe"


def test_binary_name_non_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    assert _binary_name() == "td_transporter"


def test_binary_name_other_non_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    assert _binary_name() == "td_transporter"


def test_module_constant_matches_current_platform():
    expected = "td_transporter.exe" if sys.platform == "win32" else "td_transporter"
    assert TRANSPORTER_BINARY == expected


# --- command shape -------------------------------------------------------


def test_command_shape(monkeypatch, tmp_path):
    _install_fake_popen(monkeypatch, returncode=0)
    request = tmp_path / "transporter.yaml"
    report = tmp_path / "transporter-report.yaml"
    run_transporter(request, report)
    assert _FakePopen.instances[0].cmd == [
        TRANSPORTER_BINARY,
        "--request",
        str(request),
        "--report",
        str(report),
    ]


def test_popen_uses_line_buffered_text_pipes(monkeypatch, tmp_path):
    """Pipes must be opened in text mode with bufsize=1 so each line
    ships as soon as the binary emits it — without this the parent
    could sit blocked on a full pipe and starve the liveness ticks."""
    _install_fake_popen(monkeypatch, returncode=0)
    run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")
    kw = _FakePopen.instances[0].kwargs
    assert kw["stdout"] == subprocess.PIPE
    assert kw["stderr"] == subprocess.PIPE
    assert kw["bufsize"] == 1
    assert kw["text"] is True


# --- exit code propagation -----------------------------------------------


def test_returns_zero_on_success(monkeypatch, tmp_path):
    _install_fake_popen(monkeypatch, returncode=0)
    assert run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml") == 0


@pytest.mark.parametrize("rc", [1, 2, 42, 127])
def test_returns_nonzero_exit_code(monkeypatch, tmp_path, rc):
    _install_fake_popen(monkeypatch, returncode=rc)
    assert run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml") == rc


# --- missing binary ------------------------------------------------------


def test_missing_binary_raises_typed_error(monkeypatch, tmp_path):
    def _raises(cmd, **kwargs):
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(subprocess, "Popen", _raises)
    with pytest.raises(TransporterNotFoundError):
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")


def test_typed_error_inherits_filenotfound():
    """Catchable as the stdlib FileNotFoundError."""
    assert isinstance(TransporterNotFoundError("missing"), FileNotFoundError)


# --- stdout/stderr forwarding through logger -----------------------------


def test_stdout_lines_forwarded_at_info(monkeypatch, tmp_path, caplog):
    _install_fake_popen(
        monkeypatch,
        stdout_lines=["copying file 1/3\n", "copying file 2/3\n"],
        returncode=0,
    )
    with caplog.at_level(logging.DEBUG, logger=transporter_proc.__name__):
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")

    info_msgs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and r.name == transporter_proc.__name__
    ]
    assert any("copying file 1/3" in m for m in info_msgs)
    assert any("copying file 2/3" in m for m in info_msgs)


def test_stderr_lines_forwarded_at_warning(monkeypatch, tmp_path, caplog):
    _install_fake_popen(
        monkeypatch,
        stderr_lines=["slow source detected\n"],
        returncode=0,
    )
    with caplog.at_level(logging.DEBUG, logger=transporter_proc.__name__):
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")

    warn_msgs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == transporter_proc.__name__
    ]
    assert any("slow source detected" in m for m in warn_msgs)


def test_streams_closed_after_run(monkeypatch, tmp_path):
    _install_fake_popen(
        monkeypatch,
        stdout_lines=["a\n", "b\n"],
        stderr_lines=["x\n"],
        returncode=0,
    )
    run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")
    inst = _FakePopen.instances[0]
    assert inst.stdout.closed
    assert inst.stderr.closed


def test_lines_logged_without_trailing_newline(monkeypatch, tmp_path, caplog):
    """rstrip() strips the trailing newline so the log line is clean."""
    _install_fake_popen(
        monkeypatch,
        stdout_lines=["pretty output\n"],
        returncode=0,
    )
    with caplog.at_level(logging.DEBUG, logger=transporter_proc.__name__):
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")
    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.endswith("pretty output") for m in msgs)
    assert not any(m.endswith("\n") for m in msgs)
