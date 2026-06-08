#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib import transporter_proc
from tabsdatak.server._tdrunner.function._lib.transporter_proc import (
    TRANSPORTER_BINARY_NAME,
    TransporterNotFoundError,
    run_transporter,
)
from tabsdatak.common._interpreter.python.error import BinaryNotFoundError

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


_FAKE_BINARY_PATH = Path("/fake/scripts/td_transporter")


def _install_fake_popen(monkeypatch, *, stdout_lines=(), stderr_lines=(), returncode=0):
    """Stub subprocess.Popen with _FakePopen and stub PythonEnvironment.binary
    so tests exercising the run-path don't need a real td_transporter
    on disk. Argv assertions can then compare against _FAKE_BINARY_PATH.
    """
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
    monkeypatch.setattr(
        transporter_proc.PythonEnvironment,
        "binary",
        lambda self, name: _FAKE_BINARY_PATH,
    )


# --- binary name constant ------------------------------------------------


def test_binary_name_constant():
    """The module-level constant carries just the basename; the platform
    suffix is appended by ``PythonEnvironment.binary`` via sysconfig
    (covered in ``test_venv.py``)."""
    assert TRANSPORTER_BINARY_NAME == "td_transporter"


# --- command shape -------------------------------------------------------


def test_command_shape(monkeypatch, tmp_path):
    """The exec'd argv starts with the absolute path resolved by
    _resolve_binary, not the bare basename."""
    _install_fake_popen(monkeypatch, returncode=0)
    request = tmp_path / "transporter.yaml"
    report = tmp_path / "transporter-report.yaml"
    run_transporter(request, report)
    assert _FakePopen.instances[0].cmd == [
        str(_FAKE_BINARY_PATH),
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


def test_missing_binary_raises_typed_error_before_popen(monkeypatch, tmp_path):
    """The binary-not-found check is up-front (PythonEnvironment.binary
    raises before we ever reach Popen), so if the venv lacks
    td_transporter we raise *before* Popen is ever invoked -- argv[0]
    never gets constructed and there's no chance of a PATH-lookup
    picking up an unrelated binary."""

    def _missing(self, name):
        raise BinaryNotFoundError(name=name, scripts=tmp_path)

    monkeypatch.setattr(transporter_proc.PythonEnvironment, "binary", _missing)

    popen_invocations: list = []

    def _spy_popen(cmd, **kwargs):
        popen_invocations.append(cmd)
        raise AssertionError("Popen should not be reached when binary is missing")

    monkeypatch.setattr(subprocess, "Popen", _spy_popen)

    with pytest.raises(TransporterNotFoundError) as exc_info:
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")
    assert popen_invocations == []
    assert isinstance(exc_info.value.__cause__, BinaryNotFoundError)


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
