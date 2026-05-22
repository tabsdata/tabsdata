#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import subprocess
import sys

import pytest

from tabsdatak._tdrunner.function._lib.transporter_proc import (
    TRANSPORTER_BINARY,
    TransporterNotFoundError,
    _binary_name,
    run_transporter,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _stub_completed(returncode: int):
    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(args=cmd, returncode=returncode)

    return fake_run


# --- platform-aware binary name ---


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


# --- command shape ---


def test_command_shape(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = list(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    request = tmp_path / "transporter.yaml"
    report = tmp_path / "transporter-report.yaml"
    run_transporter(request, report)
    assert captured["cmd"] == [
        TRANSPORTER_BINARY,
        "--request",
        str(request),
        "--report",
        str(report),
    ]


# --- exit code propagation ---


def test_returns_zero_on_success(monkeypatch, tmp_path):
    monkeypatch.setattr(subprocess, "run", _stub_completed(0))
    assert run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml") == 0


@pytest.mark.parametrize("rc", [1, 2, 42, 127])
def test_returns_nonzero_exit_code(monkeypatch, tmp_path, rc):
    monkeypatch.setattr(subprocess, "run", _stub_completed(rc))
    assert run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml") == rc


# --- missing binary ---


def test_missing_binary_raises_typed_error(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(TransporterNotFoundError):
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml")


def test_typed_error_inherits_filenotfound():
    """Catchable as the stdlib FileNotFoundError."""
    err = TransporterNotFoundError("missing")
    assert isinstance(err, FileNotFoundError)


# --- liveness context manager ---


def test_no_liveness_runs_directly(monkeypatch, tmp_path):
    monkeypatch.setattr(subprocess, "run", _stub_completed(0))
    assert (
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml", liveness=None) == 0
    )


def test_liveness_context_is_entered_and_exited(monkeypatch, tmp_path):
    monkeypatch.setattr(subprocess, "run", _stub_completed(0))

    events: list[str] = []

    class FakeLiveness:
        def __enter__(self):
            events.append("enter")
            return self

        def __exit__(self, *exc):
            events.append("exit")

    rc = run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml", FakeLiveness())
    assert rc == 0
    assert events == ["enter", "exit"]


def test_liveness_exits_on_subprocess_exception(monkeypatch, tmp_path):
    """If the subprocess call itself raises, the liveness context still
    exits cleanly so its background thread is stopped."""

    def fake_run(cmd, **kwargs):
        raise FileNotFoundError("boom")

    monkeypatch.setattr(subprocess, "run", fake_run)

    events: list[str] = []

    class FakeLiveness:
        def __enter__(self):
            events.append("enter")
            return self

        def __exit__(self, *exc):
            events.append("exit")

    with pytest.raises(TransporterNotFoundError):
        run_transporter(tmp_path / "x.yaml", tmp_path / "r.yaml", FakeLiveness())
    assert events == ["enter", "exit"]
