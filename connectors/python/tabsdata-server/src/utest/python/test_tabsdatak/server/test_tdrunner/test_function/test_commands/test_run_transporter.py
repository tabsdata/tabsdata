#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
)
from tabsdatak.server._tdrunner.function.commands.run_transporter import main
from tabsdatak.server._tdrunner.function.commands.run_transporter.error import (
    BinaryMissingError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

_MOD = "tabsdatak.server._tdrunner.function.commands.run_transporter"


def _set_argv(
    monkeypatch,
    request_path: Path,
    report: Path,
    liveness: Path,
    *,
    responder_url,
    responder_method="POST",
    responder_authorization="Bearer tok",
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-transporter",
            "--request",
            str(request_path),
            "--report",
            str(report),
            "--liveness",
            str(liveness),
            "--responder-url",
            responder_url,
            "--responder-method",
            responder_method,
            "--responder-authorization",
            responder_authorization,
            "--journal-file",
            "/tmp/em.yaml",
        ],
    )


def test_main_returns_zero_when_transporter_succeeds(monkeypatch, tmp_path, http_ok_url):
    captured: dict = {}

    def fake(request_yaml, report):
        captured["request_yaml"] = request_yaml
        captured["report"] = report
        return 0

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    request = tmp_path / "transporter.yaml"
    report = tmp_path / "transporter-report.yaml"
    liveness = tmp_path / "live"
    _set_argv(monkeypatch, request, report, liveness, responder_url=http_ok_url)
    rc = main()
    assert rc == 0
    assert captured["request_yaml"] == request
    assert captured["report"] == report
    # Installing the handler creates the liveness file before any
    # transporter work runs.
    assert liveness.exists()


@pytest.mark.parametrize("rc", [1, 2, 7, 42])
def test_main_propagates_nonzero_exit(monkeypatch, tmp_path, http_ok_url, rc):
    monkeypatch.setattr(f"{_MOD}.run_transporter", lambda req, report: rc)
    _set_argv(
        monkeypatch,
        tmp_path / "x.yaml",
        tmp_path / "r.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == rc


def test_main_missing_binary_returns_command_specific(monkeypatch, tmp_path, http_ok_url):
    def fake(req, report):
        raise TransporterNotFoundError("nope")

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    _set_argv(
        monkeypatch,
        tmp_path / "x.yaml",
        tmp_path / "r.yaml",
        tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == BinaryMissingError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run-transporter"])
    assert main() == 1


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_transporter"
