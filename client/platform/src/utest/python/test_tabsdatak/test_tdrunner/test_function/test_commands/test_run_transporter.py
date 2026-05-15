#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tabsdatak._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
)
from tabsdatak._tdrunner.function.commands.run_transporter import (
    EXIT_BINARY_MISSING,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

_MOD = "tabsdatak._tdrunner.function.commands.run_transporter"


def _set_argv(monkeypatch, request_path: Path, liveness: Path):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-transporter",
            "--request",
            str(request_path),
            "--liveness",
            str(liveness),
        ],
    )


def test_main_returns_zero_when_transporter_succeeds(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(request_yaml, liveness=None):
        captured["request_yaml"] = request_yaml
        captured["had_liveness"] = liveness is not None
        return 0

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    request = tmp_path / "transporter.yaml"
    _set_argv(monkeypatch, request, tmp_path / "live")
    rc = main()
    assert rc == 0
    assert captured["request_yaml"] == request
    assert captured["had_liveness"] is True


@pytest.mark.parametrize("rc", [1, 2, 7, 42])
def test_main_propagates_nonzero_exit(monkeypatch, tmp_path, rc):
    monkeypatch.setattr(f"{_MOD}.run_transporter", lambda req, liveness=None: rc)
    _set_argv(monkeypatch, tmp_path / "x.yaml", tmp_path / "live")
    assert main() == rc


def test_main_missing_binary_returns_command_specific(monkeypatch, tmp_path):
    def fake(req, liveness=None):
        raise TransporterNotFoundError("nope")

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    _set_argv(monkeypatch, tmp_path / "x.yaml", tmp_path / "live")
    assert main() == EXIT_BINARY_MISSING


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run-transporter"])
    assert main() == 1


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_transporter"
