#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sys
from datetime import datetime, timezone

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.begin_execution import main
from tabsdatak.server._tdrunner.function.commands.begin_execution.error import (
    BeginExecutionErrorCode,
    WriteFailedError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- error model (unit) ---


def test_be_1_resolves_to_write_failed_error():
    """BE_1 must resolve to the per-command WriteFailedError leaf
    (not the shared ``_lib.errors.WriteFailedError`` base) so the
    leaf's ``exit_code`` is what reaches the orchestration layer.
    """
    exc = BeginExecutionErrorCode.BE_1.exception(path="/tmp/x.yaml", detail="boom")
    assert isinstance(exc, WriteFailedError)
    assert exc.exit_code == WriteFailedError.exit_code
    assert "/tmp/x.yaml" in str(exc)
    assert "boom" in str(exc)


# --- main (integration) ---


def _set_argv(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["begin-execution", *args])


def _required_args(
    monkeypatch,
    *,
    journal_file,
    liveness,
    responder_url="https://api/w/1",
    responder_method="POST",
    responder_authorization="Bearer tok",
    extra=(),
):
    _set_argv(
        monkeypatch,
        "--journal-file",
        str(journal_file),
        "--liveness",
        str(liveness),
        "--responder-url",
        responder_url,
        "--responder-method",
        responder_method,
        "--responder-authorization",
        responder_authorization,
        *extra,
    )


def test_main_writes_journal_yaml(monkeypatch, tmp_path, http_ok_url):
    journal_file = tmp_path / "journal.yaml"
    before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    _required_args(
        monkeypatch,
        journal_file=journal_file,
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == 0
    written = yaml.safe_load(journal_file.read_text())
    assert isinstance(written, dict)
    assert set(written) == {"start", "end"}
    # begin-execution only stamps `start`; `end` is filled by a later step.
    assert written["end"] is None
    after = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    # start is unix-epoch milliseconds, captured at command time.
    assert before <= written["start"] <= after


def test_main_creates_parent_folders_when_absent(monkeypatch, tmp_path, http_ok_url):
    nested = tmp_path / "deep" / "nested" / "path" / "journal.yaml"
    assert not nested.parent.exists()
    _required_args(
        monkeypatch,
        journal_file=nested,
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == 0
    assert nested.is_file()
    assert "start" in yaml.safe_load(nested.read_text())


def test_main_write_error_returns_command_specific_exit(
    monkeypatch, tmp_path, http_ok_url
):
    # Parent path component is an existing file → mkdir(parents=True)
    # inside Journal.save fails with NotADirectoryError (subclass
    # of OSError) → BeginExecutionErrorCode.BE_1 → WriteFailedError.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    _required_args(
        monkeypatch,
        journal_file=blocker / "subdir" / "journal.yaml",
        liveness=tmp_path / "live",
        responder_url=http_ok_url,
    )
    assert main() == WriteFailedError.exit_code


def test_main_missing_required_option_returns_1(monkeypatch):
    _set_argv(
        monkeypatch,
        "--responder-url",
        "https://api/w/1",
        # missing every other required arg, including --journal-file.
    )
    assert main() == 1
