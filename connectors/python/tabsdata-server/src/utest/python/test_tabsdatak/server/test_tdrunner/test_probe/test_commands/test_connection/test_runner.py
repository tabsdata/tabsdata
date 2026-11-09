#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.server._tdrunner.function._commons.document.journal import Journal
from tabsdatak.server._tdrunner.function._commons.run.command import Responder
from tabsdatak.server._tdrunner.probe.commands.connection import main
from tabsdatak.server._tdrunner.probe.commands.connection.error import (
    BadRequestError,
)
from tabsdatak.server._tdrunner.probe.commands.connection.options import (
    ConnectionProbeArguments,
)
from tabsdatak.server._tdrunner.probe.commands.connection.runner import (
    ConnectionProbe,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@pytest.fixture
def connection_file(tmp_path: Path) -> Path:
    base = tmp_path / "data"
    base.mkdir()
    path = tmp_path / "connection.yaml"
    path.write_text(yaml.safe_dump(LocalFileSrcConn(base_path=str(base)).to_dict()))
    return path


def arguments(tmp_path: Path, request: Path, connection_file: Path):
    return ConnectionProbeArguments(
        liveness=tmp_path / "liveness",
        responder_url="http://127.0.0.1:1",
        responder_method="POST",
        responder_authorization="Bearer probe",
        journal_file=tmp_path / "journal.yaml",
        request=request,
        connection_file=connection_file,
    )


def test_the_outcome_is_posted_as_the_done_output(
    tmp_path, connection_file, monkeypatch
):
    posted = []
    monkeypatch.setattr(
        Responder, "report", lambda responder, payload: posted.append(payload)
    )
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"probe_id": "p9", "kind": "check"}))
    probe = ConnectionProbe(arguments(tmp_path, request, connection_file))
    assert probe.run() == 0
    [payload] = posted
    assert payload.output["probe_id"] == "p9"
    assert payload.output["status"] == "succeeded"
    assert payload.start <= payload.end


def test_the_done_report_never_logs_the_output(
    tmp_path, connection_file, monkeypatch, caplog
):
    monkeypatch.setattr(Responder, "notify", lambda responder, body, what: None)
    probe_arguments = arguments(tmp_path, tmp_path / "request.json", connection_file)
    Journal(start=1, end=2).save(probe_arguments.journal_file)
    Responder(probe_arguments).done(output={"rows": [["SENTINEL-ROW-VALUE"]]})
    assert "SENTINEL-ROW-VALUE" not in caplog.text


@pytest.mark.parametrize(
    "content",
    ["not json", json.dumps({"kind": "drop"})],
    ids=["not json", "unknown kind"],
)
def test_an_invalid_request_is_refused(tmp_path, connection_file, content):
    request = tmp_path / "request.json"
    request.write_text(content)
    probe = ConnectionProbe(arguments(tmp_path, request, connection_file))
    with pytest.raises(BadRequestError, match="CP_1"):
        probe.run()


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td-probe-connection"
