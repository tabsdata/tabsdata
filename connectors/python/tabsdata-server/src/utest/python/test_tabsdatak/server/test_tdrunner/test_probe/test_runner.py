#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import json
import logging
import time
import urllib.parse
from pathlib import Path

import pytest
import yaml

from tabsdatak.common._catalog import schema_fingerprint
from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.conn.localfile._explorer import LocalFileExplorer
from tabsdatak.server._tdrunner.function._commons.run import command
from tabsdatak.server._tdrunner.function._lib.errors import CommandException
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.probe import main, runner
from tabsdatak.server._tdrunner.probe.runner import (
    MAX_ERROR_BYTES,
    _json_safe,
    load_request,
    run,
    run_probe,
    scrub,
)


def _connection_file(tmp_path: Path, base_path: Path) -> Path:
    path = tmp_path / "connection.yaml"
    path.write_text(
        yaml.safe_dump(LocalFileSrcConn(base_path=str(base_path)).to_dict())
    )
    return path


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    base = tmp_path / "data"
    (base / "sales").mkdir(parents=True)
    (base / "sales" / "orders.csv").write_text("id,total\n1,10\n2,20\n3,30\n")
    (base / "accounts.csv").write_text("id,name\n1,Acme\n")
    return base


@pytest.fixture
def connection_file(tmp_path: Path, data_dir: Path) -> Path:
    return _connection_file(tmp_path, data_dir)


def test_check_succeeds_against_a_reachable_connection(connection_file):
    outcome = run_probe({"probe_id": "p1", "kind": "check"}, connection_file)
    assert outcome == {
        "probe_id": "p1",
        "kind": "check",
        "status": "succeeded",
        "result": {"ok": True},
    }


def test_a_refused_check_is_a_succeeded_probe_reporting_not_ok(tmp_path):
    connection_file = _connection_file(tmp_path, tmp_path / "missing")
    outcome = run_probe({"probe_id": "p1", "kind": "check"}, connection_file)
    assert outcome["status"] == "succeeded"
    assert outcome["result"]["ok"] is False
    assert "NotADirectoryError" in outcome["result"]["error"]


def test_metadata_returns_the_entity_rows_and_the_catalog_fingerprint(
    connection_file, data_dir
):
    outcome = run_probe({"probe_id": "p2", "kind": "metadata"}, connection_file)
    assert outcome["status"] == "succeeded"
    result = outcome["result"]
    fqns = [e["fqn"] for e in result["entities"]]
    assert fqns == sorted(fqns) and len(fqns) == 2
    assert result["entity_count"] == 2
    assert result["truncated"] is False
    row = result["entities"][0]
    assert row["entity_kind"] == "prefix"
    assert row["field_names"] == "id name"
    assert row["field_count"] == 2
    assert row["doc"]["fqn"] == row["fqn"]
    assert [c["name"] for c in row["doc"]["columns"]] == ["id", "name"]

    entities = LocalFileExplorer().metadata(LocalFileSrcConn(base_path=str(data_dir)))
    assert result["schema_hash"] == schema_fingerprint(entities)


def test_metadata_past_the_entity_cap_keeps_the_first_by_fqn(connection_file):
    full = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    capped = run_probe(
        {"probe_id": "p", "kind": "metadata", "max_entities": 1}, connection_file
    )
    assert capped["result"]["truncated"] is True
    assert capped["result"]["entity_count"] == 1
    assert capped["result"]["entities"] == full["result"]["entities"][:1]
    assert capped["result"]["schema_hash"] == full["result"]["schema_hash"]


def test_metadata_past_the_byte_budget_keeps_the_first_by_fqn(connection_file):
    full = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    first = full["result"]["entities"][0]
    budget = len(json.dumps(first).encode("utf-8")) + 1
    capped = run_probe(
        {"probe_id": "p", "kind": "metadata", "max_entities_bytes": budget},
        connection_file,
    )
    assert capped["result"]["entities"] == [first]
    assert capped["result"]["entity_count"] == 1
    assert capped["result"]["truncated"] is True
    assert capped["result"]["schema_hash"] == full["result"]["schema_hash"]


def test_non_finite_floats_become_strings_and_the_outcome_is_strict_json():
    safe = _json_safe([[float("nan"), float("inf"), -float("inf"), 1.5, {"x": float("nan")}]])
    assert safe == [["nan", "inf", "-inf", 1.5, {"x": "nan"}]]
    json.dumps(safe, allow_nan=False)


def test_notify_status_done_never_logs_the_output(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(command, "notify_status", lambda arguments, payload: None)
    journal = tmp_path / "journal.yaml"
    Journal(start=1, end=2).save(journal)
    args = argparse.Namespace(journal_file=str(journal))
    caplog.set_level(logging.DEBUG)
    command.notify_status_done(args, output={"rows": [["SENTINEL-ROW-VALUE"]]})
    assert "SENTINEL-ROW-VALUE" not in caplog.text


def test_sample_reads_rows_of_a_catalog_entity(connection_file):
    metadata = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    orders = next(
        e for e in metadata["result"]["entities"] if e["fqn"].endswith("orders.csv")
    )
    outcome = run_probe(
        {
            "probe_id": "p3",
            "kind": "sample",
            "params": {"entity": orders["fqn"], "limit": 2},
            "entity_doc": orders["doc"],
        },
        connection_file,
    )
    assert outcome["status"] == "succeeded"
    result = outcome["result"]
    assert result["entity"] == orders["fqn"]
    assert result["columns"] == ["id", "total"]
    assert len(result["rows"]) == 2


def test_a_sample_of_several_entities_keeps_each_sample_and_each_failure(
    connection_file, data_dir
):
    metadata = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    docs = [e["doc"] for e in metadata["result"]["entities"]]
    gone = {**docs[0], "fqn": "file:///gone/x.csv", "prefix": str(data_dir / "gone")}
    outcome = run_probe(
        {
            "probe_id": "p4",
            "kind": "sample",
            "params": {"entities": [d["fqn"] for d in docs], "limit": 1},
            "entity_docs": [*docs, gone],
        },
        connection_file,
    )
    assert outcome["status"] == "succeeded"
    samples = outcome["result"]["samples"]
    assert [s["entity"] for s in samples[:2]] == [d["fqn"] for d in docs]
    assert all(len(s["rows"]) == 1 for s in samples[:2])
    assert "error" in samples[2] and "rows" not in samples[2]


def test_several_samples_over_the_byte_cap_lose_rows_from_the_largest(
    connection_file, data_dir, monkeypatch
):
    metadata = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    docs = [e["doc"] for e in metadata["result"]["entities"]]
    request = {"probe_id": "p5", "kind": "sample", "params": {"limit": 3}, "entity_docs": docs}
    full = run_probe(request, connection_file)["result"]["samples"]
    cap = len(json.dumps(full)) - 1
    monkeypatch.setattr(runner, "MAX_SAMPLE_BYTES", cap)
    samples = run_probe(request, connection_file)["result"]["samples"]
    assert len(json.dumps(samples)) <= cap
    largest = max(range(len(full)), key=lambda i: len(full[i]["rows"]))
    assert samples[largest]["truncated"] is True
    assert len(samples[largest]["rows"]) == len(full[largest]["rows"]) - 1


class _FakeExplorer:
    meta_model = None
    entity_type = None
    behaviour = None

    def check_connect(self, conn):
        return type(self).behaviour()

    def metadata(self, conn):
        return type(self).behaviour()


@pytest.fixture
def fake_explorer(monkeypatch):
    monkeypatch.setattr(runner, "explorer_for_conn", lambda conn: _FakeExplorer)
    return _FakeExplorer


def test_a_connector_without_an_explorer_is_unsupported(connection_file, monkeypatch):
    monkeypatch.setattr(runner, "explorer_for_conn", lambda conn: None)
    outcome = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    assert outcome["status"] == "succeeded"
    assert outcome["result"] == {"supported": False}


def test_a_hanging_call_times_out(connection_file, fake_explorer, monkeypatch):
    monkeypatch.setitem(runner.TIMEOUT_SECS, "check", 0.05)
    fake_explorer.behaviour = lambda: time.sleep(5)
    outcome = run_probe({"probe_id": "p", "kind": "check"}, connection_file)
    assert outcome["status"] == "timeout"
    assert outcome["error"]["code"] == "timeout"


def test_a_failed_call_is_reported_without_the_connection_secrets(
    connection_file, fake_explorer, monkeypatch
):
    monkeypatch.setenv("SECRET_CONN__PASSWORD", "hunter2-s3cret")

    def fail():
        raise RuntimeError("login failed for password hunter2-s3cret")

    fake_explorer.behaviour = fail
    outcome = run_probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    assert outcome["status"] == "failed"
    assert outcome["error"]["code"] == "metadata_failed"
    assert "hunter2-s3cret" not in outcome["error"]["message"]
    assert "***" in outcome["error"]["message"]


def test_an_unloadable_connection_fails_the_probe(tmp_path):
    connection_file = tmp_path / "connection.yaml"
    connection_file.write_text("kind: nonsense\n")
    outcome = run_probe({"probe_id": "p", "kind": "check"}, connection_file)
    assert outcome["status"] == "failed"
    assert outcome["error"]["code"] == "check_failed"


def test_scrub_replaces_every_secret_value_and_caps_the_text():
    environ = {
        "SECRET_CONN__USER": "bobby",
        "SECRET_CONN__TOKEN": "bobbytoken",
        "HOME": "x",
    }
    assert scrub("bobby used bobbytoken", environ) == "*** used ***"
    assert scrub("x" * (MAX_ERROR_BYTES * 2), environ) == "x" * MAX_ERROR_BYTES


def test_scrub_catches_url_encoded_and_json_escaped_secrets():
    secret = 'p@ss w/"q"&=\\'
    environ = {"SECRET_CONN__PASSWORD": secret}
    for form in [
        urllib.parse.quote(secret, safe=""),
        urllib.parse.quote_plus(secret),
        json.dumps(secret)[1:-1],
    ]:
        assert form != secret
        scrubbed = scrub(f"failed with {form} at host", environ)
        assert scrubbed == "failed with *** at host", form


def test_scrub_leaves_values_too_short_to_be_secrets():
    environ = {"SECRET_CONN__PORT": "543", "SECRET_CONN__DB": "a"}
    assert scrub("a table on port 543", environ) == "a table on port 543"


def test_scrub_never_splits_a_multibyte_character():
    capped = scrub("é" * MAX_ERROR_BYTES, {})
    assert len(capped.encode("utf-8")) <= MAX_ERROR_BYTES
    assert set(capped) == {"é"}


@pytest.mark.parametrize("content", ["not json", json.dumps({"kind": "drop"})])
def test_an_invalid_request_is_rejected(tmp_path, content):
    path = tmp_path / "request.json"
    path.write_text(content)
    with pytest.raises(CommandException):
        load_request(path)


def test_run_posts_the_outcome_as_the_done_output(
    tmp_path, connection_file, monkeypatch
):
    posted = {}

    def notify_status_done(args, *, output=None, exit_code=0):
        journal = Journal.load(Path(args.journal_file), strict=True)
        posted.update(output=output, start=journal.start, end=journal.end)

    monkeypatch.setattr(runner, "notify_status_done", notify_status_done)
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"probe_id": "p9", "kind": "check"}))
    args = argparse.Namespace(
        journal_file=str(tmp_path / "journal.yaml"),
        request=str(request),
        connection_file=str(connection_file),
    )
    assert run(args) == 0
    assert posted["output"]["probe_id"] == "p9"
    assert posted["output"]["status"] == "succeeded"
    assert posted["start"] <= posted["end"]


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_probe"
