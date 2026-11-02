#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
import yaml

from tabsdatak.common._catalog import schema_fingerprint
from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.conn.localfile._explorer import LocalFileExplorer
from tabsdatak.server._tdrunner.probe._handlers.connection import explorer
from tabsdatak.server._tdrunner.probe._handlers.connection.explorer import (
    ConnectionExplorerProbe,
    json_safe,
)
from tabsdatak.server._tdrunner.probe._handlers.connection.request import Kind, ConnectionProbeRequest

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def connection(tmp_path: Path, base_path: Path) -> Path:
    path = tmp_path / "connection.yaml"
    path.write_text(
        yaml.safe_dump(LocalFileSrcConn(base_path=str(base_path)).to_dict())
    )
    return path


def probe(request: dict, connection_file: Path) -> dict:
    return ConnectionExplorerProbe(connection_file).run(ConnectionProbeRequest.of(request))


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    base = tmp_path / "data"
    (base / "sales").mkdir(parents=True)
    (base / "sales" / "orders.csv").write_text("id,total\n1,10\n2,20\n3,30\n")
    (base / "accounts.csv").write_text("id,name\n1,Acme\n")
    return base


@pytest.fixture
def connection_file(tmp_path: Path, data_dir: Path) -> Path:
    return connection(tmp_path, data_dir)


def test_check_succeeds_against_a_reachable_connection(connection_file):
    outcome = probe({"probe_id": "p1", "kind": "check"}, connection_file)
    assert outcome == {
        "probe_id": "p1",
        "kind": "check",
        "status": "succeeded",
        "result": {"ok": True},
    }


def test_a_refused_check_is_a_succeeded_probe_reporting_not_ok(tmp_path):
    connection_file = connection(tmp_path, tmp_path / "missing")
    outcome = probe({"probe_id": "p1", "kind": "check"}, connection_file)
    assert outcome["status"] == "succeeded"
    assert outcome["result"]["ok"] is False
    assert "NotADirectoryError" in outcome["result"]["error"]


def test_metadata_returns_the_entity_rows_and_the_catalog_fingerprint(
    connection_file, data_dir
):
    outcome = probe({"probe_id": "p2", "kind": "metadata"}, connection_file)
    assert outcome["status"] == "succeeded"
    result = outcome["result"]
    fqns = [entity["fqn"] for entity in result["entities"]]
    assert fqns == sorted(fqns) and len(fqns) == 2
    assert result["entity_count"] == 2
    assert result["truncated"] is False
    row = result["entities"][0]
    assert row["entity_kind"] == "prefix"
    assert row["field_names"] == "id name"
    assert row["field_count"] == 2
    assert row["doc"]["fqn"] == row["fqn"]
    assert [column["name"] for column in row["doc"]["columns"]] == ["id", "name"]
    entities = LocalFileExplorer().metadata(LocalFileSrcConn(base_path=str(data_dir)))
    assert result["schema_hash"] == schema_fingerprint(entities)


def test_metadata_past_the_entity_cap_keeps_the_first_by_fqn(connection_file):
    full = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    capped = probe(
        {"probe_id": "p", "kind": "metadata", "max_entities": 1}, connection_file
    )
    assert capped["result"]["truncated"] is True
    assert capped["result"]["entity_count"] == 1
    assert capped["result"]["entities"] == full["result"]["entities"][:1]
    assert capped["result"]["schema_hash"] == full["result"]["schema_hash"]


def test_metadata_past_the_byte_budget_keeps_the_first_by_fqn(connection_file):
    full = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    first = full["result"]["entities"][0]
    budget = len(json.dumps(first).encode("utf-8")) + 1
    capped = probe(
        {"probe_id": "p", "kind": "metadata", "max_entities_bytes": budget},
        connection_file,
    )
    assert capped["result"]["entities"] == [first]
    assert capped["result"]["entity_count"] == 1
    assert capped["result"]["truncated"] is True
    assert capped["result"]["schema_hash"] == full["result"]["schema_hash"]


def test_non_finite_floats_become_strings_and_the_outcome_is_strict_json():
    safe = json_safe([[float("nan"), float("inf"), -float("inf"), 1.5, {"x": float("nan")}]])
    assert safe == [["nan", "inf", "-inf", 1.5, {"x": "nan"}]]
    json.dumps(safe, allow_nan=False)


def test_sample_reads_rows_of_a_catalog_entity(connection_file):
    metadata = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    orders = next(
        entity
        for entity in metadata["result"]["entities"]
        if entity["fqn"].endswith("orders.csv")
    )
    outcome = probe(
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
    metadata = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    docs = [entity["doc"] for entity in metadata["result"]["entities"]]
    gone = {**docs[0], "fqn": "file:///gone/x.csv", "prefix": str(data_dir / "gone")}
    outcome = probe(
        {
            "probe_id": "p4",
            "kind": "sample",
            "params": {"entities": [doc["fqn"] for doc in docs], "limit": 1},
            "entity_docs": [*docs, gone],
        },
        connection_file,
    )
    assert outcome["status"] == "succeeded"
    samples = outcome["result"]["samples"]
    assert [sample["entity"] for sample in samples[:2]] == [doc["fqn"] for doc in docs]
    assert all(len(sample["rows"]) == 1 for sample in samples[:2])
    assert "error" in samples[2] and "rows" not in samples[2]


def test_several_samples_over_the_byte_cap_lose_rows_from_the_largest(
    connection_file, monkeypatch
):
    metadata = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    docs = [entity["doc"] for entity in metadata["result"]["entities"]]
    request = {
        "probe_id": "p5",
        "kind": "sample",
        "params": {"limit": 3},
        "entity_docs": docs,
    }
    full = probe(request, connection_file)["result"]["samples"]
    cap = len(json.dumps(full)) - 1
    monkeypatch.setattr(explorer, "MAX_SAMPLE_BYTES", cap)
    samples = probe(request, connection_file)["result"]["samples"]
    assert len(json.dumps(samples)) <= cap
    largest = max(range(len(full)), key=lambda index: len(full[index]["rows"]))
    assert samples[largest]["truncated"] is True
    assert len(samples[largest]["rows"]) == len(full[largest]["rows"]) - 1


class FakeExplorer:
    meta_model = None
    entity_type = None
    behaviour = None

    def check_connect(self, conn):
        return type(self).behaviour()

    def metadata(self, conn):
        return type(self).behaviour()


@pytest.fixture
def fake_explorer(monkeypatch):
    monkeypatch.setattr(explorer, "explorer_for_conn", lambda conn: FakeExplorer)
    return FakeExplorer


def test_a_connector_without_an_explorer_is_unsupported(connection_file, monkeypatch):
    monkeypatch.setattr(explorer, "explorer_for_conn", lambda conn: None)
    outcome = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    assert outcome["status"] == "succeeded"
    assert outcome["result"] == {"supported": False}


def test_a_hanging_call_times_out(connection_file, fake_explorer, monkeypatch):
    monkeypatch.setitem(explorer.TIMEOUT_SECONDS, Kind.CHECK, 0.05)
    fake_explorer.behaviour = lambda: time.sleep(5)
    outcome = probe({"probe_id": "p", "kind": "check"}, connection_file)
    assert outcome["status"] == "timeout"
    assert outcome["error"]["code"] == "timeout"


def test_a_failed_call_is_reported_without_the_connection_secrets(
    connection_file, fake_explorer, monkeypatch
):
    monkeypatch.setenv("SECRET_CONN__PASSWORD", "hunter2-s3cret")

    def fail():
        raise RuntimeError("login failed for password hunter2-s3cret")

    fake_explorer.behaviour = fail
    outcome = probe({"probe_id": "p", "kind": "metadata"}, connection_file)
    assert outcome["status"] == "failed"
    assert outcome["error"]["code"] == "metadata_failed"
    assert "hunter2-s3cret" not in outcome["error"]["message"]
    assert "***" in outcome["error"]["message"]


def test_an_unloadable_connection_fails_the_probe(tmp_path):
    connection_file = tmp_path / "connection.yaml"
    connection_file.write_text("kind: nonsense\n")
    outcome = probe({"probe_id": "p", "kind": "check"}, connection_file)
    assert outcome["status"] == "failed"
    assert outcome["error"]["code"] == "check_failed"
