#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for tabsdatax.itest.tdksystems.kafka_producer — schema derivation and the
plain (no-registry) serializers, plus the CLI's kafka load routing.

The pure schema/serialization logic is exercised directly. Avro / Protobuf
round-trips are skipped when those optional libs aren't installed. The
broker / registry calls need live infra and are not covered here."""
from __future__ import annotations

import argparse
import io
import json

import pytest

from tabsdatax.itest.tdksystems import cli, descriptor, env_check, kafka_producer as kp
from tabsdatax.itest.tdksystems.emit import Column
from tabsdatax.itest.tdksystems.errors import DatagenError

COLS = [
    Column("id", "int", nullable=False, auto=True),
    Column("name", "varchar(20)", nullable=False),
    Column("pop", "bigint"),
    Column("lat", "decimal(9,6)"),
    Column("active", "boolean"),
    Column("born", "date"),
    Column("score", "double"),
]


def test_kind_of_mapping():
    assert kp._kind_of("int") == "int"
    assert kp._kind_of("smallint") == "int"
    assert kp._kind_of("bigint") == "long"
    assert kp._kind_of("boolean") == "bool"
    assert kp._kind_of("float") == "float"
    assert kp._kind_of("double") == "double"
    assert kp._kind_of("decimal(9,6)") == "double"   # json data carries float
    assert kp._kind_of("varchar(10)") == "string"
    assert kp._kind_of("date") == "string"           # ISO string in json
    assert kp._kind_of("datetime") == "string"


def test_topic_name():
    assert kp.topic_name("redpanda_IN", "wld_city") == "redpanda_IN.wld_city"
    assert kp.topic_name("", "wld_city") == "wld_city"


def test_safe_name():
    assert kp._safe_name("wld_city") == "wld_city"
    assert kp._safe_name("1bad") == "_1bad"
    assert kp._safe_name("a-b.c") == "a_b_c"


def test_avro_schema_nullability():
    s = kp.avro_schema("wld_city", COLS)
    assert s["type"] == "record" and s["name"] == "wld_city"
    by = {f["name"]: f for f in s["fields"]}
    assert by["id"]["type"] == "int"                  # NOT NULL → bare type
    assert by["pop"]["type"] == ["null", "long"]      # nullable → union
    assert by["pop"]["default"] is None
    assert by["born"]["type"] == ["null", "string"]


def test_json_schema():
    s = kp.json_schema("t", COLS)
    assert s["type"] == "object"
    assert s["properties"]["id"]["type"] == "integer"
    assert s["properties"]["pop"]["type"] == ["integer", "null"]
    assert s["properties"]["lat"]["type"] == ["number", "null"]
    assert "id" in s["required"] and "pop" not in s["required"]


def test_proto_schema():
    p = kp.proto_schema("t", COLS)
    assert 'syntax = "proto3";' in p
    assert "int32 id = 1;" in p
    assert "string name = 2;" in p
    assert "int64 pop = 3;" in p
    assert "double lat = 4;" in p
    assert "bool active = 5;" in p
    assert "string born = 6;" in p


@pytest.mark.parametrize("ser,ext", [
    ("json", "schema.json"), ("avro", "avsc"), ("protobuf", "proto"),
])
def test_write_schema_file(tmp_path, ser, ext):
    p = kp.write_schema_file(tmp_path, "wld_city", COLS, ser)
    assert p.name == f"wld_city.{ext}"
    body = p.read_text().strip()
    assert body
    if ser in ("json", "avro"):
        json.loads(body)  # must be valid JSON


def test_schema_text_rejects_unknown_ser():
    with pytest.raises(DatagenError):
        kp.schema_text("t", COLS, "xml")


def test_plain_json_serializer():
    row = {"id": 1, "name": "Q", "pop": 5, "lat": 1.5,
           "active": True, "born": "2020-01-01", "score": 2.0}
    b = kp._plain_json_serializer("t", COLS)(row)
    assert json.loads(b)["name"] == "Q"


def test_plain_avro_roundtrip():
    fastavro = pytest.importorskip("fastavro")
    row = {"id": 1, "name": "Q", "pop": None, "lat": 1.5,
           "active": True, "born": "2020-01-01", "score": 2.0}
    b = kp._plain_avro_serializer("t", COLS)(row)
    schema = fastavro.parse_schema(kp.avro_schema("t", COLS))
    back = fastavro.schemaless_reader(io.BytesIO(b), schema)
    assert back["name"] == "Q" and back["pop"] is None and back["active"] is True


def test_plain_protobuf_roundtrip():
    pytest.importorskip("google.protobuf")
    row = {"id": 1, "name": "Q", "pop": 5, "lat": 1.5,
           "active": True, "born": "2020-01-01", "score": 2.0}
    b = kp._plain_protobuf_serializer("t", COLS)(row)
    cls = kp._dynamic_proto_class("t", COLS)
    m = cls()
    m.ParseFromString(b)
    assert m.name == "Q" and m.pop == 5 and m.active is True


# --- CLI routing into a kafka domain (produce mocked; no broker) -----------

def _redpanda_domain(tmp_path):
    dom = tmp_path / "redpanda_IN"
    descriptor.write_container_domain(
        out=str(dom), system_type="redpanda", system_kind="kafka",
        host="localhost", port="53999", user="", password="", db="redpanda_IN",
        sqlalchemy_uri="kafka://localhost:53999/redpanda_IN",
        domain_desc="a topic", supports_sql=False,
        extra_vars={"KAFKA_BROKERS": "localhost:53999",
                    "SCHEMA_REGISTRY_URL": "http://localhost:54001"},
    )
    return dom


def test_kafka_load_requires_schemas_and_ser(tmp_path, monkeypatch):
    monkeypatch.setattr(env_check, "check_requirements", lambda: None)
    dom = _redpanda_domain(tmp_path)
    base = dict(model="world", domain=str(dom), size=None, format=None,
                chunks=None, force=False, use_schema_registry=False)
    with pytest.raises(DatagenError, match="--schemas"):
        cli._run_data_load(argparse.Namespace(ser="avro", schemas=None, **base))
    with pytest.raises(DatagenError, match="--ser"):
        cli._run_data_load(argparse.Namespace(ser=None, schemas=str(tmp_path), **base))


def test_kafka_load_routes_to_producer(tmp_path, monkeypatch):
    monkeypatch.setattr(env_check, "check_requirements", lambda: None)
    dom = _redpanda_domain(tmp_path)
    captured = {}

    def fake_produce(**kw):
        captured.update(kw)
        return {f"redpanda_IN.{n}": len(r) for n, _, r in kw["structured"]}

    monkeypatch.setattr(kp, "produce_model", fake_produce)
    args = argparse.Namespace(
        model="world", domain=str(dom), size=None, format=None, chunks=None,
        force=False, ser="avro", schemas=str(tmp_path / "schemas"),
        use_schema_registry=False,
    )
    rc = cli._run_data_load(args)
    assert rc == 0
    assert captured["ser"] == "avro"
    assert captured["domain"] == "redpanda_IN"
    assert captured["brokers"] == "localhost:53999"
    assert captured["registry_url"] == "http://localhost:54001"
    assert {n for n, _, _ in captured["structured"]} == {
        "wld_country", "wld_city", "wld_country_language",
    }
