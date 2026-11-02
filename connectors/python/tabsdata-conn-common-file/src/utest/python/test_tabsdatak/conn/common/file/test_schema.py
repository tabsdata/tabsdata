#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
import os

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tabsdatak.conn.common.file import _schema
from tabsdatak.conn.common.file._explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
)
from tabsdatak.conn.common.file._schema import attach_schemas, read_schema

META = FileStorageConnMetaModel()


def _entity(prefix: str, name: str, modified: str = "2026-09-30T00:00:00") -> FileStorageEntity:
    fmt = os.path.splitext(name)[1][1:]
    return FileStorageEntity(
        connection_type=META,
        scheme="file",
        prefix=prefix,
        file=FilePattern(pattern=name, format=fmt),
        last_modified=modified,
    )


def _open(entity: FileStorageEntity):
    return open(os.path.join(entity.prefix, entity.file.pattern), "rb")


def _columns(columns) -> list[tuple[str, str]]:
    return [(c.name, c.type) for c in columns]


def test_csv_columns_and_types_come_from_the_first_lines():
    data = b"id,email,amount,due,paid,note\n1,a@x.com,120.5,2026-10-01,true,\n2,b@y.com,99,2026-10-05,false,late\n"
    assert _columns(read_schema("csv", io.BytesIO(data))) == [
        ("id", "int"),
        ("email", "string"),
        ("amount", "float"),
        ("due", "date"),
        ("paid", "bool"),
        ("note", "string"),
    ]
    assert _columns(read_schema("tsv", io.BytesIO(b"a\tb\n1\tx\n"))) == [("a", "int"), ("b", "string")]


def test_a_csv_line_cut_by_the_head_limit_is_left_out(monkeypatch):
    monkeypatch.setattr(_schema, "HEAD_BYTES", 16)
    data = b"id,name\n1,ann\n22222,bbbbbbbbbbbbb\n"
    assert _columns(read_schema("csv", io.BytesIO(data))) == [("id", "int"), ("name", "string")]


def test_ndjson_keys_are_merged_over_the_first_lines():
    lines = [{"id": 1, "at": "2026-09-30T10:00:00Z"}, {"id": 2.5, "tags": ["a"], "meta": {"k": 1}, "at": None}]
    data = "\n".join(json.dumps(line) for line in lines).encode()
    assert _columns(read_schema("ndjson", io.BytesIO(data))) == [
        ("id", "float"),
        ("at", "datetime"),
        ("tags", "list"),
        ("meta", "object"),
    ]


def test_parquet_and_avro_schemas_come_from_their_footer_and_header(tmp_path):
    table = pa.table({"id": pa.array([1], pa.int64()), "name": pa.array(["a"], pa.string())})
    pq.write_table(table, tmp_path / "t.parquet")
    with open(tmp_path / "t.parquet", "rb") as handle:
        assert _columns(read_schema("parquet", handle)) == [("id", "int64"), ("name", "string")]
    schema = {
        "type": "record",
        "name": "r",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "email", "type": ["null", "string"]},
            {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        ],
    }
    buffer = io.BytesIO()
    fastavro.writer(buffer, schema, [{"id": 1, "email": None, "at": 0}])
    buffer.seek(0)
    assert _columns(read_schema("avro", buffer)) == [
        ("id", "long"),
        ("email", "string"),
        ("at", "timestamp-millis"),
    ]


def test_files_of_one_prefix_and_format_share_the_schema_their_newest_files_agree_on(tmp_path):
    for day in range(1, 6):
        (tmp_path / f"orders_{day}.csv").write_text("id,total\n1,2.5\n")
    (tmp_path / "notes.bin").write_bytes(b"\x00\x01")
    entities = [_entity(str(tmp_path), f"orders_{d}.csv", f"2026-09-0{d}") for d in range(1, 6)]
    entities.append(_entity(str(tmp_path), "notes.bin"))
    reads = []

    def opener(entity):
        reads.append(entity.file.pattern)
        return _open(entity)

    entities = attach_schemas(entities, opener)
    assert sorted(reads) == ["orders_4.csv", "orders_5.csv"]
    for entity in entities[:5]:
        assert _columns(entity.columns) == [("id", "int"), ("total", "float")]
    newest = entities[4]
    assert newest.schema_from == ""
    assert entities[0].schema_from == newest.fqn()
    assert entities[5].columns == [] and entities[5].field_names() == []
    assert entities[0].field_names() == ["id", "total"]
    assert entities[0].to_dict()["columns"] == [{"name": "id", "type": "int"}, {"name": "total", "type": "float"}]
    restored = FileStorageEntity.from_dict(entities[0].to_dict(), META)
    assert restored.schema_from == newest.fqn() and restored.field_names() == ["id", "total"]


def test_when_the_newest_files_disagree_every_file_is_read_for_its_own_schema(tmp_path):
    (tmp_path / "a.csv").write_text("id\n1\n")
    (tmp_path / "b.csv").write_text("name\nx\n")
    (tmp_path / "c.csv").write_text("id\n1\n")
    entities = [
        _entity(str(tmp_path), "a.csv", "2026-09-01"),
        _entity(str(tmp_path), "b.csv", "2026-09-03"),
        _entity(str(tmp_path), "c.csv", "2026-09-02"),
    ]
    entities = attach_schemas(entities, _open)
    assert [e.field_names() for e in entities] == [["id"], ["name"], ["id"]]
    assert all(not e.schema_from for e in entities)


def test_one_table_per_file_folders_give_every_file_its_own_columns(tmp_path):
    tables = {
        "sap_ecc_fi": {
            "BKPF": ["BUKRS", "BELNR", "GJAHR"],
            "BSEG": ["BUKRS", "BELNR", "BUZEI", "HKONT"],
            "BSID": ["BUKRS", "KUNNR", "DMBTR"],
        },
        "sfdc_raw": {
            "Account": ["Id", "Name", "Industry"],
            "Lead": ["Id", "Email", "Company"],
            "Opportunity": ["Id", "AccountId", "Amount"],
        },
        "sap_ecc_sd": {"VBAK": ["VBELN", "ERDAT"], "VBAP": ["VBELN", "POSNR"]},
    }
    entities = []
    for folder, files in tables.items():
        (tmp_path / folder).mkdir()
        for table, columns in files.items():
            (tmp_path / folder / f"{table}.csv").write_text(
                ",".join(columns) + "\n" + ",".join(f"{c}_1" for c in columns) + "\n"
            )
            entities.append(_entity(str(tmp_path / folder), f"{table}.csv"))
    reads = []

    def opener(entity):
        reads.append(entity.file.pattern)
        return _open(entity)

    entities = attach_schemas(entities, opener)
    assert sorted(reads) == sorted(f"{t}.csv" for files in tables.values() for t in files)
    expected = [columns for files in tables.values() for columns in files.values()]
    assert [e.field_names() for e in entities] == expected
    assert all(not e.schema_from for e in entities)


def test_files_of_a_mixed_folder_past_the_cap_keep_no_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(_schema, "MAX_SCHEMA_READS", 3)
    for index in range(5):
        (tmp_path / f"t{index}.csv").write_text(f"c{index}\n1\n")
    entities = [_entity(str(tmp_path), f"t{i}.csv", f"2026-09-0{i + 1}") for i in range(5)]
    reads = []

    def opener(entity):
        reads.append(entity.file.pattern)
        return _open(entity)

    entities = attach_schemas(entities, opener)
    assert sorted(reads) == ["t2.csv", "t3.csv", "t4.csv"]
    assert [e.field_names() for e in entities] == [[], [], ["c2"], ["c3"], ["c4"]]


def test_an_unreadable_file_keeps_no_schema_and_shares_none(tmp_path):
    (tmp_path / "a.csv").write_text("id\n1\n")
    entities = [_entity(str(tmp_path), "a.csv", "2026-09-02"), _entity(str(tmp_path), "gone.csv", "2026-09-03")]
    entities.append(_entity(str(tmp_path), "old.csv", "2026-09-01"))
    entities = attach_schemas(entities, _open)
    assert [e.field_names() for e in entities] == [["id"], [], []]


def test_reads_stop_at_the_cap_and_past_the_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(_schema, "MAX_SCHEMA_READS", 3)
    prefixes = []
    for index in range(4):
        prefix = tmp_path / f"p{index}"
        prefix.mkdir()
        (prefix / "a.csv").write_text("id\n1\n")
        (prefix / "b.csv").write_text("id\n1\n")
        prefixes.append(str(prefix))
    entities = [_entity(p, n) for p in prefixes for n in ("a.csv", "b.csv")]
    reads = []

    def opener(entity):
        reads.append(entity.fqn())
        return _open(entity)

    attach_schemas(entities, opener)
    assert len(reads) == 3

    ticks = iter([0.0] + [_schema.READ_BUDGET_S + 1] * 100)
    late = [_entity(p, n) for p in prefixes for n in ("a.csv", "b.csv")]
    late = attach_schemas(late, _open, clock=lambda: next(ticks))
    assert all(not e.columns for e in late)


@pytest.mark.parametrize("fmt", ["csv", "ndjson"])
def test_columns_are_capped(fmt, monkeypatch):
    monkeypatch.setattr(_schema, "MAX_COLUMNS", 2)
    data = b"a,b,c\n1,2,3\n" if fmt == "csv" else b'{"a": 1, "b": 2, "c": 3}\n'
    assert [c.name for c in read_schema(fmt, io.BytesIO(data))] == ["a", "b"]


def test_rewriting_an_older_file_with_the_same_schema_keeps_the_fingerprint(tmp_path):
    from tabsdatak.common._catalog import schema_fingerprint

    for name in ("a.csv", "b.csv", "c.csv"):
        (tmp_path / name).write_text("id,total\n1,2.5\n")
    days = {"a.csv": "2026-09-01", "b.csv": "2026-09-02", "c.csv": "2026-09-03"}
    before = attach_schemas([_entity(str(tmp_path), n, d) for n, d in days.items()], _open)
    days["a.csv"] = "2026-09-04"
    after = attach_schemas([_entity(str(tmp_path), n, d) for n, d in days.items()], _open)
    assert [e.schema_from for e in before] != [e.schema_from for e in after]
    assert schema_fingerprint(after) == schema_fingerprint(before)
