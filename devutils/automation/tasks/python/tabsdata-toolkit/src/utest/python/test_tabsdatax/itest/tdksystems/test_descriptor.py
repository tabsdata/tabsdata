"""Tests for tabsdatax.itest.tdksystems.descriptor — writing connection settings, the
`stamp` that attaches the tdx identity (name/started/model/data), and the
`info` renderer (json/export/unset, with unprefixed keys)."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import descriptor
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- name validation -------------------------------------------------------

@pytest.mark.parametrize("bad", ["", "not-an-ident", "1leading", "a b", "x.y"])
def test_validate_name_rejects_non_identifiers(bad):
    with pytest.raises(DatagenError, match="must be an identifier"):
        descriptor.validate_name(bad)


def test_validate_name_accepts_identifier():
    descriptor.validate_name("my_db_2")   # no raise


# ---- write_* produce a self-describing descriptor; stamp adds identity ----

def _load(path):
    # descriptors are encrypted on disk; load() decrypts + parses.
    return descriptor.load(path)


def test_container_system_descriptor_shape(tmp_path):
    out = tmp_path / "mysql.json"
    descriptor.write_container_system(
        out=str(out), system_kind="sql", system_type="mysql", label="MySQL",
        container="mysmoke", host="localhost", port="3306",
        user="root", password="secret", domain_desc="a database",
    )
    doc = _load(out)
    assert doc["generator"] == "tdx"
    assert doc["kind"] == "container-system"
    assert doc["category"] == "container"   # picks the helper module family
    assert doc["system"] == "mysql"         # picks the specific backend
    assert doc["role"] == "sys"
    # Fresh write has no identity yet — stamp attaches it.
    assert "name" not in doc
    descriptor.stamp(out, name="my_db", started="2026-07-01T00:00:00Z")
    doc = _load(out)
    assert doc["name"] == "my_db"
    assert doc["started"] == "2026-07-01T00:00:00Z"
    meta = {e["name"]: e.get("value") for e in doc["meta"]}
    assert meta["CONTAINER"] == "mysmoke" and meta["PASSWORD"] == "secret"


def test_stamp_records_model_and_data_and_extra_meta(tmp_path):
    out = tmp_path / "mydb.json"
    descriptor.write_container_domain(
        out=str(out), system_kind="sql", system_type="mysql",
        host="h", port="3306", user="u", password="p", db="mydb",
        sqlalchemy_uri="mysql://x", domain_desc="d", supports_sql=True,
    )
    descriptor.stamp(out, name="inbox", started="t", model="world", data="xs",
                     extra_meta={"SYS_CONTAINER": "c", "SYS_ADMIN_PASSWORD": "pw"})
    doc = _load(out)
    assert doc["model"] == "world" and doc["data"] == "xs"
    meta = {e["name"]: e.get("value") for e in doc["meta"]}
    assert meta["SYS_CONTAINER"] == "c" and meta["SYS_ADMIN_PASSWORD"] == "pw"
    # Admin back-reference is not part of the info (exported) set.
    assert "SYS_ADMIN_PASSWORD" not in doc["exported"]


def test_cloud_and_saas_categories(tmp_path):
    s3 = tmp_path / "s3.json"
    descriptor.write_cloud_system(
        out=str(s3), storage_type="s3", system_kind="file", label="s3",
        sys_path="root", sys_path_env=None, forwarded=[("BUCKET", "b", None)])
    sf = tmp_path / "snow.json"
    descriptor.write_saas_system(
        out=str(sf), saas_type="snowflake", system_kind="sql", label="Snowflake",
        forwarded=[("ACCOUNT", "a", None)])
    assert _load(s3)["category"] == "cloud" and _load(s3)["system"] == "s3"
    assert _load(sf)["category"] == "saas" and _load(sf)["system"] == "snowflake"


# ---- resolve: env-backed values + slash strip ------------------------------

def test_resolve_reads_env_backed_value(tmp_path, monkeypatch):
    out = tmp_path / "s3.json"
    descriptor.write_cloud_system(
        out=str(out), storage_type="s3", system_kind="file", label="s3",
        sys_path=None, sys_path_env="MY_ROOT",
        forwarded=[("REGION", None, "AWS_REGION")])
    doc = descriptor.load(out)
    monkeypatch.delenv("MY_ROOT", raising=False)
    with pytest.raises(DatagenError, match="MY_ROOT env var must be set"):
        descriptor.resolve(doc)
    monkeypatch.setenv("MY_ROOT", "data")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    meta = descriptor.resolve(doc)
    assert meta["SYS_PATH"] == "data"
    assert meta["REGION"] == "us-east-1"


def test_resolve_strips_leading_slash(tmp_path, capsys):
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="/xp-s3", sys_path_env=None, base_path="/xp-s3/s3_OUT",
        forwarded=[])
    meta = descriptor.resolve(descriptor.load(out), warn=True)
    assert meta["SYS_PATH"] == "xp-s3"
    assert meta["BASE_PATH"] == "xp-s3/s3_OUT"
    assert capsys.readouterr().err.count("WARNING") == 1


# ---- info: json / export / unset, all with unprefixed keys ----------------

def _stamped_domain(tmp_path):
    out = tmp_path / "mydb.json"
    descriptor.write_container_domain(
        out=str(out), system_kind="sql", system_type="mysql",
        host="h", port="3306", user="u", password="it's a secret", db="mydb",
        sqlalchemy_uri="mysql://x", domain_desc="d", supports_sql=True)
    descriptor.stamp(out, name="inbox", started="2026-07-01T00:00:00Z",
                     model="world", data="xs")
    return descriptor.load(out)


def test_info_json_has_identity_and_connection(tmp_path):
    obj = json.loads(descriptor.info(_stamped_domain(tmp_path), "json"))
    assert obj["NAME"] == "inbox"
    assert obj["TYPE"] == "mysql"
    assert obj["STARTED"] == "2026-07-01T00:00:00Z"
    assert obj["MODEL"] == "world"
    assert obj["DATA"] == "xs"
    assert obj["DB"] == "mydb"


def test_info_export_is_shell_quoted(tmp_path):
    text = descriptor.info(_stamped_domain(tmp_path), "export")
    assert "export NAME=inbox" in text
    assert "export PASSWORD='it'\"'\"'s a secret'" in text


def test_info_unset_lists_same_keys(tmp_path):
    doc = _stamped_domain(tmp_path)
    exported = set(json.loads(descriptor.info(doc, "json")))
    unset_keys = {ln.split()[1] for ln in descriptor.info(doc, "unset").splitlines()}
    assert exported == unset_keys


def test_info_rejects_bad_format(tmp_path):
    with pytest.raises(DatagenError, match="--format"):
        descriptor.info(_stamped_domain(tmp_path), "yaml")


# ---- load / is_descriptor --------------------------------------------------

def test_is_descriptor(tmp_path):
    good = tmp_path / "s.json"
    descriptor.write_cloud_system(out=str(good), storage_type="fs",
                                  system_kind="file", label="fs",
                                  sys_path="/tmp/x", sys_path_env=None,
                                  forwarded=[], strip_sys_path_slash=False)
    assert descriptor.is_descriptor(good)
    plain = tmp_path / "notes.txt"
    plain.write_text("not json")
    assert not descriptor.is_descriptor(plain)


def test_load_rejects_non_descriptor(tmp_path):
    p = tmp_path / "x.json"
    p.write_text('{"hello": 1}')
    with pytest.raises(DatagenError, match="not a tdx descriptor"):
        descriptor.load(p)
