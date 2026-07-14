"""Tests for tabsdatax.itest.tdksystems.descriptor — writing connection settings, the
`stamp` that attaches the tdx identity (name/started/model/data), and the
`info` renderer (json/export/unset, with unprefixed keys)."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import descriptor, systems
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.systems.base import Setting


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


def test_uri_without_credentials():
    f = descriptor._uri_without_credentials
    assert (
        f("mysql+mysqlconnector://u:p@h:3306/db")
        == "mysql+mysqlconnector://h:3306/db"
    )
    # SQLAlchemy driver schemes with '_' (db2) are handled; the query is kept.
    assert (
        f("db2+ibm_db://db2inst1:pw@h:50000/DB?currentSchema=s")
        == "db2+ibm_db://h:50000/DB?currentSchema=s"
    )
    # No userinfo, or no scheme -> returned unchanged.
    assert f("clickhouse+http://h:8123") == "clickhouse+http://h:8123"
    assert f("kafka://h:9092/topic") == "kafka://h:9092/topic"
    assert f("not-a-uri") == "not-a-uri"


def test_container_domain_exports_credentials_free_uri(tmp_path):
    out = tmp_path / "mydb.json"
    descriptor.write_container_domain(
        out=str(out), system_kind="sql", system_type="mysql",
        host="h", port="3306", user="finance", password="s3cr3t", db="finance_db",
        sqlalchemy_uri="mysql+mysqlconnector://finance:s3cr3t@h:3306/finance_db",
        domain_desc="d", supports_sql=True,
    )
    attrs = descriptor.attributes(_load(out))
    # A credentials-free URI is surfaced alongside the credential-bearing one.
    assert attrs["URI"] == "mysql+mysqlconnector://h:3306/finance_db"
    assert (
        attrs["SQLALCHEMY_URI"]
        == "mysql+mysqlconnector://finance:s3cr3t@h:3306/finance_db"
    )
    assert "s3cr3t" not in attrs["URI"]
    assert "URI" in _load(out)["exported"]


def test_stamp_adds_redundant_entity_term_key(tmp_path):
    # fs: the entity container is a "folder" — stamp adds a redundant FOLDER key
    # (== the external entity name) to meta + exported.
    out = tmp_path / "inbox.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="fs", system_kind="file",
        sys_path="/tmp/x", sys_path_env=None, base_path="/tmp/x/pfx_inbox",
        forwarded=[], strip_sys_path_slash=False)
    descriptor.stamp(out, name="inbox", started="t", external_name="pfx_inbox",
                     entity_term="folder")
    attrs = descriptor.attributes(_load(out))
    assert attrs["FOLDER"] == "pfx_inbox"
    assert attrs["EXTERNAL_NAME"] == "pfx_inbox"
    assert "FOLDER" in _load(out)["exported"]


def test_stamp_skips_entity_term_when_key_already_present(tmp_path):
    # bigquery bakes DATASET itself; stamp must not add a duplicate DATASET.
    out = tmp_path / "ds.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="bigquery", system_kind="file",
        sys_path="root", sys_path_env=None, base_path="root/pfx_ds",
        forwarded=[("DATASET", "pfx_ds", None)])
    descriptor.stamp(out, name="ds", started="t", external_name="pfx_ds",
                     entity_term="dataset")
    doc = _load(out)
    assert [e["name"] for e in doc["meta"]].count("DATASET") == 1
    assert doc["exported"].count("DATASET") == 1


def test_record_entities_surfaced_by_info_and_attributes(tmp_path):
    doc = _stamped_domain(tmp_path)
    # _stamped_domain wrote to mydb.json; record entities on it.
    out = tmp_path / "mydb.json"
    descriptor.record_entities(out, {"wld_city": 100, "wld_country": 239})
    doc = _load(out)
    attrs = descriptor.attributes(doc)
    assert attrs["ENTITIES"] == {"wld_city": 100, "wld_country": 239}
    obj = json.loads(descriptor.info(doc, "json"))
    assert obj["ENTITIES"] == {"wld_city": 100, "wld_country": 239}


def test_info_export_renders_entities_as_json(tmp_path):
    out = tmp_path / "mydb.json"
    _stamped_domain(tmp_path)
    descriptor.record_entities(out, {"wld_city": 100})
    text = descriptor.info(_load(out), "export")
    assert "export ENTITIES='{\"wld_city\":100}'" in text
    unset = {ln.split()[1] for ln in descriptor.info(_load(out), "unset").splitlines()}
    assert "ENTITIES" in unset


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


# ---- resolve: baked values + env marker + slash strip ----------------------

def test_resolve_uses_baked_value_not_env(tmp_path, monkeypatch):
    out = tmp_path / "s3.json"
    descriptor.write_cloud_system(
        out=str(out), storage_type="s3", system_kind="file", label="s3",
        sys_path="data", sys_path_env="MY_ROOT",
        forwarded=[("REGION", "us-east-1", "AWS_REGION")])
    doc = descriptor.load(out)
    # Values are baked at create, so resolve reads nothing from the environment.
    monkeypatch.delenv("MY_ROOT", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    meta = descriptor.resolve(doc)
    assert meta["SYS_PATH"] == "data"
    assert meta["REGION"] == "us-east-1"
    # The source env var names survive in the raw descriptor for a manifest dump.
    raw = {e["name"]: e.get("env") for e in doc["meta"]}
    assert raw["SYS_PATH"] == "MY_ROOT"
    assert raw["REGION"] == "AWS_REGION"


def test_write_bakes_env_backed_settings(tmp_path, monkeypatch):
    # write() resolves an env-backed setting at create time and bakes the value,
    # so status/destroy/info later work without the env set (the descriptor is
    # encrypted at rest). azureblob is the motivating case: the account key is
    # only in $AZ__ACCOUNT_KEY at create.
    az = systems.get("azureblob")
    settings = {
        "CONTAINER": Setting("CONTAINER", env_var="AZ__CONTAINER"),
        "ACCOUNT_NAME": Setting("ACCOUNT_NAME", env_var="AZ__ACCOUNT_NAME"),
        "ACCOUNT_KEY": Setting("ACCOUNT_KEY", env_var="AZ__ACCOUNT_KEY"),
        "SYS_PATH": Setting("SYS_PATH", env_var="AZ__SYS_PATH", is_path=True),
    }
    monkeypatch.setenv("AZ__CONTAINER", "c")
    monkeypatch.setenv("AZ__ACCOUNT_NAME", "acct")
    monkeypatch.setenv("AZ__ACCOUNT_KEY", "supersecret")
    monkeypatch.setenv("AZ__SYS_PATH", "root")
    out = tmp_path / "az.json"
    descriptor.write(az, "sys", str(out), settings)

    for var in ("AZ__CONTAINER", "AZ__ACCOUNT_NAME", "AZ__ACCOUNT_KEY", "AZ__SYS_PATH"):
        monkeypatch.delenv(var, raising=False)
    meta = descriptor.resolve(_load(str(out)))
    assert meta["ACCOUNT_KEY"] == "supersecret"
    assert meta["CONTAINER"] == "c"
    assert meta["SYS_PATH"] == "root"
    # The source env var names survive on the raw entries for a manifest dump.
    raw = {e["name"]: e.get("env") for e in _load(str(out))["meta"]}
    assert raw["ACCOUNT_KEY"] == "AZ__ACCOUNT_KEY"


def test_write_errors_when_env_backed_setting_is_unset(tmp_path, monkeypatch):
    az = systems.get("azureblob")
    settings = {
        "CONTAINER": Setting("CONTAINER", value="c"),
        "ACCOUNT_NAME": Setting("ACCOUNT_NAME", value="acct"),
        "ACCOUNT_KEY": Setting("ACCOUNT_KEY", env_var="AZ__ACCOUNT_KEY"),
        "SYS_PATH": Setting("SYS_PATH", value="root", is_path=True),
    }
    monkeypatch.delenv("AZ__ACCOUNT_KEY", raising=False)
    with pytest.raises(DatagenError, match="AZ__ACCOUNT_KEY.*unset"):
        descriptor.write(az, "sys", str(tmp_path / "az.json"), settings)


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


def test_info_prefix_prefixes_every_key(tmp_path):
    doc = _stamped_domain(tmp_path)
    plain = json.loads(descriptor.info(doc, "json"))
    prefixed = json.loads(descriptor.info(doc, "json", prefix="SRC"))
    assert set(prefixed) == {f"SRC__{k}" for k in plain}
    assert prefixed["SRC__NAME"] == plain["NAME"]

    exp = descriptor.info(doc, "export", prefix="SRC")
    assert "export SRC__NAME=inbox" in exp
    unset = {ln.split()[1] for ln in
             descriptor.info(doc, "unset", prefix="SRC").splitlines()}
    assert unset == set(prefixed)


def test_info_prefix_none_or_empty_is_unprefixed(tmp_path):
    doc = _stamped_domain(tmp_path)
    plain = descriptor.info(doc, "json")
    assert descriptor.info(doc, "json", prefix=None) == plain
    assert descriptor.info(doc, "json", prefix="") == plain


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
