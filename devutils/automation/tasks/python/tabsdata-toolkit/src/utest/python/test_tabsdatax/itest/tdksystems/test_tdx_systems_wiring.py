"""tdx.py routes s3 / mysql / snowflake through the unified systems/ registry
(not the container/cloud/saas category branches). Exercised end-to-end through
the real tdx verb functions with the docker / boto3 / connector layers faked."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import data_gen, descriptor, store, systems, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.shell import ArgumentParser
from tabsdatax.itest.tdksystems.systems.base import System
from tabsdatax.itest.tdksystems.systems.gcs.system import GcsSystem
from tabsdatax.itest.tdksystems.systems.mysql.system import MysqlSystem
from tabsdatax.itest.tdksystems.systems.s3.system import S3System
from tabsdatax.itest.tdksystems.systems.snowflake.system import SnowflakeSystem

_S3_CREATE = ["--bucket", "b", "--region", "r", "--access-key", "AK",
              "--secret-key", "SK", "--sys-path", "data", "-f"]

_GCS_CREATE = ["--bucket", "b", "--service-account-key", "SAK",
               "--hmac-access-key", "HAK", "--hmac-secret-key", "HSK",
               "--sys-path", "data", "-f"]


class _FakeS3:
    def status(self): pass
    def exists(self, p): return False
    def clear(self, p): pass
    def upload(self, p, paths): return 0


class _FakeCur:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, s): pass
    def fetchone(self): return (1,)


@pytest.fixture
def fake_s3(monkeypatch):
    monkeypatch.setattr(S3System, "_backend", lambda self, values: _FakeS3())


@pytest.fixture
def fake_gcs(monkeypatch):
    monkeypatch.setattr(GcsSystem, "_backend", lambda self, values: _FakeS3())


@pytest.fixture
def fake_sf(monkeypatch):
    monkeypatch.setattr(SnowflakeSystem, "_cursor", lambda self, values: _FakeCur())


def test_registry_serves_every_system():
    # All 17 systems are now on the unified registry.
    assert set(tdx.systems.all_slugs()) == {
        "mysql", "postgres", "mssqlserver", "oracle", "db2", "clickhouse",
        "mongodb", "starrocks", "redpanda",
        "s3", "gcs", "azureblob", "fs",
        "databricks", "snowflake", "salesforce", "bigquery",
    }
    assert tdx._unified("s3") and tdx._unified("postgres")


@pytest.mark.parametrize("slug", systems.all_slugs())
def test_every_system_loads_and_declares_create_options(slug):
    """Structural check for all 16 ports: loads, is a System, its create
    options parse, and each declared option resolves to a default-prefix ref."""
    sysobj = systems.get(slug)
    assert isinstance(sysobj, System)
    p = ArgumentParser(prog=f"tdx-system create --type {slug}")
    sysobj.add_create_options(p)                 # must not raise
    ns = p.parse_args([])                         # create options are optional
    settings = sysobj.resolve_create_settings(ns)  # no env lookup at this stage
    expected = {o.name for o in sysobj.options}
    if sysobj.staging_root:
        expected.add("SYS_PATH")
    assert set(settings) == expected
    # Each declared credential option defaults to a $<PREFIX>__<OPT> ref, where
    # PREFIX is the system's default env prefix (azureblob overrides it to AZ).
    for opt in sysobj.options:
        st = settings[opt.name]
        assert st.value is None
        assert st.env_var == f"{sysobj.default_env_prefix}__{opt.name}"


# ---- s3 (cloud) -----------------------------------------------------------

def test_s3_lifecycle_through_tdx(stores, fake_s3):
    assert tdx._sys_create(["--type", "s3", "--sys", "store",
                            *_S3_CREATE]) == 0
    doc = descriptor.load(store.system_path("store"))
    assert (doc["category"], doc["system"], doc["role"]) == ("cloud", "s3", "sys")
    assert tdx._sys_status(["--sys", "store"]) == 0
    # Pin the prefix off so BASE_PATH is exactly data/<domain> (the default
    # prefix, covered separately, would be host-user dependent).
    assert tdx._domain_create(["--sys", "store", "--domain", "inbox",
                               "--domain-prefix", "", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("inbox"))
    assert m["KIND"] == "cloud-domain"
    assert m["STORAGE_TYPE"] == "s3"      # cli.py's cloud loader still finds it
    assert m["BASE_PATH"] == "data/inbox"
    assert tdx._domain_destroy(["--domain", "inbox"]) == 0
    assert not store.domain_path("inbox").exists()
    assert tdx._sys_destroy(["--sys", "store"]) == 0
    assert not store.system_path("store").exists()


def test_s3_descriptor_matches_old_writer(tmp_path, stores, fake_s3):
    """descriptor.write reproduces the pre-migration descriptor exactly."""
    ref = str(tmp_path / "ref.json")
    tdx._sys_create(["--type", "s3", "--sys", "store", *_S3_CREATE])
    descriptor.write_cloud_system(
        out=ref, storage_type="s3", system_kind="file", label="S3",
        sys_path="data", sys_path_env=None,
        forwarded=[("BUCKET", "b", None), ("REGION", "r", None),
                   ("ACCESS_KEY", "AK", None), ("SECRET_KEY", "SK", None)],
        strip_sys_path_slash=True)
    a, b = descriptor.load(store.system_path("store")), descriptor.load(ref)
    for key in ("kind", "category", "system", "role", "meta", "exported", "strip_slash"):
        assert a[key] == b[key], key


def test_cloud_create_does_not_probe_or_clear_root(stores, monkeypatch):
    # Object-store create verifies reachability only: it must not check the root
    # prefix for existing objects nor clear it (the root is externally owned).
    class _StrictS3:
        def status(self): pass
        def exists(self, p): raise AssertionError("must not probe root existence")
        def clear(self, p): raise AssertionError("must not clear root prefix")
        def upload(self, p, paths): return 0

    monkeypatch.setattr(S3System, "_backend", lambda self, values: _StrictS3())
    assert tdx._sys_create(["--type", "s3", "--sys", "store", *_S3_CREATE]) == 0
    assert tdx._sys_destroy(["--sys", "store"]) == 0


# ---- gcs (cloud) — S3-interop HMAC credentials -----------------------------

def test_gcs_exports_hmac_keys(stores, fake_gcs):
    # The HMAC access/secret keys are settable directly (or via $GCS__*) and are
    # surfaced in the descriptor's exported set and `info`.
    assert tdx._sys_create(["--type", "gcs", "--sys", "store", *_GCS_CREATE]) == 0
    doc = descriptor.load(store.system_path("store"))
    assert "HMAC_ACCESS_KEY" in doc["exported"]
    assert "HMAC_SECRET_KEY" in doc["exported"]
    info = json.loads(descriptor.info(doc, "json"))
    assert info["HMAC_ACCESS_KEY"] == "HAK"
    assert info["HMAC_SECRET_KEY"] == "HSK"


# ---- mysql (container) — domain via tdx; system descriptor built directly -

def test_mysql_domain_through_tdx(stores, monkeypatch):
    # Admin runs over TCP now; stub the SQL helpers so no real DB is needed.
    monkeypatch.setattr(MysqlSystem, "_run_admin_sql",
                        lambda self, admin, statements: None)
    monkeypatch.setattr(MysqlSystem, "_query_admin_names",
                        lambda self, admin, query: [])
    sysf = store.system_path("mydb")
    descriptor.write_container_system(
        out=str(sysf), system_type="mysql", system_kind="sql", label="MySQL",
        container="mydb", host="localhost", port="3306", user="root",
        password="rootpw", domain_desc="db")
    descriptor.stamp(str(sysf), name="mydb", started="t")
    assert tdx._domain_create(["--sys", "mydb", "--domain", "finance",
                               "--domain-prefix", "", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("finance"))
    assert m["KIND"] == "container-domain" and m["DB"] == "finance"
    assert m["SYS_CONTAINER"] == "mydb"        # back-ref for destroy
    assert m["SQLALCHEMY_URI"].startswith("mysql+mysqlconnector://finance:")
    assert tdx._domain_destroy(["--domain", "finance"]) == 0
    assert not store.domain_path("finance").exists()


def test_container_domain_entity_is_prefixed(stores, monkeypatch):
    """The entity container (DB / user) created in the system carries the
    DOMAIN_PREFIX; the descriptor's logical name stays the bare domain name and
    the prefixed name is recorded as EXTERNAL_NAME."""
    monkeypatch.setattr(MysqlSystem, "_run_admin_sql",
                        lambda self, admin, statements: None)
    monkeypatch.setattr(MysqlSystem, "_query_admin_names",
                        lambda self, admin, query: [])
    sysf = store.system_path("mydb")
    descriptor.write_container_system(
        out=str(sysf), system_type="mysql", system_kind="sql", label="MySQL",
        container="mydb", host="localhost", port="3306", user="root",
        password="rootpw", domain_desc="db")
    descriptor.stamp(str(sysf), name="mydb", started="t")
    assert tdx._domain_create(["--sys", "mydb", "--domain", "finance",
                               "--domain-prefix", "ci_1_", "-f"]) == 0
    doc = descriptor.load(store.domain_path("finance"))
    assert doc["name"] == "finance"                    # logical name unchanged
    assert doc["external_name"] == "ci_1_finance"
    m = descriptor.read_meta(store.domain_path("finance"))
    assert m["DB"] == "ci_1_finance"                   # the actual database
    assert m["SQLALCHEMY_URI"].startswith("mysql+mysqlconnector://ci_1_finance:")


# ---- snowflake (saas) -----------------------------------------------------

def test_snowflake_lifecycle_through_tdx(stores, fake_sf):
    tdx._sys_create(["--type", "snowflake", "--sys", "snow",
                     "--account", "ac", "--username", "u", "--pat", "tok",
                     "--warehouse", "wh", "--database", "ANALYTICS", "-f"])
    assert descriptor.load(store.system_path("snow"))["category"] == "saas"
    assert tdx._sys_status(["--sys", "snow"]) == 0
    assert tdx._domain_create(["--sys", "snow", "--domain", "finance",
                               "--domain-prefix", "", "-f"]) == 0
    assert descriptor.read_meta(store.domain_path("finance"))["SCHEMA"] == "finance"
    # pause is container-only — capability flag, surfaced as a clear error.
    with pytest.raises(DatagenError, match="only applies to container"):
        tdx._sys_pause(["--sys", "snow"])
    assert tdx._domain_destroy(["--domain", "finance"]) == 0
    assert tdx._sys_destroy(["--sys", "snow"]) == 0


# ---- create --help advertises the available choices -----------------------

def test_domain_create_help_lists_models(capsys):
    """`tdx-domain create --help` lists the available models, mirroring how
    `tdx-system create --help` lists the systems."""
    with pytest.raises(SystemExit):
        tdx._domain_create(["--help"])
    out = capsys.readouterr().out
    models = data_gen.list_models()
    assert models, "expected at least one model"
    for model in models:
        assert model in out, f"{model!r} missing from tdx-domain create --help"
