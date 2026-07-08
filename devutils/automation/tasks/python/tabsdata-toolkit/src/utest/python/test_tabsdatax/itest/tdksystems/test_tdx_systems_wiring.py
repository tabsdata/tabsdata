"""tdx.py routes s3 / mysql / snowflake through the unified systems/ registry
(not the container/cloud/saas category branches). Exercised end-to-end through
the real tdx verb functions with the docker / boto3 / connector layers faked."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import data_gen, descriptor, store, systems, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.shell import ArgumentParser
from tabsdatax.itest.tdksystems.systems.base import System
from tabsdatax.itest.tdksystems.systems.mysql.system import MysqlSystem
from tabsdatax.itest.tdksystems.systems.s3.system import S3System
from tabsdatax.itest.tdksystems.systems.snowflake.system import SnowflakeSystem


_S3_CREATE = ["--bucket", "b", "--region", "r", "--access-key", "AK",
              "--secret-key", "SK", "--sys-path", "data", "-f"]


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
    for opt in sysobj.options:
        st = settings[opt.name]
        assert st.value is None and st.env_var == f"{slug.upper()}__{opt.name}"


# ---- s3 (cloud) -----------------------------------------------------------

def test_s3_lifecycle_through_tdx(stores, fake_s3):
    assert tdx._sys_create(["--type", "s3", "--sys", "store",
                            *_S3_CREATE]) == 0
    doc = descriptor.load(store.system_path("store"))
    assert (doc["category"], doc["system"], doc["role"]) == ("cloud", "s3", "sys")
    assert tdx._sys_status(["--sys", "store"]) == 0
    assert tdx._domain_create(["--sys", "store", "--domain", "inbox", "-f"]) == 0
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
    assert tdx._domain_create(["--sys", "mydb", "--domain", "finance", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("finance"))
    assert m["KIND"] == "container-domain" and m["DB"] == "finance"
    assert m["SYS_CONTAINER"] == "mydb"        # back-ref for destroy
    assert m["SQLALCHEMY_URI"].startswith("mysql+mysqlconnector://finance:")
    assert tdx._domain_destroy(["--domain", "finance"]) == 0
    assert not store.domain_path("finance").exists()


# ---- snowflake (saas) -----------------------------------------------------

def test_snowflake_lifecycle_through_tdx(stores, fake_sf):
    tdx._sys_create(["--type", "snowflake", "--sys", "snow",
                     "--account", "ac", "--username", "u", "--pat", "tok",
                     "--warehouse", "wh", "--database", "ANALYTICS", "-f"])
    assert descriptor.load(store.system_path("snow"))["category"] == "saas"
    assert tdx._sys_status(["--sys", "snow"]) == 0
    assert tdx._domain_create(["--sys", "snow", "--domain", "finance", "-f"]) == 0
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
