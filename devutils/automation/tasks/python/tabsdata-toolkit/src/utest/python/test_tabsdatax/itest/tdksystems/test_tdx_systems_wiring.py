#
# Copyright 2026 Tabsdata Inc.
#

"""tdx.py routes s3 / mysql / snowflake through the unified systems/ registry
(not the container/cloud/saas category branches). Exercised end-to-end through
the real tdx verb functions with the docker / boto3 / connector layers faked."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import data_gen, descriptor, store, systems, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.shell import ArgumentParser
from tabsdatax.itest.tdksystems.systems.base import Health, System
from tabsdatax.itest.tdksystems.systems.druid.system import DruidSystem
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
    # Every system is on the unified registry (incl. the CDC variants).
    assert set(tdx.systems.all_slugs()) == {
        "mysql", "mysqlcdc", "postgres", "postgrescdc",
        "mssqlserver", "oracle", "db2", "db2cdc", "clickhouse",
        "mongodb", "starrocks", "redpanda",
        "druid",
        "s3", "gcs", "azureblob", "fs",
        "databricks", "snowflake", "salesforce", "bigquery",
        "anthropic", "openai",
        "dev",
    }
    assert tdx._unified("s3") and tdx._unified("postgres")


def test_cdc_systems_reuse_base_ddl_and_declare_capture():
    # A CDC system is a thin variant of its plain sibling: same data-plane kind,
    # but it reads the base system's model DDL (ddl_slug) so no duplicate .ddl is
    # needed, and it overrides configure_cdc.
    from tabsdatax.itest.tdksystems.systems.base import System

    for cdc, base in (("mysqlcdc", "mysql"), ("postgrescdc", "postgres"),
                      ("db2cdc", "db2")):
        sysobj = systems.get(cdc)
        assert sysobj.ddl_slug == base
        assert sysobj.kind == "sql" and sysobj.supports_sql
        # configure_cdc is specialised (not the base no-op) on every CDC system.
        assert type(sysobj).configure_cdc is not System.configure_cdc


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
    # Default naming: BASE_PATH is data/d_<user>_<build>_<domain> (`u_0` pinned).
    assert tdx._domain_create(["--sys", "store", "--domain", "inbox", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("inbox"))
    assert m["KIND"] == "cloud-domain"
    assert m["STORAGE_TYPE"] == "s3"      # cli.py's cloud loader still finds it
    assert m["BASE_PATH"] == "data/d_u_0_inbox"
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
                   ("ACCESS_KEY", "AK", None), ("SECRET_KEY", "SK", None)])
    a, b = descriptor.load(store.system_path("store")), descriptor.load(ref)
    for key in ("kind", "category", "system", "role", "meta", "exported"):
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
    assert tdx._domain_create(["--sys", "mydb", "--domain", "finance", "-f"]) == 0
    m = descriptor.read_meta(store.domain_path("finance"))
    assert m["KIND"] == "container-domain" and m["DB"] == "d_u_0_finance"
    assert m["SYS_CONTAINER"] == "mydb"        # back-ref for destroy
    assert m["SQLALCHEMY_URI"].startswith("mysql+mysqlconnector://d_u_0_finance:")
    assert tdx._domain_destroy(["--domain", "finance"]) == 0
    assert not store.domain_path("finance").exists()


def test_container_domain_entity_is_prefixed(stores, monkeypatch):
    """The entity container (DB / user) created in the system carries the default
    `d_<user>_<build>_` prefix; the descriptor's logical name stays the bare
    domain name and the prefixed name is recorded as EXTERNAL_NAME."""
    monkeypatch.setenv("TD_BUILD_USER", "ci")
    monkeypatch.setenv("TD_BUILD_NUMBER", "1")
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
    doc = descriptor.load(store.domain_path("finance"))
    assert doc["name"] == "finance"                    # logical name unchanged
    assert doc["external_name"] == "d_ci_1_finance"
    m = descriptor.read_meta(store.domain_path("finance"))
    assert m["DB"] == "d_ci_1_finance"                 # the actual database
    assert m["SQLALCHEMY_URI"].startswith("mysql+mysqlconnector://d_ci_1_finance:")


# ---- snowflake (saas) -----------------------------------------------------

def test_snowflake_lifecycle_through_tdx(stores, fake_sf):
    tdx._sys_create(["--type", "snowflake", "--sys", "snow",
                     "--account", "ac", "--username", "u", "--pat", "tok",
                     "--warehouse", "wh", "--database", "ANALYTICS", "-f"])
    assert descriptor.load(store.system_path("snow"))["category"] == "saas"
    assert tdx._sys_status(["--sys", "snow"]) == 0
    assert tdx._domain_create(["--sys", "snow", "--domain", "finance", "-f"]) == 0
    assert descriptor.read_meta(store.domain_path("finance"))["SCHEMA"] == "d_u_0_finance"
    # pause is container-only — capability flag, surfaced as a clear error.
    with pytest.raises(DatagenError, match="only applies to container"):
        tdx._sys_pause(["--sys", "snow"])
    assert tdx._domain_destroy(["--domain", "finance"]) == 0
    assert tdx._sys_destroy(["--sys", "snow"]) == 0


# ---- druid (containers / compose stack) -----------------------------------

@pytest.fixture
def fake_druid(monkeypatch):
    """Run the druid compose lifecycle offline: no docker, no HTTP. The stack's
    endpoints are pinned so the descriptor content is deterministic."""
    from tabsdatax.itest.tdksystems.systems import _containers
    monkeypatch.setattr(_containers, "require_docker", lambda: None)
    monkeypatch.setattr(_containers, "compose_container_ids", lambda p, f: [])
    monkeypatch.setattr(DruidSystem, "_plan_endpoints", lambda self: {
        "OVERLORD_URL": "http://localhost:18081",
        "ROUTER_URL": "http://localhost:18888",
    })
    monkeypatch.setattr(DruidSystem, "_compose_pull", lambda self, p, env=None: None)
    monkeypatch.setattr(DruidSystem, "_compose_up", lambda self, p, env=None: None)
    monkeypatch.setattr(DruidSystem, "_compose_down", lambda self, p: None)
    monkeypatch.setattr(DruidSystem, "_wait_ready", lambda self, settings, t: None)
    monkeypatch.setattr(DruidSystem, "_system_health",
                        lambda self, meta: Health(True, "ready"))


def test_druid_lifecycle_through_tdx(stores, fake_druid, monkeypatch):
    deleted: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        DruidSystem, "_delete_datasource",
        lambda self, url, ds, user="", password="": deleted.append((url, ds, user)))

    assert tdx._sys_create(["--type", "druid", "--sys", "d1", "-f"]) == 0
    doc = descriptor.load(store.system_path("d1"))
    assert doc["category"] == "containers"
    assert (doc["system"], doc["role"]) == ("druid", "sys")
    m = descriptor.read_meta(store.system_path("d1"))
    assert m["KIND"] == "containers-system"
    assert m["PROJECT"] == "tdx_d1"          # the compose project (destroy finds it)
    assert m["OVERLORD_URL"] == "http://localhost:18081"
    assert m["ROUTER_URL"] == "http://localhost:18888"
    assert m["IMAGE"].startswith("apache/druid:")
    # Basic-auth admin credentials are minted + exported; the internal-client
    # password is baked but NOT exported.
    assert m["USER"] == "admin" and m["PASSWORD"]
    assert "INTERNAL_PASSWORD" not in descriptor.load(store.system_path("d1"))["exported"]
    assert tdx._sys_status(["--sys", "d1"]) == 0

    # Domain == a datasource; default naming -> d_<user>_<build>_events (`u_0`).
    assert tdx._domain_create(["--sys", "d1", "--domain", "events", "-f"]) == 0
    dm = descriptor.read_meta(store.domain_path("events"))
    assert dm["KIND"] == "containers-domain"
    assert dm["DATASOURCE"] == "d_u_0_events"
    assert dm["OVERLORD_URL"] == "http://localhost:18081"
    assert dm["ROUTER_URL"] == "http://localhost:18888"
    assert dm["USER"] == "admin" and dm["PASSWORD"] == m["PASSWORD"]  # inherited

    assert tdx._domain_destroy(["--domain", "events"]) == 0
    # coordinator DELETE, authenticated as the admin user.
    assert deleted == [("http://localhost:18081", "d_u_0_events", "admin")]
    assert not store.domain_path("events").exists()
    assert tdx._sys_destroy(["--sys", "d1"]) == 0
    assert not store.system_path("d1").exists()


def test_druid_datasource_is_prefixed(stores, fake_druid, monkeypatch):
    """The datasource created carries the default `d_<user>_<build>_` prefix; the
    descriptor's logical name stays bare and the prefixed name is EXTERNAL_NAME."""
    monkeypatch.setenv("TD_BUILD_USER", "ci")
    monkeypatch.setenv("TD_BUILD_NUMBER", "1")
    tdx._sys_create(["--type", "druid", "--sys", "d1", "-f"])
    assert tdx._domain_create(["--sys", "d1", "--domain", "events", "-f"]) == 0
    doc = descriptor.load(store.domain_path("events"))
    assert doc["name"] == "events"                      # logical name unchanged
    assert doc["external_name"] == "d_ci_1_events"
    m = descriptor.read_meta(store.domain_path("events"))
    assert m["DATASOURCE"] == "d_ci_1_events"           # the actual datasource


def test_druid_rejects_data_but_accepts_model(stores, fake_druid):
    # Druid records a model (create-only) but loads no data: a datasource
    # materialises when the connector ingests, so tdx stages/loads nothing.
    from tabsdatax.itest.tdksystems import systems
    d = systems.get("druid")
    assert d.supports_models is True and d.supports_data is False
    tdx._sys_create(["--type", "druid", "--sys", "d1", "-f"])
    with pytest.raises(DatagenError, match="does not load data"):
        tdx._domain_create(["--sys", "d1", "--domain", "events",
                            "--model", "world", "--data", "xs", "-f"])


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
