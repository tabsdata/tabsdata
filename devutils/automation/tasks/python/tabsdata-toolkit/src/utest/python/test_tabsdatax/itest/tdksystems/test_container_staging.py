"""File-staging container systems (clickhouse / starrocks) carry a staging-root
SYS_PATH on the system and a full BASE_PATH (= SYS_PATH/<domain>) on each domain
— mirroring the cloud/fs systems."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import cli, descriptor, systems
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.systems._container import ContainerBase, DomainResult
from tabsdatax.itest.tdksystems.systems.base import (
    CreateDomainSpec, CreateSystemSpec, SystemContext,
)


def _meta(path) -> dict[str, str]:
    return descriptor.read_meta(path)


def _info_json(path, name):
    import json
    descriptor.stamp(path, name=name, started="t")
    return json.loads(descriptor.info(descriptor.load(path), "json"))


# ---- descriptor: system bakes + exports SYS_PATH -------------------------

def test_system_descriptor_bakes_and_exports_sys_path_literal(tmp_path):
    out = tmp_path / "clickhouse.json"
    descriptor.write_container_system(
        out=str(out), system_type="clickhouse", system_kind="sql",
        label="ClickHouse", container="clickhouse", host="localhost",
        port="8123", user="default", password="pw", domain_desc="db",
        sys_path="/xp-staging", sys_path_env=None,
    )
    assert _meta(out)["SYS_PATH"] == "xp-staging"          # leading slash stripped
    assert _info_json(out, "clickhouse")["SYS_PATH"] == "xp-staging"


def test_system_descriptor_sys_path_env_reference(tmp_path, monkeypatch):
    out = tmp_path / "clickhouse.json"
    descriptor.write_container_system(
        out=str(out), system_type="clickhouse", system_kind="sql",
        label="ClickHouse", container="clickhouse", host="localhost",
        port="8123", user="default", password="pw", domain_desc="db",
        sys_path=None, sys_path_env="STAGE_ROOT",
    )
    monkeypatch.setenv("STAGE_ROOT", "stage")
    meta = _meta(out)
    assert meta["SYS_PATH"] == "stage"
    assert meta["FORWARDED_SYS_PATH_ENV"] == "STAGE_ROOT"


def test_system_descriptor_without_staging_has_no_sys_path(tmp_path):
    out = tmp_path / "mysql.json"
    descriptor.write_container_system(
        out=str(out), system_type="mysql", system_kind="sql", label="MySQL",
        container="mysql", host="localhost", port="3306", user="root",
        password="pw", domain_desc="db",
    )
    assert "SYS_PATH" not in _meta(out)


# ---- descriptor: domain bakes + exports the full BASE_PATH ---------------

def test_domain_descriptor_bakes_full_base_path(tmp_path):
    out = tmp_path / "clickhouse_OUT.json"
    descriptor.write_container_domain(
        out=str(out), system_type="clickhouse", system_kind="sql",
        host="localhost", port="8123", user="clickhouse_OUT", password="pw",
        db="clickhouse_OUT", sqlalchemy_uri="clickhouse+http://x",
        domain_desc="db", supports_sql=True,
        sys_path="/xp-staging", sys_path_env=None,
        base_path="/xp-staging/clickhouse_OUT",
    )
    assert _meta(out)["BASE_PATH"] == "xp-staging/clickhouse_OUT"
    info = _info_json(out, "clickhouse_out")
    assert info["SYS_PATH"] == "xp-staging"
    assert info["BASE_PATH"] == "xp-staging/clickhouse_OUT"


def test_domain_descriptor_without_staging_has_no_base_path(tmp_path):
    out = tmp_path / "mysql_OUT.json"
    descriptor.write_container_domain(
        out=str(out), system_type="mysql", system_kind="sql",
        host="localhost", port="3306", user="u", password="p",
        db="mysql_OUT", sqlalchemy_uri="mysql://x", domain_desc="db",
        supports_sql=True,
    )
    assert "BASE_PATH" not in _meta(out)


# ---- create_system: staging root required iff the system stages files ----

def test_create_system_requires_staging_root():
    # clickhouse is a file-staging system; no SYS_PATH -> fails before docker.
    sysobj = systems.get("clickhouse")
    with pytest.raises(DatagenError, match="staging root is required"):
        sysobj.create_system(CreateSystemSpec(name="c", out="/tmp/x", settings={}))


# ---- create_domain: derives BASE_PATH = SYS_PATH/<domain> ----------------

def test_create_domain_derives_base_path(monkeypatch):
    sysobj = systems.get("clickhouse")
    monkeypatch.setattr(sysobj, "do_list", lambda admin: [])
    monkeypatch.setattr(
        sysobj, "do_create",
        lambda admin, name: DomainResult(
            db=name, user=name, password="pw",
            sqlalchemy_uri=f"clickhouse+http://{name}"))
    ctx = SystemContext("clickhouse", "clickhouse", {
        "CONTAINER": "clickhouse", "PASSWORD": "pw", "PORT": "8123",
        "SYS_PATH": "xp-staging"})
    baked = sysobj.create_domain(ctx, CreateDomainSpec(name="clickhouse_OUT",
                                                       out="/tmp/x"))
    assert baked["BASE_PATH"].value == "xp-staging/clickhouse_OUT"
    assert baked["SYS_PATH"].value == "xp-staging"


# ---- _resolve_staging_root: namespace resolution -------------------------

class _NS:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def test_resolve_staging_root_literal():
    sysobj = systems.get("clickhouse")
    ns = _NS(lit_SYS_PATH="/xp-staging/", env_SYS_PATH=None, env_prefix=None)
    assert cli._resolve_staging_root(sysobj, ns) == ("/xp-staging", None)


def test_resolve_staging_root_env_prefix(monkeypatch):
    sysobj = systems.get("clickhouse")
    monkeypatch.setenv("STAGING__SYS_PATH", "/x")
    ns = _NS(lit_SYS_PATH=None, env_SYS_PATH=None, env_prefix="STAGING")
    assert cli._resolve_staging_root(sysobj, ns) == (None, "STAGING__SYS_PATH")


def test_resolve_staging_root_rejects_both():
    sysobj = systems.get("clickhouse")
    ns = _NS(lit_SYS_PATH="/x", env_SYS_PATH="V", env_prefix=None)
    with pytest.raises(DatagenError, match="not both"):
        cli._resolve_staging_root(sysobj, ns)


def test_resolve_staging_root_missing_env(monkeypatch):
    sysobj = systems.get("clickhouse")
    monkeypatch.delenv("MISSING_STAGE", raising=False)
    ns = _NS(lit_SYS_PATH=None, env_SYS_PATH="MISSING_STAGE", env_prefix=None)
    with pytest.raises(DatagenError, match="unset / empty"):
        cli._resolve_staging_root(sysobj, ns)


def test_resolve_staging_root_noop_for_non_staging():
    sysobj = systems.get("mysql")
    assert cli._resolve_staging_root(sysobj, _NS()) == (None, None)


# ---- flags are set on the right systems ----------------------------------

def test_staging_flag_set_on_clickhouse_and_starrocks():
    assert systems.get("clickhouse").uses_file_staging is True
    assert systems.get("starrocks").uses_file_staging is True
    assert systems.get("mysql").uses_file_staging is False
    assert ContainerBase.uses_file_staging is False
