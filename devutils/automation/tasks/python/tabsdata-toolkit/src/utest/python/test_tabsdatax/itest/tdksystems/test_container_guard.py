#
# Copyright 2026 Tabsdata Inc.
#

"""CONTAINER_ID staleness guard: a container descriptor records the container's
real docker id (CONTAINER_ID; SYS_CONTAINER_ID on its domains). Reads flag and
mutations refuse when the live container no longer matches (removed / replaced)."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import descriptor, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError


def _patch_container_id(monkeypatch, value):
    """Patch the container_id lookup the stale check uses. `value` is the live id
    (str), None (container gone), or an Exception to raise (docker unavailable)."""

    def fake(_name):
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(tdx, "container_id", fake)


SYS_META = {
    "SYSTEM_LOCATION": "container",
    "CONTAINER": "tdx_db",
    "CONTAINER_ID": "abc123def456",
}


def test_fresh_when_live_id_matches(monkeypatch):
    _patch_container_id(monkeypatch, "abc123def456")
    assert tdx._container_stale(SYS_META) is None


def test_stale_when_replaced(monkeypatch):
    _patch_container_id(monkeypatch, "999999999999")
    reason = tdx._container_stale(SYS_META)
    assert reason and "replaced" in reason and "tdx_db" in reason


def test_stale_when_gone(monkeypatch):
    _patch_container_id(monkeypatch, None)
    reason = tdx._container_stale(SYS_META)
    assert reason and "gone" in reason


def test_not_flagged_when_docker_unavailable(monkeypatch):
    _patch_container_id(monkeypatch, DatagenError("docker is not available"))
    assert tdx._container_stale(SYS_META) is None


def test_ssh_tunnel_never_stale(monkeypatch):
    _patch_container_id(monkeypatch, "999")  # would mismatch, but tunnels are exempt
    assert tdx._container_stale(dict(SYS_META, SYSTEM_LOCATION="ssh-tunnel")) is None


def test_no_recorded_id_is_never_stale(monkeypatch):
    _patch_container_id(monkeypatch, None)
    assert tdx._container_stale({"CONTAINER": "tdx_db"}) is None  # pre-CONTAINER_ID
    assert tdx._container_stale({"BUCKET": "b"}) is None  # non-container


def test_domain_pair_checks_sys_container(monkeypatch):
    _patch_container_id(monkeypatch, "999999999999")
    meta = {"SYS_CONTAINER": "tdx_db", "SYS_CONTAINER_ID": "abc123def456"}
    reason = tdx._container_stale(meta)
    assert reason and "replaced" in reason and "domain" in reason


def test_guard_raises_when_stale(monkeypatch):
    _patch_container_id(monkeypatch, None)
    with pytest.raises(DatagenError, match="refusing to run"):
        tdx._guard_container_fresh(SYS_META)


def test_guard_ok_when_fresh(monkeypatch):
    _patch_container_id(monkeypatch, "abc123def456")
    tdx._guard_container_fresh(SYS_META)  # no raise


def test_descriptor_bakes_container_id(tmp_path):
    out = str(tmp_path / "db")
    descriptor.write_container_system(
        out=out, system_type="postgres", system_kind="sql", label="PostgreSQL",
        container="tdx_db", host="localhost", port="5432", user="postgres",
        password="pw", domain_desc="", image="postgres:17",
        container_id="deadbeefcafe",
    )
    doc = descriptor.load(out)
    assert descriptor.resolve(doc)["CONTAINER_ID"] == "deadbeefcafe"
    assert "CONTAINER_ID" in doc["exported"]


def test_record_meta_adds_resolvable_key(tmp_path):
    out = str(tmp_path / "db")
    descriptor.write_container_system(
        out=out, system_type="postgres", system_kind="sql", label="PostgreSQL",
        container="tdx_db", host="localhost", port="5432", user="postgres",
        password="pw", domain_desc="",
    )
    assert "CONTAINER_ID" not in descriptor.resolve(descriptor.load(out))
    descriptor.record_meta(out, {"CONTAINER_ID": "feed0000"})
    doc = descriptor.load(out)
    assert descriptor.resolve(doc)["CONTAINER_ID"] == "feed0000"
    assert "CONTAINER_ID" in doc["exported"]
    # idempotent: replacing the value doesn't duplicate the meta / exported entry
    descriptor.record_meta(out, {"CONTAINER_ID": "newvalue"})
    doc2 = descriptor.load(out)
    assert descriptor.resolve(doc2)["CONTAINER_ID"] == "newvalue"
    assert doc2["exported"].count("CONTAINER_ID") == 1
    assert sum(1 for e in doc2["meta"] if e["name"] == "CONTAINER_ID") == 1


def test_create_records_id_then_guard_detects_replacement(stores, monkeypatch):
    """End-to-end (docker mocked): create bakes the live container id, and once
    the container is replaced a read flags stale and destroy refuses without
    -f."""
    from tabsdatax.itest.tdksystems import store
    from tabsdatax.itest.tdksystems.systems import _container
    from tabsdatax.itest.tdksystems.systems.base import Setting

    def fake_plan(_self, spec):
        return {
            "CONTAINER": Setting("CONTAINER", value=f"tdx_{spec.name}"),
            "IMAGE": Setting("IMAGE", value="postgres:17"),
            "HOST": Setting("HOST", value="localhost"),
            "PORT": Setting("PORT", value="5432"),
            "USER": Setting("USER", value="postgres"),
            "PASSWORD": Setting("PASSWORD", value="pw"),
        }

    monkeypatch.setattr(_container.ContainerBase, "plan_system", fake_plan)
    monkeypatch.setattr(
        _container.ContainerBase, "provision_system", lambda *a, **k: None
    )
    # container_id used by provisioned_meta (in _container) at create time.
    monkeypatch.setattr(_container, "container_id", lambda _name: "cid-original")

    assert tdx._sys_create(["--type", "postgres", "--sys", "pg", "-f"]) == 0
    meta = descriptor.resolve(descriptor.load(store.system_path("pg")))
    assert meta["CONTAINER_ID"] == "cid-original"

    # The container has since been replaced (different id): reads flag, mutations
    # refuse. (container_id used by the guard lives in the tdx module.)
    monkeypatch.setattr(tdx, "container_id", lambda _name: "cid-REPLACED")
    ok, detail = tdx._sys_status_one("pg", None)
    assert ok is False and "stale" in detail
    with pytest.raises(DatagenError, match="refusing to destroy"):
        tdx._sys_destroy_one("pg", None, force=False)
    detail = tdx._sys_destroy_one("pg", None, force=True)
    assert "without touching docker" in detail
    assert not store.system_path("pg").exists()
