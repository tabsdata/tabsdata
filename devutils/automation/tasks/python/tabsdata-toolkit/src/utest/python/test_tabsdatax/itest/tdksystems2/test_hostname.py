#
# Copyright 2026 Tabsdata Inc.
#

"""``create --sys-hostname``: the host a system advertises to everyone else.

A container is published on a host port, and by default the descriptor calls that
host ``localhost`` — so the domains built on it (and the connections built on
those) only work on the machine that ran docker. These check that the flag
reaches every endpoint the descriptor records, including the address Redpanda
hands back to a client that bootstraps against it.
"""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import descriptor, manifest, store, systems, tdx
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems._common.commands import sys_create
from tabsdatax.itest.tdksystems2.systems.druid import backend as druid
from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql
from tabsdatax.itest.tdksystems2.systems.redpanda import backend as redpanda

HOST = "box.example.com"


@pytest.fixture
def faked(monkeypatch):
    """Stub the docker / SQL touching methods on the MySQL backend."""
    monkeypatch.setattr(Mysql, "provision_system", lambda self, meta, force=False: {})
    monkeypatch.setattr(Mysql, "list_domain_entities", lambda self, meta: [])
    monkeypatch.setattr(
        Mysql, "provision_domain",
        lambda self, sys_meta, entity, dmeta, force=False: None,
    )
    monkeypatch.setattr(sys_create, "port_open", lambda host, port: True)


def _sys(argv):
    return tdx._dispatch("tdx-system", argv)


def _dom(argv):
    return tdx._dispatch("tdx-domain", argv)


def test_hostname_reaches_the_domain_uris(stores, faked):
    assert _sys(["create", "--type", "mysql", "--sys", "s", "--sys-hostname", HOST]) == 0
    sdoc = descriptor.load(store.system_path("s"))
    assert sdoc.meta["HOST"] == HOST
    assert sdoc.sys_hostname == HOST  # recorded, so a manifest replay repeats it

    assert _dom(["create", "--sys", "s", "--domain", "d"]) == 0
    meta = descriptor.load(store.domain_path("d")).meta
    assert meta["HOST"] == HOST
    assert f"@{HOST}:{sdoc.meta['PORT']}" in meta["SQLALCHEMY_URI"]
    assert f"{HOST}:{sdoc.meta['PORT']}" in meta["URI"]


def test_default_stays_localhost(stores, faked):
    assert _sys(["create", "--type", "mysql", "--sys", "s"]) == 0
    sdoc = descriptor.load(store.system_path("s"))
    assert sdoc.meta["HOST"] == "localhost" and sdoc.sys_hostname == ""


def test_a_system_with_no_host_says_so_and_still_creates(stores, capsys):
    # `create --type ALL --sys-hostname H` hands the flag to every type, so one
    # that advertises nothing must not fail the run.
    assert _sys(["create", "--type", "dev", "--sys", "d", "--sys-hostname", HOST]) == 0
    assert "dev has no host of its own" in capsys.readouterr().out
    assert descriptor.load(store.system_path("d")).sys_hostname == ""


def test_a_remote_system_is_told_its_host_is_its_own(capsys):
    # rdspostgres / snowflake are not containers but do have a host — one they
    # read from their own configuration, not one tdx can name.
    assert sys_create._bind_hostname(systems.get("rdspostgres"), HOST) == ""
    assert "takes its host from its own configuration" in capsys.readouterr().out


@pytest.mark.parametrize(
    "bad", ["box.example.com:8081", "http://box.example.com", "fd00::1", "", "  ", "a b"]
)
def test_an_unusable_hostname_is_rejected_before_anything_is_created(bad, stores):
    with pytest.raises(TdxError):
        _sys(["create", "--type", "mysql", "--sys", "s", "--sys-hostname", bad])
    assert not descriptor.is_descriptor(store.system_path("s"))


def test_an_unreachable_endpoint_is_reported_at_create(stores, faked, capsys, monkeypatch):
    monkeypatch.setattr(sys_create, "port_open", lambda host, port: False)
    assert _sys(["create", "--type", "mysql", "--sys", "s", "--sys-hostname", HOST]) == 0
    assert "does not answer from this machine" in capsys.readouterr().out


def test_a_reachable_endpoint_is_not_reported(stores, faked, capsys):
    assert _sys(["create", "--type", "mysql", "--sys", "s", "--sys-hostname", HOST]) == 0
    assert "does not answer" not in capsys.readouterr().out


def test_redpanda_advertises_the_hostname_to_its_clients(monkeypatch):
    started: list[list[str]] = []
    monkeypatch.setattr(redpanda, "require_docker", lambda: None)
    monkeypatch.setattr(redpanda, "container_exists", lambda name: False)
    monkeypatch.setattr(redpanda, "docker_pull", lambda image, platform: None)
    monkeypatch.setattr(redpanda, "docker_run", lambda args: started.append(args))
    monkeypatch.setattr(redpanda, "container_id", lambda name: "cid")
    monkeypatch.setattr(
        redpanda.Redpanda, "_wait_ready", lambda self, c, p, t: None
    )

    backend = systems.get("redpanda")
    backend.use_hostname(HOST)
    meta = backend.plan_system("rp")
    assert meta["KAFKA_BROKERS"] == f"{HOST}:{meta['PORT']}"
    assert meta["SCHEMA_REGISTRY_URL"] == f"http://{HOST}:{meta['REGISTRY_PORT']}"

    backend.provision_system(meta)
    argv = started[0]
    advertised = argv[argv.index("--advertise-kafka-addr") + 1]
    # a bootstrapping client is redirected to the external address, so that one
    # carries the hostname; the in-container listener stays loopback
    assert advertised == f"internal://localhost:9092,external://{HOST}:{meta['PORT']}"


def test_druid_urls_follow_the_hostname():
    backend = systems.get("druid")
    backend.use_hostname(HOST)
    meta = backend.plan_system("dr")
    assert meta["OVERLORD_URL"].startswith(f"http://{HOST}:")
    assert meta["ROUTER_URL"].startswith(f"http://{HOST}:")


def test_druid_probes_the_stack_over_loopback(monkeypatch):
    # The stack is brought up and reported on from the docker host, so a
    # hostname that only resolves elsewhere must not fail readiness — which
    # tears the healthy stack back down.
    polled: list[str] = []
    monkeypatch.setattr(
        druid, "http_ok", lambda url, **kw: polled.append(url) or True
    )
    backend = systems.get("druid")
    backend.use_hostname(HOST)
    meta = backend.plan_system("dr")

    backend.wait_ready(meta, 1)
    assert backend.system_health(meta)[0]
    assert polled and all(HOST not in url for url in polled)
    assert all(url.startswith("http://localhost:") for url in polled)
    ports = {druid.Druid._port(url) for url in polled}
    assert ports == {druid.Druid._port(meta["OVERLORD_URL"]), druid.Druid._port(meta["ROUTER_URL"])}


def test_manifest_replays_the_hostname(tmp_path):
    path = tmp_path / "s"
    descriptor.write_system(
        path, "mysql", "s", {"HOST": HOST, "PORT": "3306"}, sys_hostname=HOST
    )
    entry = manifest.system_entry(descriptor.load(path))
    assert entry["sys-hostname"] == HOST
    argv = tdx._entry_steps("tdx-system", entry, force=False, ports=None)[0]
    assert argv[argv.index("--sys-hostname") + 1] == HOST
