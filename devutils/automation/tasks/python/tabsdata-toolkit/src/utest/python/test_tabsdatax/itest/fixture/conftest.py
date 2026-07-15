"""Provision backends for the fixture tests by running the REAL `tdx-system` /
`tdx-domain` create path (with the live provisioning I/O stubbed), so the
fixtures are exercised against exactly the descriptors tdx writes — not
hand-rolled ones.

The `provision` fixture points `$TD_BACKENDS_DIR` at a tmp store and returns a
helper whose `.system(slug, name)` / `.domain(slug, sys, dom)` drive
`tdx._sys_create` / `tdx._domain_create`. Only fs actually touches the disk
(mkdir under /tmp); every other backend's provisioning (docker, boto3, cloud
clients, SQL) is mocked out — the descriptor content is unaffected.

In a real run the ``system_<name>`` / ``domain_<name>`` fixtures generate at
plugin import from the pre-existing store; here no store exists at import, so
the names the injection tests declare are fed to the same fixture factories
directly (mocked input), and each test provisions its descriptors through
`provision` as usual."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tabsdatax.itest.fixture import backend
from tabsdatax.itest.tdksystems import crypto, store, systems, tdx
from tabsdatax.itest.tdksystems.systems import _container

for declared_system_name in ("my_fs", "alpha", "beta", "db", "store", "wh"):
    globals()[f"system_{declared_system_name}"] = backend.system_fixture(
        declared_system_name
    )
for declared_domain_name in ("inbox", "outbox", "archive", "shop", "sales"):
    globals()[f"domain_{declared_domain_name}"] = backend.domain_fixture(
        declared_domain_name
    )


class _FakeStore:
    def status(self): pass
    def exists(self, prefix): return False
    def clear(self, prefix): pass
    def upload(self, prefix, paths): return 0


class _FakeBq:
    def ping(self): pass
    def dataset_exists(self, name): return False
    def create_dataset(self, name, *, force=False): pass
    def delete_dataset(self, name): pass


def _dummy(opt):
    return "d1" if opt.is_path else "x"


def _opt_args(options):
    return [a for opt in options for a in (f"--{opt.cli_flag}", _dummy(opt))]


def _mock_provisioning(monkeypatch, sysobj):
    """Stub the live provisioning so `create` runs offline; what it *writes* is
    the real descriptor."""
    cls, cat = type(sysobj), sysobj.category
    if cat == "container":
        monkeypatch.setattr(_container, "require_docker", lambda: None)
        monkeypatch.setattr(_container, "container_exists", lambda name: False)
        monkeypatch.setattr(_container, "docker_pull", lambda image, platform="": None)
        monkeypatch.setattr(cls, "_docker_run", lambda self, c, port, pw: "")
        monkeypatch.setattr(cls, "_wait_ready", lambda self, c, pw, timeout: None)
        monkeypatch.setattr(cls, "_post_start", lambda self, c, pw: None)
        monkeypatch.setattr(cls, "do_list", lambda self, admin: [])
        monkeypatch.setattr(cls, "do_create", lambda self, admin, name:
                            _container.DomainResult(db=name, user="u", password="p",
                                                    sqlalchemy_uri=f"proto://{name}"))
        monkeypatch.setattr(cls, "do_delete", lambda self, admin, name: None)
    elif cat == "containers":               # druid (compose stack)
        from tabsdatax.itest.tdksystems.systems import _containers
        from tabsdatax.itest.tdksystems.systems.base import Health
        monkeypatch.setattr(_containers, "require_docker", lambda: None)
        monkeypatch.setattr(_containers, "compose_container_ids", lambda p, f: [])
        monkeypatch.setattr(cls, "_plan_endpoints", lambda self: {
            "OVERLORD_URL": "http://localhost:18081",
            "ROUTER_URL": "http://localhost:18888"})
        monkeypatch.setattr(cls, "_compose_pull", lambda self, p, env=None: None)
        monkeypatch.setattr(cls, "_compose_up", lambda self, p, env=None: None)
        monkeypatch.setattr(cls, "_compose_down", lambda self, p: None)
        monkeypatch.setattr(cls, "_wait_ready", lambda self, settings, t: None)
        monkeypatch.setattr(cls, "_system_health",
                            lambda self, meta: Health(True, "ready"))
        monkeypatch.setattr(cls, "_delete_datasource",
                            lambda self, url, ds, user="", password="": None)
    elif sysobj.slug == "bigquery":
        monkeypatch.setattr(cls, "_bq", lambda self, values: _FakeBq())
        monkeypatch.setattr(cls, "_gcs", lambda self, values: _FakeStore())
    elif cat in ("cloud", "native"):        # s3 / gcs / azureblob (fs is real)
        monkeypatch.setattr(cls, "_backend", lambda self, values: _FakeStore())
    elif cat == "saas":                     # snowflake / databricks / salesforce
        monkeypatch.setattr(cls, "_ping", lambda self, values: None)
        monkeypatch.setattr(cls, "create_schema",
                            lambda self, values, name, *, force=False: None)
        monkeypatch.setattr(cls, "drop_schema", lambda self, values, name: None)


class Provisioner:
    def __init__(self, monkeypatch, sys_path):
        self._mp = monkeypatch
        self._sys_path = sys_path

    def system(self, slug, name):
        sysobj = systems.get(slug)
        _mock_provisioning(self._mp, sysobj)
        args = ["--type", slug, "--sys", name]
        if sysobj.staging_root:
            args += ["--sys-path", str(self._sys_path)]
        args += _opt_args(sysobj.options)
        assert tdx._sys_create([*args, "-f"]) == 0

    def domain(self, slug, sysname, domname, *, model=None, data=None):
        sysobj = systems.get(slug)
        _mock_provisioning(self._mp, sysobj)
        args = ["--sys", sysname, "--domain", domname, *_opt_args(sysobj.domain_options)]
        if model:
            args += ["--model", model]
        if data:
            args += ["--data", data]
        assert tdx._domain_create([*args, "-f"]) == 0


@pytest.fixture
def provision(tmp_path, monkeypatch):
    monkeypatch.setenv(store.BACKENDS_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(crypto.ENV_VAR, "fixture-test-key-abcdef")
    monkeypatch.delenv("TD_TEST_ON_MISSING_BACKEND", raising=False)
    monkeypatch.delenv("TD_TEST_SKIP_BACKENDS", raising=False)
    crypto.use_secret_env(None)
    # fs validates its root lives under /tmp|/home|/Users and mkdirs it for real.
    sys_path = Path(f"/tmp/fixture-be-{tmp_path.name}")
    shutil.rmtree(sys_path, ignore_errors=True)
    yield Provisioner(monkeypatch, sys_path)
    shutil.rmtree(sys_path, ignore_errors=True)
