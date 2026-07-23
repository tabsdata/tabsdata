#
# Copyright 2026 Tabsdata Inc.
#

"""Provision backends for the fixture tests by running the REAL tdx-system2 /
tdx-domain2 create path in-process (via ``tdx._dispatch``) with the live
provisioning I/O stubbed, so the fixtures are exercised against exactly the
descriptors tdx writes — not hand-rolled ones.

The `provision` fixture points ``$TD_BACKENDS_DIR`` at a tmp store and returns a
helper whose ``.system(slug, name)`` / ``.domain(slug, sys, dom)`` drive the v2
create commands. Every backend's live provisioning (docker, boto3, cloud
clients, SQL, warehouses, compose) is stubbed to a no-op — the descriptor
content (from ``plan_system`` / ``domain_meta``) is unaffected. The env-config
backends (object stores / warehouses / services / bigquery) read their
``<PREFIX>__<KEY>`` settings from a fake environment set here.

In a real run the ``system_<name>`` / ``domain_<name>`` fixtures generate at
plugin import from the pre-existing store; here no store exists at import, so the
names the injection tests declare are fed to the same fixture factories directly,
and each test provisions its descriptors through `provision` as usual."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tabsdatax.itest.fixture import backend
from tabsdatax.itest.tdksystems2 import constants, crypto, store, systems, tdx

for declared_system_name in ("my_fs", "alpha", "beta", "db", "store", "wh"):
    globals()[f"system_{declared_system_name}"] = backend.system_fixture(
        declared_system_name
    )
for declared_domain_name in ("inbox", "outbox", "archive", "shop", "sales"):
    globals()[f"domain_{declared_domain_name}"] = backend.domain_fixture(
        declared_domain_name
    )


# Fake <PREFIX>__<KEY> config the env-configured backends read at create; the
# provisioning that would use it is stubbed, so the values only need to be set.
_FAKE_ENV = {
    "S3__BUCKET": "b",
    "S3__REGION": "r",
    "S3__ACCESS_KEY": "ak",
    "S3__SECRET_KEY": "sk",
    "S3__SYS_PATH": "root",
    "GCS__BUCKET": "b",
    "GCS__SERVICE_ACCOUNT_KEY": '{"type": "service_account"}',
    "GCS__SYS_PATH": "root",
    "AZ__CONTAINER": "c",
    "AZ__ACCOUNT_NAME": "acct",
    "AZ__ACCOUNT_KEY": "k",
    "AZ__SYS_PATH": "root",
    "SNOWFLAKE__ACCOUNT": "acct",
    "SNOWFLAKE__USERNAME": "u",
    "SNOWFLAKE__PAT": "p",
    "SNOWFLAKE__DATABASE": "DB",
    "SNOWFLAKE__WAREHOUSE": "WH",
    "DATABRICKS__HOST": "https://example.databricks.com",
    "DATABRICKS__TOKEN": "t",
    "DATABRICKS__WAREHOUSE_ID": "wid",
    "DATABRICKS__CATALOG": "CAT",
    "DATABRICKS__VOLUME": "vol",
    "BIGQUERY__PROJECT": "proj",
    "BIGQUERY__BUCKET": "b",
    "BIGQUERY__SERVICE_ACCOUNT_KEY": '{"type": "service_account"}',
    "BIGQUERY__SYS_PATH": "root",
    "ANTHROPIC__API_KEY": "sk-ant-test",
    "OPENAI__API_KEY": "sk-oai-test",
    "SALESFORCE__URL": "https://example.my.salesforce.com",
    "SALESFORCE__USERNAME": "u",
    "SALESFORCE__PASSWORD": "pw",
    "SALESFORCE__TOKEN": "tok",
}


def _stub_provisioning(monkeypatch, cls):
    """Skip a backend's live provisioning I/O; what create *writes* is real."""
    monkeypatch.setattr(
        cls, "provision_system", lambda self, meta, *, force=False: {}, raising=False
    )
    monkeypatch.setattr(
        cls,
        "provision_domain",
        lambda self, sys_meta, entity, dmeta, *, force=False: None,
        raising=False,
    )
    monkeypatch.setattr(
        cls, "list_domain_entities", lambda self, sys_meta: [], raising=False
    )


class Provisioner:
    def __init__(self, monkeypatch, sys_path):
        self._mp = monkeypatch
        self._sys_path = sys_path

    def system(self, slug, name):
        _stub_provisioning(self._mp, systems.get_class(slug))
        argv = ["create", "--type", slug, "--sys", name, "-f"]
        if slug == "fs":
            argv += ["--sys-path", str(self._sys_path)]
        assert tdx._dispatch(constants.SYS_TOOL, argv) == 0

    def domain(self, slug, sysname, domname):
        _stub_provisioning(self._mp, systems.get_class(slug))
        argv = ["create", "--sys", sysname, "--domain", domname, "-f"]
        assert tdx._dispatch(constants.DOM_TOOL, argv) == 0


@pytest.fixture
def provision(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.ENV_BACKENDS_DIR, str(tmp_path))
    monkeypatch.setenv(crypto.ENV_VAR, "fixture-test-key-abcdef")
    for var in (*backend.ON_MISSING_ENV.values(), *backend.SKIP_ENV.values()):
        monkeypatch.delenv(var, raising=False)
    for key, value in _FAKE_ENV.items():
        monkeypatch.setenv(key, value)
    # fs validates its root lives under /tmp|/home|/Users and mkdirs it for real.
    sys_path = Path(f"/tmp/fixture-be-{tmp_path.name}")
    shutil.rmtree(sys_path, ignore_errors=True)
    yield Provisioner(monkeypatch, sys_path)
    shutil.rmtree(sys_path, ignore_errors=True)
