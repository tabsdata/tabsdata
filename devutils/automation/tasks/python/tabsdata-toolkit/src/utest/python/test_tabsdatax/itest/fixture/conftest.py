#
# Copyright 2026 Tabsdata Inc.
#

"""Provision backends for the fixture tests by running the REAL ``tdd system
create`` / ``tdd domain create`` with ``--descriptor-only``: tdd writes exactly
the descriptors a create writes and provisions nothing, so the fixtures are
exercised against the descriptors tdd itself produces — not hand-rolled ones.

The `provision` fixture points tdd at a tmp store (``$TDD_SYSTEM_DIR`` /
``$TDD_DOMAIN_DIR``) with a fake key, and returns a helper whose
``.system(slug, name)`` / ``.domain(sys, dom)`` create into it. The
env-configured backends (object stores / warehouses / services / bigquery) read
their ``<PREFIX>__<KEY>`` settings from a fake environment set here.

In a real run the ``system_<name>`` / ``domain_<name>`` fixtures generate at
plugin import from the store tdd lists; here the store is made per test, so the
names the injection tests declare are fed to the same fixture factories
directly, and a listing that failed at import is no concern of theirs."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tabsdatax.itest.fixture import _tdd, backend

backend.LISTING_ERRORS.clear()

for declared_system_name in ("my_fs", "alpha", "beta", "db", "store", "wh"):
    globals()[f"system_{declared_system_name}"] = backend.system_fixture(
        declared_system_name
    )
for declared_domain_name in ("inbox", "outbox", "archive", "shop", "sales"):
    globals()[f"domain_{declared_domain_name}"] = backend.domain_fixture(
        declared_domain_name
    )


# Fake <PREFIX>__<KEY> config the env-configured backends read at create; a
# --descriptor-only create never uses it, so the values only need to be set.
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


class Provisioner:
    def __init__(self, sys_path):
        self._sys_path = sys_path

    def system(self, slug, name):
        argv = ["--type", slug, "--sys", name, "--force", "--descriptor-only"]
        if slug == "fs":
            argv += ["--sys-path", str(self._sys_path)]
        _tdd.run(_tdd.SYSTEM, "create", *argv)

    def domain(self, sysname, domname):
        _tdd.run(
            _tdd.DOMAIN,
            "create",
            "--sys",
            sysname,
            "--domain",
            domname,
            "--force",
            "--descriptor-only",
        )

    def external_name(self, domname):
        """The domain's name inside its system, as tdd chose it."""
        return _tdd.descriptor(_tdd.DOMAIN, domname)["external_name"]


@pytest.fixture
def provision(tmp_path, monkeypatch):
    monkeypatch.setenv(_tdd.ENV_STORE_DIR[_tdd.SYSTEM], str(tmp_path / "system"))
    monkeypatch.setenv(_tdd.ENV_STORE_DIR[_tdd.DOMAIN], str(tmp_path / "domain"))
    monkeypatch.setenv("_TDD_CRYPTO_KEY", "fixture-test-key-abcdef")
    for var in (
        *backend.ON_MISSING_ENV.values(),
        *backend.SKIP_ENV.values(),
        *backend.MANIFEST_ENV.values(),
    ):
        monkeypatch.delenv(var, raising=False)
    for key, value in _FAKE_ENV.items():
        monkeypatch.setenv(key, value)
    # fs keeps its data under a root it creates for real; keep it under /tmp.
    sys_path = Path(f"/tmp/fixture-be-{tmp_path.name}")
    shutil.rmtree(sys_path, ignore_errors=True)
    yield Provisioner(sys_path)
    shutil.rmtree(sys_path, ignore_errors=True)
