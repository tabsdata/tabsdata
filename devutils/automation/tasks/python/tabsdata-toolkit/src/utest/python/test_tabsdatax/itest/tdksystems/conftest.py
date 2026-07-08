"""Shared pytest fixtures.

Descriptors are encrypted with `$TD_BACKENDS_CRYPTO_KEY`, so every test that writes or
reads one needs it set. The autouse fixture below provides a valid dev secret
for the whole unit suite; tests that exercise the "missing / short secret"
gate override it with `monkeypatch.delenv` / `monkeypatch.setenv`."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _td_dev_secret(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "utest-dev-secret")


@pytest.fixture(autouse=True)
def _reset_active_secret_env():
    """Every descriptor verb points crypto at its `--secret-env` (a module
    global). Reset it around each test so a `--secret-env FOO` case can't leak
    the active var into a later test that reads a descriptor directly."""
    from tabsdatax.itest.tdksystems import crypto
    crypto.use_secret_env(None)
    yield
    crypto.use_secret_env(None)


@pytest.fixture
def stores(tmp_path, monkeypatch):
    """Point the system & domain descriptor stores at fresh tmp dirs.

    tdx addresses systems/domains by *name*; this sets `$TD_BACKENDS_DIR` to
    `tmp_path`, so a test creates by name and then locates the file with
    `store.system_path(name)` / `store.domain_path(name)` — the same resolution
    the CLI uses. Returns `tmp_path` (the stores are `tmp_path/systems` and
    `tmp_path/domains`)."""
    monkeypatch.setenv("TD_BACKENDS_DIR", str(tmp_path))
    return tmp_path
