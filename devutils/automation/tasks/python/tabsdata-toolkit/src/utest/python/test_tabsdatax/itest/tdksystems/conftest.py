"""Shared pytest fixtures.

Descriptors are encrypted with `$TD_DEV_PERSONAL_SECRET`, so every test that writes or
reads one needs it set. The autouse fixture below provides a valid dev secret
for the whole unit suite; tests that exercise the "missing / short secret"
gate override it with `monkeypatch.delenv` / `monkeypatch.setenv`."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _td_dev_secret(monkeypatch):
    monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "utest-dev-secret")
