#
# Copyright 2026 Tabsdata Inc.
#

"""Shared fixtures for the tdksystems2 unit suite.

Descriptors are encrypted with ``$TD_BACKENDS_CRYPTO_KEY``, so every test that
reads or writes one needs it set. The build identity is pinned so the default
external prefix ``d_<user>_<build>_`` (``d_u_0_``) is deterministic. ``stores``
points the descriptor stores at a fresh tmp dir.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _td_dev_secret(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "utest-dev-secret")


@pytest.fixture(autouse=True)
def _fixed_build_identity(monkeypatch):
    monkeypatch.setenv("TD_BUILD_USER", "u")
    monkeypatch.setenv("TD_BUILD_NUMBER", "0")


@pytest.fixture
def stores(tmp_path, monkeypatch):
    """Point the system & domain stores at fresh tmp dirs via ``$TD_BACKENDS_DIR``
    (so they resolve to ``tmp_path/systems2`` and ``tmp_path/domains2``)."""
    monkeypatch.setenv("TD_BACKENDS_DIR", str(tmp_path))
    return tmp_path
