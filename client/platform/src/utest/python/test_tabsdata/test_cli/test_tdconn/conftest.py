#
# Copyright 2026 Tabs Data Inc.
#
"""Shared fixtures for the tdconn CLI unit tests.

:class:`StubConn` is registered as a ``Conn`` subclass at import time
so that ``NamedConn._load`` can resolve ``"type": "StubConn"`` payloads
without pulling real connector drivers into the test process.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tabsdata._io.connections.connections import Conn


class StubConn(Conn):
    """Minimal Conn subclass used exclusively by tdconn tests."""

    def __init__(self, **configs):
        self.configs = configs

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "StubConn":
        return cls(**configs)


@pytest.fixture()
def conn_file(tmp_path: Path):
    """Factory: write a minimal valid connection JSON and return its path.

    The JSON mirrors the ``connectionDef`` schema that
    :class:`NamedConn._load` expects -- ``kind``/``apiVersion`` envelope
    plus ``name``/``conn_type``/``meta_model``/``conn_configs``.
    """

    def _make(name: str = "finance", **overrides) -> Path:
        payload = {
            "kind": "connectionDef",
            "apiVersion": "1.0",
            "name": name,
            "conn_type": "StubConn",
            "conn_mode": "src",
            "meta_model": "sql",
            "description": "test connection",
            "conn_configs": {"uri": "stub://host/db"},
        }
        payload.update(overrides)
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    return _make


@pytest.fixture()
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``Path.home()`` at a temp dir so file-store roots don't leak."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("TD_PATH_INSTANCE", raising=False)
    monkeypatch.delenv("TD_PATH_CONFIG", raising=False)
    return home
