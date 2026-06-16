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
from types import SimpleNamespace

import pytest

from tabsdata._io.connections.connections import Conn
from tabsdata.api.apiserver import APIServerError


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

    The JSON is the canonical connection document
    :class:`NamedConn._load` expects -- ``kind``/``apiVersion`` envelope
    plus ``name``/``conn_configs``.
    """

    def _make(name: str = "finance", **overrides) -> Path:
        payload = {
            "kind": "stubSrcConn",
            "apiVersion": "1.0",
            "name": name,
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


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeAPIServer:
    """In-memory stand-in for the apiserver's collection-connection endpoints.

    Mirrors the response shapes :class:`ApiConnStore` consumes so the CLI
    tests exercise the full add/info/list/delete flows without a server.
    """

    def __init__(self):
        self.documents: dict[str, tuple[dict, list[str]]] = {}

    def _read(self, name: str) -> dict:
        document, secret_names = self.documents[name]
        return {
            "data": {
                "id": f"id-{name}",
                "connection": document,
                "secret_names": secret_names,
                "var_names": [],
            }
        }

    def collection_create(
        self,
        name,
        description,
        group=None,
        connection=None,
        secrets=None,
        variables=None,
        raise_for_status=True,
    ):
        if name in self.documents:
            raise APIServerError({"error_description": "already exists"})
        self.documents[name] = (
            connection,
            sorted({entry["key"] for entry in secrets or []}),
        )
        return _FakeResponse(201, {"data": {"name": name}})

    def collection_connection_upsert(
        self, name, connection, secrets=None, variables=None, raise_for_status=True
    ):
        if name not in self.documents:
            raise APIServerError(
                {"error_description": "the collection has no connection configured"}
            )
        self.documents[name] = (
            connection,
            sorted({entry["key"] for entry in secrets or []}),
        )
        return _FakeResponse(201, self._read(name))

    def collection_connection_get(self, name, raise_for_status=True):
        if name in self.documents:
            return _FakeResponse(200, self._read(name))
        if raise_for_status:
            raise APIServerError(
                {"error_description": "the collection has no connection configured"}
            )
        return _FakeResponse(404, {})

    def collection_list(self, request_len=None, **_kwargs):
        rows = [
            {"name": name, "connection_id": f"id-{name}"}
            for name in sorted(self.documents)
        ]
        return _FakeResponse(200, {"data": {"data": rows}})


@pytest.fixture()
def api_server(monkeypatch: pytest.MonkeyPatch) -> FakeAPIServer:
    """Wire the tdconn CLI to a fake server session."""
    fake = FakeAPIServer()

    def _init(ctx):
        ctx.obj["tabsdataserver"] = SimpleNamespace(connection=fake)

    monkeypatch.setattr(
        "tabsdata._cli.tdconn.cli.initialise_tabsdata_server_connection", _init
    )
    return fake
