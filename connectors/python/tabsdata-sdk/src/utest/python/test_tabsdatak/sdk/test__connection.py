#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.sdk._connection import load_connection


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _Api:
    """Minimal apiserver stub recording the collection it was asked for."""

    def __init__(self, payload, *, as_response=True):
        self._payload = payload
        self._as_response = as_response
        self.calls: list[str] = []

    def collection_connection_get(self, name):
        self.calls.append(name)
        return _Resp(self._payload) if self._as_response else self._payload


def _localfile_doc():
    # A real, secret-free connectionDef document, obtained by round-tripping a
    # constructible reference connection so the test never hard-codes the wire
    # shape.
    from tabsdatak.conn.localfile import LocalFileSrcConn

    doc = LocalFileSrcConn().to_dict()
    return LocalFileSrcConn, doc


def test_load_connection_fetches_and_deserializes():
    from_cls, doc = _localfile_doc()
    api = _Api({"data": {"connection": doc, "secret_names": [], "var_names": []}})

    conn = load_connection(api, "mycoll")

    assert api.calls == ["mycoll"]
    assert isinstance(conn, from_cls)
    # Secret-free doc ⇒ hydration is a no-op; the round-trip is faithful.
    assert conn.to_dict() == doc


def test_load_connection_accepts_a_plain_dict_return():
    # Some clients return the parsed body rather than a requests.Response.
    from_cls, doc = _localfile_doc()
    api = _Api(
        {"data": {"connection": doc, "secret_names": [], "var_names": []}},
        as_response=False,
    )

    conn = load_connection(api, "mycoll")

    assert isinstance(conn, from_cls)


def test_load_connection_hydrate_false_skips_env():
    # With hydrate=False, no environment lookup happens (and a secret-free doc
    # still round-trips cleanly).
    from_cls, doc = _localfile_doc()
    api = _Api({"data": {"connection": doc, "secret_names": [], "var_names": []}})

    conn = load_connection(api, "mycoll", hydrate=False)

    assert isinstance(conn, from_cls)
