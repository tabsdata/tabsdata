from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.common._error import CommonErrorCode, CommonRuntimeException
from tabsdatak.common._json_serde import (
    KeyValue,
    dehydrate,
    env_hydrate,
    extract_connection_names,
    find_by_kind,
    hydrate,
    hydrate_connections,
    list_refs,
    replace_at,
)


def _env(kind: str, spec: dict | list) -> dict:
    """Build a minimal envelope. ``apiVersion`` / ``type`` content is
    irrelevant to the walker -- only ``kind`` is matched."""
    return {"kind": kind, "apiVersion": "1.0", "type": "X", "spec": spec}


def _wrap(spec: dict) -> dict:
    """Top-level envelope -- mimics a serialized SrcDef / DestDef so its
    ``spec`` provides the ``$.spec.`` prefix that ``_to_key`` strips."""
    return {"kind": "outer", "apiVersion": "1.0", "type": "Outer", "spec": spec}


# A secret string leaf has two wire forms: the hydrated ``"secret:<value>"``
# and the dehydrated marker ``":secret:"`` (``Secret().to_dict()``). This
# helper spells out the form the walker matches on.
def _secret(value: str | None) -> str:
    return f"secret:{value}" if value is not None else ":secret:"


# ---------------------------------------------------------------------------
# find_by_kind
# ---------------------------------------------------------------------------


def test_find_by_kind_on_empty_dict_returns_nothing():
    assert find_by_kind({}, "secret") == []


def test_find_by_kind_matches_root_envelope():
    root = _env("connectionRef", {"name": "PW"})
    assert find_by_kind(root, "connectionRef") == [("$", root)]


def test_find_by_kind_nested_under_dict_key():
    inner = _env("connectionRef", {"name": "PW"})
    root = _env("outer", {"creds": inner})
    assert find_by_kind(root, "connectionRef") == [("$.spec.creds", inner)]


def test_find_by_kind_nested_under_list_index():
    a = _env("connectionRef", {"i": 0})
    b = _env("connectionRef", {"i": 1})
    root = _env("outer", [a, b])
    assert find_by_kind(root, "connectionRef") == [("$.spec[0]", a), ("$.spec[1]", b)]


def test_find_by_kind_skips_non_dict_non_list_values():
    root = {"kind": "x", "spec": "not-a-container", "n": 1, "b": True, "z": None}
    assert find_by_kind(root, "x") == [("$", root)]


def test_find_by_kind_does_not_match_partial_envelopes():
    root = {"apiVersion": "1.0", "spec": {"name": "PW"}}
    assert find_by_kind(root, "connectionRef") == []


# ---------------------------------------------------------------------------
# replace_at
# ---------------------------------------------------------------------------


def test_replace_at_dict_key():
    root = _env("outer", {"creds": _env("x", {"name": "old"})})
    replace_at(root, "$.spec.creds", _env("x", {"name": "new"}))
    assert root["spec"]["creds"]["spec"]["name"] == "new"


def test_replace_at_list_index():
    root = _env("outer", [10, 20, 30])
    replace_at(root, "$.spec[1]", 99)
    assert root["spec"] == [10, 99, 30]


def test_replace_at_mutates_in_place_returns_none():
    root = _env("outer", {"x": 1})
    assert replace_at(root, "$.spec.x", 2) is None
    assert root["spec"]["x"] == 2


def test_replace_at_rejects_root_replacement():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "$", "anything")
    assert exc_info.value.api_error is CommonErrorCode.COMMON_5


def test_replace_at_rejects_path_without_dollar_prefix():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "spec.x", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_1


def test_replace_at_rejects_empty_dict_key():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "$.", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_2


def test_replace_at_rejects_unclosed_bracket():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": [1, 2]}, "$.x[0", 9)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_3


def test_replace_at_rejects_unexpected_char_after_root():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "$x", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_4


# ---------------------------------------------------------------------------
# Wire form sanity: Secret serializes to the strings the walker matches.
# ---------------------------------------------------------------------------


def test_secret_serialize_to_dehydrated_marker():
    # A Secret always serializes to the dehydrated marker; its value never
    # lands in the serialized form.
    assert Secret("hunter2").to_dict() == ":secret:"
    assert Secret().to_dict() == ":secret:"


# ---------------------------------------------------------------------------
# dehydrate
# ---------------------------------------------------------------------------


def test_dehydrate_empty_payload():
    out, secrets = dehydrate(_wrap({}))
    assert out == _wrap({})
    assert secrets == []


def test_dehydrate_harvests_literal_secret_and_blanks_it():
    data = _wrap({"password": _secret("hunter2")})
    out, secrets = dehydrate(data)
    # Value harvested, keyed by the jsonpath (prefixed SECRET_); leaf blanked.
    assert secrets == [KeyValue(key="SECRET_PASSWORD", value="hunter2")]
    assert out["spec"]["password"] == _secret(None)


def test_dehydrate_harvests_ref_secret_with_none_value():
    data = _wrap({"password": _secret(None)})
    out, secrets = dehydrate(data)
    assert secrets == [KeyValue(key="SECRET_PASSWORD", value=None)]
    assert out["spec"]["password"] == _secret(None)


def test_dehydrate_harvests_multiple_secrets():
    data = _wrap({"password": _secret("pw"), "token": _secret("tk")})
    out, secrets = dehydrate(data)
    assert secrets == [
        KeyValue(key="SECRET_PASSWORD", value="pw"),
        KeyValue(key="SECRET_TOKEN", value="tk"),
    ]
    assert out["spec"] == {"password": _secret(None), "token": _secret(None)}


def test_dehydrate_ignores_non_secret_prefixed_strings():
    # A bare ``var:``-looking string is now an ordinary literal, not harvested.
    data = _wrap({"schema": "var:etl", "password": _secret("pw")})
    out, secrets = dehydrate(data)
    assert secrets == [KeyValue(key="SECRET_PASSWORD", value="pw")]
    assert out["spec"]["schema"] == "var:etl"


def test_dehydrate_handles_list_paths():
    data = _wrap({"creds": [_secret("a"), _secret("b")]})
    _out, secrets = dehydrate(data)
    assert secrets == [
        KeyValue(key="SECRET_CREDS_0_", value="a"),
        KeyValue(key="SECRET_CREDS_1_", value="b"),
    ]


def test_dehydrate_key_uses_jsonpath():
    data = _wrap({"password": _secret("hunter2")})
    _out, secrets = dehydrate(data)
    assert secrets[0].key == "SECRET_PASSWORD"


def test_dehydrate_ignores_plain_strings():
    data = _wrap({"host": "db.example.com", "password": _secret("pw")})
    out, secrets = dehydrate(data)
    assert secrets == [KeyValue(key="SECRET_PASSWORD", value="pw")]
    assert out["spec"]["host"] == "db.example.com"


def test_dehydrate_does_not_mutate_input():
    data = _wrap({"password": _secret("hunter2")})
    dehydrate(data)
    assert data["spec"]["password"] == "secret:hunter2"


# ---------------------------------------------------------------------------
# hydrate
# ---------------------------------------------------------------------------


def test_hydrate_restores_values():
    data = _wrap({"password": _secret(None), "token": _secret(None)})
    out = hydrate(
        data,
        [
            KeyValue(key="SECRET_PASSWORD", value="hunter2"),
            KeyValue(key="SECRET_TOKEN", value="etl"),
        ],
    )
    assert out["spec"] == {"password": "secret:hunter2", "token": "secret:etl"}


def test_hydrate_does_not_mutate_input():
    data = _wrap({"password": _secret(None)})
    hydrate(data, [KeyValue(key="SECRET_PASSWORD", value="x")])
    assert data["spec"]["password"] == _secret(None)


def test_dehydrate_then_hydrate_round_trips():
    original = _wrap({"password": _secret("hunter2"), "token": _secret("etl")})
    dehydrated, secrets = dehydrate(original)
    assert hydrate(dehydrated, secrets) == original


def test_hydrate_rejects_none_value():
    data = _wrap({"password": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate(data, [KeyValue(key="SECRET_PASSWORD", value=None)])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_6


def test_hydrate_rejects_missing_keys():
    data = _wrap({"u": _secret(None), "p": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate(data, [KeyValue(key="SECRET_U", value="user")])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_7


def test_hydrate_rejects_extra_keys():
    data = _wrap({"u": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate(
            data,
            [
                KeyValue(key="SECRET_U", value="user"),
                KeyValue(key="SECRET_EXTRA", value="x"),
            ],
        )
    assert exc_info.value.api_error is CommonErrorCode.COMMON_7


# ---------------------------------------------------------------------------
# list_refs
# ---------------------------------------------------------------------------


def test_list_refs_empty():
    assert list_refs(_wrap({})) == []


def test_list_refs_returns_prefixed_keys():
    data = _wrap({"password": _secret("pw"), "schema": _secret(None)})
    assert list_refs(data) == ["SECRET_PASSWORD", "SECRET_SCHEMA"]


def test_list_refs_agrees_with_dehydrate_keys():
    data = _wrap({"password": _secret("pw"), "token": _secret("etl")})
    dehydrated, secrets = dehydrate(data)
    assert list_refs(dehydrated) == [kv.key for kv in secrets]


# ---------------------------------------------------------------------------
# env_hydrate
# ---------------------------------------------------------------------------


def test_env_hydrate_reads_prefixed_env_vars(monkeypatch):
    monkeypatch.setenv("SECRET_PASSWORD", "hunter2")
    monkeypatch.setenv("SECRET_TOKEN", "etl")
    data = _wrap({"password": _secret(None), "token": _secret(None)})
    out = env_hydrate(data)
    assert out["spec"] == {"password": "secret:hunter2", "token": "secret:etl"}


def test_env_hydrate_leaves_inline_literals_untouched(monkeypatch):
    monkeypatch.setenv("SECRET_TOKEN", "from-env")
    data = _wrap({"token": _secret(None), "pw": _secret("keepme")})
    out = env_hydrate(data)
    assert out["spec"]["token"] == "secret:from-env"
    assert out["spec"]["pw"] == "secret:keepme"


def test_env_hydrate_raises_on_missing_env(monkeypatch):
    monkeypatch.delenv("SECRET_PASSWORD", raising=False)
    data = _wrap({"password": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        env_hydrate(data)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_6


def test_env_hydrate_does_not_mutate_input(monkeypatch):
    monkeypatch.setenv("SECRET_PASSWORD", "hunter2")
    data = _wrap({"password": _secret(None)})
    env_hydrate(data)
    assert data["spec"]["password"] == _secret(None)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _conn_ref(name: str) -> dict:
    return {
        "kind": "connectionRef",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:NamedConn",
        "spec": {"name": name},
    }


def _conn_def(label: str) -> dict:
    return {
        "kind": "connectionDef",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:SomeConn",
        "spec": {"label": label},
    }


def test_extract_connection_names_empty():
    assert extract_connection_names(_wrap({})) == []


def test_extract_connection_names_returns_literal_names():
    data = _wrap({"conn": _conn_ref("prod_pg")})
    assert extract_connection_names(data) == ["prod_pg"]


def test_extract_connection_names_dedupes_in_order():
    data = _wrap(
        {"src": _conn_ref("b"), "dst": _conn_ref("a"), "also": _conn_ref("b")}
    )
    assert extract_connection_names(data) == ["b", "a"]


def test_extract_connection_names_ignores_connection_defs():
    data = _wrap({"inline": _conn_def("x"), "ref": _conn_ref("y")})
    assert extract_connection_names(data) == ["y"]


def test_hydrate_connections_replaces_ref_with_def():
    data = _wrap({"conn": _conn_ref("prod_pg")})
    out = hydrate_connections(data, {"prod_pg": _conn_def("prod_pg")})
    assert out["spec"]["conn"]["kind"] == "connectionDef"
    assert out["spec"]["conn"]["spec"]["label"] == "prod_pg"


def test_hydrate_connections_does_not_mutate_input():
    data = _wrap({"conn": _conn_ref("prod_pg")})
    hydrate_connections(data, {"prod_pg": _conn_def("prod_pg")})
    assert data["spec"]["conn"]["kind"] == "connectionRef"


def test_hydrate_connections_replaces_each_ref_independently():
    data = _wrap({"a": _conn_ref("x"), "b": _conn_ref("x")})
    conn_by_name = {"x": _conn_def("x")}
    out = hydrate_connections(data, conn_by_name)
    out["spec"]["a"]["spec"]["label"] = "tainted"
    assert out["spec"]["b"]["spec"]["label"] == "x"
    assert conn_by_name["x"]["spec"]["label"] == "x"


def test_hydrate_connections_raises_on_unknown_name():
    data = _wrap({"conn": _conn_ref("ghost")})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_connections(data, {"prod_pg": _conn_def("prod_pg")})
    assert exc_info.value.api_error is CommonErrorCode.COMMON_8


def test_hydrate_connections_raises_when_root_is_ref():
    data = _conn_ref("x")
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_connections(data, {"x": _conn_def("x")})
    assert exc_info.value.api_error is CommonErrorCode.COMMON_5


def test_hydrate_connections_leaves_connection_defs_alone():
    data = _wrap({"inline": _conn_def("already-here"), "ref": _conn_ref("x")})
    out = hydrate_connections(data, {"x": _conn_def("x")})
    assert out["spec"]["inline"]["spec"]["label"] == "already-here"
    assert out["spec"]["ref"]["spec"]["label"] == "x"
