#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.common._error import CommonErrorCode, CommonRuntimeException
from tabsdatak.common._json_serde import (
    KeyValue,
    dehydrate,
    env_hydrate,
    env_secret_ref_vars,
    hydrate,
    list_refs,
    replace_at,
    resolve_env_secret_refs,
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
    # Value harvested, keyed by the jsonpath (prefixed SECRET_CONN__); leaf blanked.
    assert secrets == [KeyValue(key="SECRET_CONN__PASSWORD", value="hunter2")]
    assert out["spec"]["password"] == _secret(None)


def test_dehydrate_harvests_ref_secret_with_none_value():
    data = _wrap({"password": _secret(None)})
    out, secrets = dehydrate(data)
    assert secrets == [KeyValue(key="SECRET_CONN__PASSWORD", value=None)]
    assert out["spec"]["password"] == _secret(None)


def test_dehydrate_harvests_multiple_secrets():
    data = _wrap({"password": _secret("pw"), "token": _secret("tk")})
    out, secrets = dehydrate(data)
    assert secrets == [
        KeyValue(key="SECRET_CONN__PASSWORD", value="pw"),
        KeyValue(key="SECRET_CONN__TOKEN", value="tk"),
    ]
    assert out["spec"] == {"password": _secret(None), "token": _secret(None)}


def test_dehydrate_ignores_non_secret_prefixed_strings():
    # A bare ``var:``-looking string is now an ordinary literal, not harvested.
    data = _wrap({"schema": "var:etl", "password": _secret("pw")})
    out, secrets = dehydrate(data)
    assert secrets == [KeyValue(key="SECRET_CONN__PASSWORD", value="pw")]
    assert out["spec"]["schema"] == "var:etl"


def test_dehydrate_handles_list_paths():
    data = _wrap({"creds": [_secret("a"), _secret("b")]})
    _out, secrets = dehydrate(data)
    assert secrets == [
        KeyValue(key="SECRET_CONN__CREDS_0_", value="a"),
        KeyValue(key="SECRET_CONN__CREDS_1_", value="b"),
    ]


def test_dehydrate_key_uses_jsonpath():
    data = _wrap({"password": _secret("hunter2")})
    _out, secrets = dehydrate(data)
    assert secrets[0].key == "SECRET_CONN__PASSWORD"


def test_dehydrate_ignores_plain_strings():
    data = _wrap({"host": "db.example.com", "password": _secret("pw")})
    out, secrets = dehydrate(data)
    assert secrets == [KeyValue(key="SECRET_CONN__PASSWORD", value="pw")]
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
            KeyValue(key="SECRET_CONN__PASSWORD", value="hunter2"),
            KeyValue(key="SECRET_CONN__TOKEN", value="etl"),
        ],
    )
    assert out["spec"] == {"password": "secret:hunter2", "token": "secret:etl"}


def test_hydrate_does_not_mutate_input():
    data = _wrap({"password": _secret(None)})
    hydrate(data, [KeyValue(key="SECRET_CONN__PASSWORD", value="x")])
    assert data["spec"]["password"] == _secret(None)


def test_dehydrate_then_hydrate_round_trips():
    original = _wrap({"password": _secret("hunter2"), "token": _secret("etl")})
    dehydrated, secrets = dehydrate(original)
    assert hydrate(dehydrated, secrets) == original


def test_hydrate_rejects_none_value():
    data = _wrap({"password": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate(data, [KeyValue(key="SECRET_CONN__PASSWORD", value=None)])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_6


def test_hydrate_rejects_missing_keys():
    data = _wrap({"u": _secret(None), "p": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate(data, [KeyValue(key="SECRET_CONN__U", value="user")])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_7


def test_hydrate_rejects_extra_keys():
    data = _wrap({"u": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate(
            data,
            [
                KeyValue(key="SECRET_CONN__U", value="user"),
                KeyValue(key="SECRET_CONN__EXTRA", value="x"),
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
    assert list_refs(data) == ["SECRET_CONN__PASSWORD", "SECRET_CONN__SCHEMA"]


def test_list_refs_agrees_with_dehydrate_keys():
    data = _wrap({"password": _secret("pw"), "token": _secret("etl")})
    dehydrated, secrets = dehydrate(data)
    assert list_refs(dehydrated) == [kv.key for kv in secrets]


# ---------------------------------------------------------------------------
# env_hydrate
# ---------------------------------------------------------------------------


def test_env_hydrate_reads_prefixed_env_vars(monkeypatch):
    monkeypatch.setenv("SECRET_CONN__PASSWORD", "hunter2")
    monkeypatch.setenv("SECRET_CONN__TOKEN", "etl")
    data = _wrap({"password": _secret(None), "token": _secret(None)})
    out = env_hydrate(data)
    assert out["spec"] == {"password": "secret:hunter2", "token": "secret:etl"}


def test_env_hydrate_rejects_inline_plaintext_secret(monkeypatch):
    # A stored/dehydrated doc must carry only the ``":secret:"`` placeholder;
    # a leaf still holding a value (``"secret:<value>"``) is a plaintext secret
    # and is rejected.
    monkeypatch.setenv("SECRET_CONN__TOKEN", "from-env")
    data = _wrap({"token": _secret(None), "pw": _secret("keepme")})
    with pytest.raises(CommonRuntimeException) as exc_info:
        env_hydrate(data)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_13


def test_env_hydrate_raises_on_missing_env(monkeypatch):
    monkeypatch.delenv("SECRET_CONN__PASSWORD", raising=False)
    data = _wrap({"password": _secret(None)})
    with pytest.raises(CommonRuntimeException) as exc_info:
        env_hydrate(data)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_6


def test_env_hydrate_does_not_mutate_input(monkeypatch):
    monkeypatch.setenv("SECRET_CONN__PASSWORD", "hunter2")
    data = _wrap({"password": _secret(None)})
    env_hydrate(data)
    assert data["spec"]["password"] == _secret(None)


# ---------------------------------------------------------------------------
# resolve_env_secret_refs
# ---------------------------------------------------------------------------


def test_resolve_env_secret_refs_reads_named_env_vars(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "hunter2")
    monkeypatch.setenv("API_TOKEN", "etl")
    data = _wrap({"password": "$secret:DB_PASSWORD", "token": "$secret:API_TOKEN"})
    out = resolve_env_secret_refs(data)
    assert out["spec"] == {"password": "secret:hunter2", "token": "secret:etl"}


def test_resolve_env_secret_refs_empty_env_var_is_valid_empty_secret(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "")
    data = _wrap({"password": "$secret:DB_PASSWORD"})
    out = resolve_env_secret_refs(data)
    assert out["spec"]["password"] == "secret:"


def test_resolve_env_secret_refs_raises_on_missing_env(monkeypatch):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    data = _wrap({"password": "$secret:DB_PASSWORD"})
    with pytest.raises(CommonRuntimeException) as exc_info:
        resolve_env_secret_refs(data)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_12


def test_resolve_env_secret_refs_leaves_other_forms_untouched(monkeypatch):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    data = _wrap({
        "dehydrated": _secret(None),
        "inline": _secret("keepme"),
        "plain": "just-a-value",
    })
    out = resolve_env_secret_refs(data)
    assert out["spec"] == data["spec"]


def test_resolve_env_secret_refs_handles_list_and_nested_paths(monkeypatch):
    monkeypatch.setenv("A", "aa")
    monkeypatch.setenv("B", "bb")
    data = _wrap({"creds": ["$secret:A", {"inner": "$secret:B"}]})
    out = resolve_env_secret_refs(data)
    assert out["spec"]["creds"] == ["secret:aa", {"inner": "secret:bb"}]


def test_resolve_env_secret_refs_does_not_mutate_input(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "hunter2")
    data = _wrap({"password": "$secret:DB_PASSWORD"})
    resolve_env_secret_refs(data)
    assert data["spec"]["password"] == "$secret:DB_PASSWORD"


# ---------------------------------------------------------------------------
# env_secret_ref_vars
# ---------------------------------------------------------------------------


def test_env_secret_ref_vars_lists_sorted_unique_vars():
    data = _wrap({"pw": "$secret:B", "tok": "$secret:A", "dup": "$secret:A"})
    assert env_secret_ref_vars(data) == ["A", "B"]


def test_env_secret_ref_vars_empty_when_no_refs():
    data = _wrap({"pw": _secret("x"), "plain": "value"})
    assert env_secret_ref_vars(data) == []
