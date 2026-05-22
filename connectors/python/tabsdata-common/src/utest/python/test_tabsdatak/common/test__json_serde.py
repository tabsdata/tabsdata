from __future__ import annotations

import pytest

from tabsdatak.api import Var, Secret, _LiteralSecret
from tabsdatak.error import ApiInternalException
from tabsdatak.common._error import CommonErrorCode, CommonRuntimeException
from tabsdatak.common._json_serde import (
    KeyValue,
    dehydrate_secrets,
    extract_connection_names,
    extract_vars,
    extract_secrets,
    find_by_kind,
    hydrate_connections,
    hydrate_secrets,
    hydrate_secrets_as_envs,
    replace_at,
)


def _env(kind: str, spec: dict | list) -> dict:
    """Build a minimal envelope. ``apiVersion`` / ``type`` content is
    irrelevant to the walker -- only ``kind`` is matched."""
    return {"kind": kind, "apiVersion": "1.0", "type": "X", "spec": spec}


# ---------------------------------------------------------------------------
# find_by_kind
# ---------------------------------------------------------------------------


def test_find_by_kind_on_empty_dict_returns_nothing():
    assert find_by_kind({}, "secret") == []


def test_find_by_kind_on_empty_list_returns_nothing():
    assert find_by_kind([], "secret") == []


def test_find_by_kind_matches_root_envelope():
    root = _env("secret", {"name": "PW"})
    results = find_by_kind(root, "secret")
    assert results == [("$", root)]


def test_find_by_kind_returns_dollar_path_for_root_match():
    # The walker enters with path ``$`` and reports the root unchanged.
    root = _env("secret", {})
    path, node = find_by_kind(root, "secret")[0]
    assert path == "$"
    assert node is root


def test_find_by_kind_nested_under_dict_key():
    inner = _env("secret", {"name": "PW"})
    root = _env("outer", {"creds": inner})
    results = find_by_kind(root, "secret")
    # Root matches ``outer``, not ``secret``. The inner envelope lives
    # under ``.spec.creds`` and is the only match.
    assert results == [("$.spec.creds", inner)]


def test_find_by_kind_nested_under_list_index():
    a = _env("secret", {"i": 0})
    b = _env("secret", {"i": 1})
    root = _env("outer", [a, b])
    results = find_by_kind(root, "secret")
    assert results == [("$.spec[0]", a), ("$.spec[1]", b)]


def test_find_by_kind_descends_into_matched_envelopes():
    inner = _env("secret", {"i": 1})
    outer = _env("secret", {"inner": inner})
    results = find_by_kind(outer, "secret")
    # The outer envelope matches, and the walker keeps descending into
    # it, so the nested match is also reported.
    assert results == [("$", outer), ("$.spec.inner", inner)]


def test_find_by_kind_skips_non_dict_non_list_values():
    # Strings, ints, None must not raise and must not be reported.
    root = {"kind": "x", "spec": "not-a-container", "n": 1, "b": True, "z": None}
    assert find_by_kind(root, "x") == [("$", root)]


def test_find_by_kind_no_match_returns_empty_list():
    root = _env("outer", {"inner": _env("middle", {})})
    assert find_by_kind(root, "secret") == []


def test_find_by_kind_does_not_match_partial_envelopes():
    # A dict without a ``kind`` key is not an envelope and must not match
    # under any kind.
    root = {"apiVersion": "1.0", "spec": {"name": "PW"}}
    assert find_by_kind(root, "secret") == []


def test_find_by_kind_returns_same_object_reference():
    inner = _env("secret", {})
    root = _env("outer", {"x": inner})
    [(_, node)] = find_by_kind(root, "secret")
    # ``node`` is the actual dict from the tree, not a copy -- callers
    # rely on this for in-place mutation.
    assert node is inner


# ---------------------------------------------------------------------------
# replace_at -- happy paths
# ---------------------------------------------------------------------------


def test_replace_at_dict_key():
    root = _env("outer", {"creds": _env("secret", {"name": "old"})})
    replace_at(root, "$.spec.creds", _env("secret", {"name": "new"}))
    assert root["spec"]["creds"]["spec"]["name"] == "new"


def test_replace_at_list_index():
    root = _env("outer", [10, 20, 30])
    replace_at(root, "$.spec[1]", 99)
    assert root["spec"] == [10, 99, 30]


def test_replace_at_deeply_nested_mixed_path():
    inner = _env("secret", {"name": "old"})
    root = _env("outer", {"items": [{"creds": inner}]})
    replace_at(root, "$.spec.items[0].creds", _env("secret", {"name": "new"}))
    assert root["spec"]["items"][0]["creds"]["spec"]["name"] == "new"


def test_replace_at_mutates_in_place_returns_none():
    root = _env("outer", {"x": 1})
    assert replace_at(root, "$.spec.x", 2) is None
    assert root["spec"]["x"] == 2


def test_replace_at_using_path_from_find_by_kind_round_trip():
    inner = _env("secret", {"name": "old"})
    root = _env("outer", {"creds": inner})
    [(path, _)] = find_by_kind(root, "secret")
    replace_at(root, path, _env("secret", {"name": "new"}))
    assert root["spec"]["creds"]["spec"]["name"] == "new"


# ---------------------------------------------------------------------------
# replace_at -- error paths
# ---------------------------------------------------------------------------


def test_replace_at_rejects_root_replacement():
    # The caller already holds the root reference; we refuse rather than
    # silently no-op.
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "$", "anything")
    assert exc_info.value.api_error is CommonErrorCode.COMMON_5


def test_replace_at_rejects_path_without_dollar_prefix():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "spec.x", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_1


def test_replace_at_rejects_empty_dict_key():
    # ``$.`` with nothing after the dot -- malformed.
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "$.", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_2


def test_replace_at_rejects_consecutive_dots():
    # ``$.a..b`` -- the second dot starts a key but the third dot ends it
    # immediately, producing an empty key.
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"a": {"b": 1}}, "$.a..b", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_2


def test_replace_at_rejects_unclosed_bracket():
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": [1, 2]}, "$.x[0", 9)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_3


def test_replace_at_rejects_unexpected_char_after_root():
    # Anything that isn't ``.`` or ``[`` immediately after ``$`` is a
    # syntax error.
    with pytest.raises(CommonRuntimeException) as exc_info:
        replace_at({"x": 1}, "$x", 2)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_4


# ---------------------------------------------------------------------------
# Secret / var envelope helpers
#
# Realistic envelope shapes:
#   secretRef:     {"kind": "secretRef",     "spec": {"_s": "<name>", "_value_pattern": "..."}}
#   secretLiteral: {"kind": "secretLiteral", "spec": {"_s": "<value>", "_value_pattern": "..."}}
#   varRef:        {"kind": "varRef",        "spec": {"s": "<name>"}}
#   varLiteral:    {"kind": "varLiteral",    "spec": {"s": "<value>"}}
# ---------------------------------------------------------------------------


def _secret_ref(name: str, *, pattern: str | None = None) -> dict:
    # ``Secret()`` is the only public constructor shape -- private
    # fields are ``init=False``. Use the same dict-input validator
    # path the framework uses to seed an instance with a named ref.
    return Secret.__pydantic_validator__.validate_python(
        {"_s": name, "_value_pattern": pattern}
    ).to_dict()


def _secret_literal(value: str, *, pattern: str | None = None) -> dict:
    return _LiteralSecret(_s=value, _value_pattern=pattern).to_dict()


def _var_ref(name: str) -> dict:
    return Var(s=name).to_dict()


def _var_literal(value: str) -> dict:
    return Var.lit(value).to_dict()


def _wrap(spec: dict) -> dict:
    """Top-level envelope -- mimics a serialized SrcDef / DestDef so its
    ``spec`` provides the ``$.spec.`` prefix that ``_to_key`` strips."""
    return {"kind": "outer", "apiVersion": "1.0", "type": "Outer", "spec": spec}


# --- extract_secrets -------------------------------------------------------


def test_extract_secrets_on_empty_payload_returns_empty():
    assert extract_secrets(_wrap({})) == []


def test_extract_secrets_returns_ref_without_value():
    data = _wrap({"password": _secret_ref("PW")})
    [kv] = extract_secrets(data)
    # Key comes from the jsonpath, not from the ref's ``_s`` -- ``_s``
    # is only the ref's display name.
    assert kv == KeyValue(key="PASSWORD", value=None)


def test_extract_secrets_returns_literal_with_value():
    data = _wrap({"password": _secret_literal("hunter2")})
    [kv] = extract_secrets(data)
    assert kv == KeyValue(key="PASSWORD", value="hunter2")


def test_extract_secrets_mixes_refs_and_literals():
    data = _wrap(
        {
            "password": _secret_ref("PW"),
            "token": _secret_literal("abc"),
        }
    )
    result = extract_secrets(data)
    # The harvest visits refs before literals, so the ref comes first.
    assert result == [
        KeyValue(key="PASSWORD", value=None),
        KeyValue(key="TOKEN", value="abc"),
    ]


def test_extract_secrets_key_uses_jsonpath_not_ref_name():
    # ``_s`` is "DIFFERENT_NAME" but the key is derived purely from the
    # jsonpath -- ``$.spec.password`` → ``PASSWORD``.
    data = _wrap({"password": _secret_ref("DIFFERENT_NAME")})
    [kv] = extract_secrets(data)
    assert kv.key == "PASSWORD"


def test_extract_secrets_handles_list_paths():
    data = _wrap({"creds": [_secret_ref("A"), _secret_literal("b")]})
    result = extract_secrets(data)
    assert result == [
        KeyValue(key="CREDS_0_", value=None),
        KeyValue(key="CREDS_1_", value="b"),
    ]


def test_extract_secrets_ignores_var_envelopes():
    data = _wrap({"u": _var_ref("USER"), "p": _secret_ref("PW")})
    result = extract_secrets(data)
    assert result == [KeyValue(key="P", value=None)]


# --- extract_vars ----------------------------------------------------------


def test_extract_vars_returns_ref_without_value():
    data = _wrap({"user": _var_ref("DB_USER")})
    [kv] = extract_vars(data)
    # No "SECRET_" prefix for envs.
    assert kv == KeyValue(key="USER", value=None)


def test_extract_vars_returns_literal_with_value():
    data = _wrap({"user": _var_literal("etl")})
    [kv] = extract_vars(data)
    assert kv == KeyValue(key="USER", value="etl")


def test_extract_vars_ignores_secret_envelopes():
    data = _wrap({"u": _var_ref("USER"), "p": _secret_ref("PW")})
    result = extract_vars(data)
    assert result == [KeyValue(key="U", value=None)]


# --- dehydrate_secrets -----------------------------------------------------


def test_dehydrate_secrets_rewrites_literal_to_ref():
    data = _wrap({"password": _secret_literal("hunter2")})
    out = dehydrate_secrets(data)
    secret = out["spec"]["password"]
    assert secret["kind"] == "secretRef"
    # Literal value is dropped -- replaced by the placeholder.
    assert secret["spec"]["_s"] == "<secret>"


def test_dehydrate_secrets_preserves_value_pattern():
    data = _wrap({"password": _secret_literal("hunter2", pattern="^.{6,}$")})
    out = dehydrate_secrets(data)
    assert out["spec"]["password"]["spec"]["_value_pattern"] == "^.{6,}$"


def test_dehydrate_secrets_does_not_mutate_input():
    data = _wrap({"password": _secret_literal("hunter2")})
    before = data["spec"]["password"]["kind"]
    dehydrate_secrets(data)
    assert data["spec"]["password"]["kind"] == before == "secretLiteral"


def test_dehydrate_secrets_leaves_refs_alone():
    data = _wrap({"password": _secret_ref("PW")})
    out = dehydrate_secrets(data)
    # The placeholder rewrite only applies to literals; existing refs
    # are copied through untouched.
    assert out["spec"]["password"]["spec"]["_s"] == "PW"


def test_dehydrate_secrets_ignores_var_envelopes():
    data = _wrap({"user": _var_literal("etl"), "pw": _secret_literal("hunter2")})
    out = dehydrate_secrets(data)
    assert out["spec"]["user"]["kind"] == "varLiteral"
    assert out["spec"]["user"]["spec"]["s"] == "etl"
    assert out["spec"]["pw"]["kind"] == "secretRef"


# --- hydrate_secrets -- happy paths ----------------------------------------


def test_hydrate_secrets_rewrites_ref_to_literal_with_value():
    data = _wrap({"password": _secret_ref("PW")})
    secrets = [KeyValue(key="PASSWORD", value="hunter2")]
    out = hydrate_secrets(data, secrets)
    secret = out["spec"]["password"]
    assert secret["kind"] == "secretLiteral"
    assert secret["spec"]["_s"] == "hunter2"


def test_hydrate_secrets_preserves_value_pattern():
    data = _wrap({"password": _secret_ref("PW", pattern="^.{6,}$")})
    secrets = [KeyValue(key="PASSWORD", value="hunter2")]
    out = hydrate_secrets(data, secrets)
    assert out["spec"]["password"]["spec"]["_value_pattern"] == "^.{6,}$"


def test_hydrate_secrets_does_not_mutate_input():
    data = _wrap({"password": _secret_ref("PW")})
    hydrate_secrets(data, [KeyValue(key="PASSWORD", value="hunter2")])
    # Input dehydrated tree is untouched -- callers can re-use it.
    assert data["spec"]["password"]["kind"] == "secretRef"


def test_hydrate_secrets_dehydrate_round_trip():
    original = _wrap({"password": _secret_literal("hunter2", pattern="^.+$")})
    dehydrated = dehydrate_secrets(original)
    rehydrated = hydrate_secrets(
        dehydrated, [KeyValue(key="PASSWORD", value="hunter2")]
    )
    # The placeholder ``<secret>`` from dehydration is replaced; the
    # pattern survives both legs.
    assert rehydrated["spec"]["password"]["kind"] == "secretLiteral"
    assert rehydrated["spec"]["password"]["spec"] == {
        "_s": "hunter2",
        "_value_pattern": "^.+$",
    }


# --- hydrate_secrets -- error paths ----------------------------------------


def test_hydrate_secrets_rejects_entries_with_none_value():
    data = _wrap({"password": _secret_ref("PW")})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_secrets(data, [KeyValue(key="PASSWORD", value=None)])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_6


def test_hydrate_secrets_rejects_missing_secret_keys():
    # data needs two secrets but only one value is supplied.
    data = _wrap({"u": _secret_ref("U"), "p": _secret_ref("P")})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_secrets(data, [KeyValue(key="U", value="user")])
    assert exc_info.value.api_error is CommonErrorCode.COMMON_7


def test_hydrate_secrets_rejects_extra_secret_keys():
    data = _wrap({"u": _secret_ref("U")})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_secrets(
            data,
            [
                KeyValue(key="U", value="user"),
                KeyValue(key="EXTRA", value="x"),
            ],
        )
    assert exc_info.value.api_error is CommonErrorCode.COMMON_7


# --- hydrate_secrets_as_envs ----------------------------------------------


def test_hydrate_secrets_as_envs_reads_prefixed_env_vars(monkeypatch):
    data = _wrap({"password": _secret_ref("PW"), "token": _secret_ref("TK")})
    monkeypatch.setenv("MY_PASSWORD", "hunter2")
    monkeypatch.setenv("MY_TOKEN", "abc")
    out = hydrate_secrets_as_envs(data, "MY_")
    assert out["spec"]["password"]["kind"] == "secretLiteral"
    assert out["spec"]["password"]["spec"]["_s"] == "hunter2"
    assert out["spec"]["token"]["spec"]["_s"] == "abc"


def test_hydrate_secrets_as_envs_empty_prefix(monkeypatch):
    data = _wrap({"password": _secret_ref("PW")})
    monkeypatch.setenv("PASSWORD", "hunter2")
    out = hydrate_secrets_as_envs(data, "")
    assert out["spec"]["password"]["spec"]["_s"] == "hunter2"


def test_hydrate_secrets_as_envs_raises_on_missing_env(monkeypatch):
    data = _wrap({"password": _secret_ref("PW")})
    monkeypatch.delenv("MY_PASSWORD", raising=False)
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_secrets_as_envs(data, "MY_")
    assert exc_info.value.api_error is CommonErrorCode.COMMON_6


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _conn_ref(name: str | dict) -> dict:
    """Serialized ``NamedConn`` envelope. ``name`` may be a literal string
    or an ``varRef`` / ``varLiteral`` envelope dict."""
    return {
        "kind": "connectionRef",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:NamedConn",
        "spec": {"name": name},
    }


def _conn_def(label: str) -> dict:
    """Stand-in for a serialized concrete ``Conn`` (``kind`` ==
    ``connectionDef``). The ``label`` field is just to confirm identity
    after replacement."""
    return {
        "kind": "connectionDef",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:SomeConn",
        "spec": {"label": label},
    }


# --- extract_connection_names ---------------------------------------------


def test_extract_connection_names_on_empty_payload_returns_empty():
    assert extract_connection_names(_wrap({})) == []


def test_extract_connection_names_returns_literal_string_name():
    data = _wrap({"conn": _conn_ref("prod-pg")})
    assert extract_connection_names(data) == ["prod-pg"]


def test_extract_connection_names_resolves_var_literal_name():
    # name is an Var literal -- value is the inlined string.
    data = _wrap({"conn": _conn_ref(Var.lit("staging-mysql").to_dict())})
    assert extract_connection_names(data) == ["staging-mysql"]


def test_extract_connection_names_resolves_var_ref_name(monkeypatch):
    monkeypatch.setenv("DB_CONN", "prod-pg")
    data = _wrap({"conn": _conn_ref(Var(s="DB_CONN").to_dict())})
    assert extract_connection_names(data) == ["prod-pg"]


def test_extract_connection_names_raises_on_unset_var_ref(monkeypatch):
    monkeypatch.delenv("DB_CONN", raising=False)
    data = _wrap({"conn": _conn_ref(Var(s="DB_CONN").to_dict())})
    with pytest.raises(CommonRuntimeException) as exc_info:
        extract_connection_names(data)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_9


def test_extract_connection_names_raises_on_unsupported_name_shape():
    # ``name`` is neither a string nor a var envelope.
    data = _wrap({"conn": _conn_ref({"kind": "something-else", "spec": {}})})
    with pytest.raises(CommonRuntimeException) as exc_info:
        extract_connection_names(data)
    assert exc_info.value.api_error is CommonErrorCode.COMMON_10


def test_extract_connection_names_dedupes_in_order():
    # Two refs pointing at the same name → one entry; original order kept.
    data = _wrap(
        {
            "src": _conn_ref("b"),
            "dst": _conn_ref("a"),
            "also": _conn_ref("b"),
        }
    )
    assert extract_connection_names(data) == ["b", "a"]


def test_extract_connection_names_ignores_connection_defs():
    # A connectionDef sitting in the tree is not a ref and must not be
    # reported.
    data = _wrap({"inline": _conn_def("x"), "ref": _conn_ref("y")})
    assert extract_connection_names(data) == ["y"]


# --- hydrate_connections --------------------------------------------------


def test_hydrate_connections_replaces_ref_with_def():
    data = _wrap({"conn": _conn_ref("prod-pg")})
    out = hydrate_connections(data, {"prod-pg": _conn_def("prod-pg")})
    assert out["spec"]["conn"]["kind"] == "connectionDef"
    assert out["spec"]["conn"]["spec"]["label"] == "prod-pg"


def test_hydrate_connections_does_not_mutate_input():
    data = _wrap({"conn": _conn_ref("prod-pg")})
    hydrate_connections(data, {"prod-pg": _conn_def("prod-pg")})
    assert data["spec"]["conn"]["kind"] == "connectionRef"


def test_hydrate_connections_replaces_each_ref_independently():
    # Two refs to the same name → each replacement is its own deep copy,
    # so mutating one must not affect the other.
    data = _wrap({"a": _conn_ref("x"), "b": _conn_ref("x")})
    conn_by_name = {"x": _conn_def("x")}
    out = hydrate_connections(data, conn_by_name)
    out["spec"]["a"]["spec"]["label"] = "tainted"
    assert out["spec"]["b"]["spec"]["label"] == "x"
    # And the source map is untouched.
    assert conn_by_name["x"]["spec"]["label"] == "x"


def test_hydrate_connections_resolves_var_named_refs(monkeypatch):
    monkeypatch.setenv("DB_CONN", "prod-pg")
    data = _wrap({"conn": _conn_ref(Var(s="DB_CONN").to_dict())})
    out = hydrate_connections(data, {"prod-pg": _conn_def("prod-pg")})
    assert out["spec"]["conn"]["spec"]["label"] == "prod-pg"


def test_hydrate_connections_raises_on_unknown_name():
    data = _wrap({"conn": _conn_ref("ghost")})
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_connections(data, {"prod-pg": _conn_def("prod-pg")})
    assert exc_info.value.api_error is CommonErrorCode.COMMON_8


def test_hydrate_connections_raises_when_root_is_ref():
    # The walker reports the root at ``$`` and `replace_at` refuses to
    # rewrite the root. ``hydrate_connections`` surfaces COMMON_5 rather
    # than silently no-op'ing.
    data = _conn_ref("x")
    with pytest.raises(CommonRuntimeException) as exc_info:
        hydrate_connections(data, {"x": _conn_def("x")})
    assert exc_info.value.api_error is CommonErrorCode.COMMON_5


def test_hydrate_connections_leaves_connection_defs_alone():
    data = _wrap({"inline": _conn_def("already-here"), "ref": _conn_ref("x")})
    out = hydrate_connections(data, {"x": _conn_def("x")})
    # The inline def is unchanged; only the ref is rewritten.
    assert out["spec"]["inline"]["spec"]["label"] == "already-here"
    assert out["spec"]["ref"]["spec"]["label"] == "x"


# --- round-trip observed through `Secret.value()` --------------------------


def test_dehydrate_then_hydrate_restores_field_value():
    # End-to-end check: a `Secret` field reconstructed from the hydrated
    # tree returns the original value, while the same field reconstructed
    # from the *dehydrated* tree raises -- there's no resolver wired up,
    # which is the whole point of dehydration.
    original = _wrap(
        {
            "password": _secret_literal("hunter2"),
            "token": _secret_literal("abc"),
        }
    )
    secrets = extract_secrets(original)
    dehydrated = dehydrate_secrets(original)

    # Dehydrated: every field is a bare `Secret` ref; ``.value()`` blows
    # up because there's no secret-store resolver to call.
    dpw = Secret.from_dict(dehydrated["spec"]["password"])
    dtk = Secret.from_dict(dehydrated["spec"]["token"])
    with pytest.raises(ApiInternalException):
        dpw.value()
    with pytest.raises(ApiInternalException):
        dtk.value()

    # Hydrated: every field is a `_LiteralSecret`, so ``.value()`` returns
    # the inlined value the literal carries.
    rehydrated = hydrate_secrets(dehydrated, secrets)
    pw = Secret.from_dict(rehydrated["spec"]["password"])
    tk = Secret.from_dict(rehydrated["spec"]["token"])
    assert pw.value() == "hunter2"
    assert tk.value() == "abc"
