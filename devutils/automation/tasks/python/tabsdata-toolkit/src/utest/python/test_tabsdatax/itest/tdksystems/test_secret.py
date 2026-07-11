"""Unit tests for the pluggable secret registry (`tabsdatax.itest.tdksystems.secret`):
`--set <PROVIDER>:<REF>` parsing, provider dispatch, and `--out` rendering."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import secret
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- parse_ref ------------------------------------------------------------

@pytest.mark.parametrize("get,scheme,ref", [
    ("1p:dev_xp",            "1p", "dev_xp"),
    ("1p:my vault",          "1p", "my vault"),
    ("hashicorp:secret/foo", "hashicorp", "secret/foo"),
    (" 1p : dev_xp ",        "1p", "dev_xp"),
])
def test_parse_ref_ok(get, scheme, ref):
    assert secret.parse_ref(get) == (scheme, ref)


def test_parse_ref_keeps_only_first_colon_as_separator():
    # A ref may itself contain ':' (e.g. a path) — only the first splits.
    assert secret.parse_ref("prov:a:b:c") == ("prov", "a:b:c")


@pytest.mark.parametrize("bad", ["", "1p", "1p:", ":dev_xp", "  :  "])
def test_parse_ref_rejects_malformed(bad):
    with pytest.raises(DatagenError, match="PROVIDER"):
        secret.parse_ref(bad)


# ---- provider dispatch ----------------------------------------------------

def test_1p_is_registered():
    assert secret._PROVIDERS.get("1p") == "secret_1p"


def test_fetch_unknown_provider_errors():
    with pytest.raises(DatagenError, match="unknown secret provider 'nope'"):
        secret.fetch("nope:whatever")


def test_fetch_dispatches_to_provider(monkeypatch):
    import tabsdatax.itest.tdksystems.secret_1p as sp
    monkeypatch.setattr(sp, "fetch", lambda ref: {"REF": ref})
    assert secret.fetch("1p:dev_xp") == {"REF": "dev_xp"}


# ---- render ---------------------------------------------------------------

_SECRETS = {"SNOWFLAKE__ACCOUNT": "acct", "SNOWFLAKE__PAT": "s p a c e"}


def test_render_json_is_a_dict():
    assert json.loads(secret.render(_SECRETS, "json")) == _SECRETS


def test_render_export_quotes_values():
    lines = secret.render(_SECRETS, "export").splitlines()
    assert lines[0] == "export SNOWFLAKE__ACCOUNT=acct"
    # shlex.quote wraps the value that has a space.
    assert lines[1] == "export SNOWFLAKE__PAT='s p a c e'"


def test_render_unset_lists_names_only():
    assert secret.render(_SECRETS, "unset").splitlines() == [
        "unset SNOWFLAKE__ACCOUNT",
        "unset SNOWFLAKE__PAT",
    ]


def test_render_empty():
    assert secret.render({}, "json") == "{}"
    assert secret.render({}, "export") == ""
    assert secret.render({}, "unset") == ""


def test_render_unknown_format_errors():
    with pytest.raises(DatagenError, match="unknown --out format"):
        secret.render(_SECRETS, "yaml")
