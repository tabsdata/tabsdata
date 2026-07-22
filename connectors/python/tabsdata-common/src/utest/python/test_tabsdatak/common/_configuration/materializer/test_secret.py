#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.common._configuration.materializer.error import (
    MaterializerSecretException,
)
from tabsdatak.common._configuration.materializer.secret import (
    encode_secret_variable,
    materialize,
)


def test_encode_uses_prefix_and_path():
    assert encode_secret_variable(["ai_providers", "openai", "api_key"]) == (
        "SECRETS__AI_PROVIDERS__OPENAI__API_KEY"
    )


def test_encode_uppercases_and_replaces_non_alphanumeric():
    assert encode_secret_variable(["my-key.name"]) == "SECRETS__MY_KEY_NAME"


def test_encode_prefixes_leading_digit_segment():
    assert encode_secret_variable(["9lives"]) == "SECRETS___9LIVES"


def test_encode_empty_segment_raises():
    with pytest.raises(MaterializerSecretException, match="empty segment"):
        encode_secret_variable(["a", ""])


def test_materialize_resolves_required_secret(monkeypatch):
    monkeypatch.setenv("SECRETS__AI_PROVIDERS__OPENAI__API_KEY", "sk-live")
    document = {"ai_providers": {"openai": {"api_key": "${{secret:openai-key}}"}}}
    assert materialize(document) == {
        "ai_providers": {"openai": {"api_key": "sk-live"}}
    }


def test_materialize_missing_required_secret_raises(monkeypatch):
    monkeypatch.delenv("SECRETS__AI_PROVIDERS__OPENAI__API_KEY", raising=False)
    document = {"ai_providers": {"openai": {"api_key": "${{secret:openai-key}}"}}}
    with pytest.raises(
        MaterializerSecretException,
        match="SECRETS__AI_PROVIDERS__OPENAI__API_KEY",
    ):
        materialize(document)


def test_materialize_optional_missing_secret_is_empty(monkeypatch):
    monkeypatch.delenv("SECRETS__AI_PROVIDERS__OPENAI__API_KEY", raising=False)
    document = {"ai_providers": {"openai": {"api_key": "${{secret:openai-key?}}"}}}
    assert materialize(document) == {"ai_providers": {"openai": {"api_key": ""}}}


def test_materialize_optional_present_secret_uses_value(monkeypatch):
    monkeypatch.setenv("SECRETS__AI_PROVIDERS__ANTHROPIC__API_KEY", "ak-live")
    document = {
        "ai_providers": {"anthropic": {"api_key": "${{secret:anthropic-key?}}"}}
    }
    assert materialize(document) == {
        "ai_providers": {"anthropic": {"api_key": "ak-live"}}
    }


def test_materialize_leaves_non_secret_scalars_unchanged():
    document = {"model": "openai/gpt-5.4", "allow_writes": True, "cache_dir": None}
    assert materialize(document) == document


def test_materialize_resolves_secret_inside_list(monkeypatch):
    monkeypatch.setenv("SECRETS__PROVIDERS___0__KEY", "first")
    document = {"providers": [{"key": "${{secret:k}}"}]}
    assert materialize(document) == {"providers": [{"key": "first"}]}


def test_materialize_trims_resolved_value(monkeypatch):
    monkeypatch.setenv("SECRETS__API_KEY", "  padded  ")
    document = {"api_key": "${{secret:k}}"}
    assert materialize(document) == {"api_key": "padded"}
