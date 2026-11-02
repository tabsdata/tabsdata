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
    assert materialize(document) == {"ai_providers": {"openai": {"api_key": "sk-live"}}}


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


def test_materialize_keys_list_items_by_their_first_value(monkeypatch):
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__ROOT__URI", "s3://root")
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__IMAGES__URI", "s3://images")
    document = {
        "storage": {
            "mounts": [
                {"id": "ROOT", "uri": "${{secret}}"},
                {"id": "IMAGES", "uri": "${{secret}}"},
            ],
        },
    }
    assert materialize(document) == {
        "storage": {
            "mounts": [
                {"id": "ROOT", "uri": "s3://root"},
                {"id": "IMAGES", "uri": "s3://images"},
            ],
        },
    }


def test_materialize_rejects_a_list_item_keyed_by_a_secret():
    document = {"mounts": [{"id": "${{secret}}", "uri": "s3://root"}]}
    with pytest.raises(MaterializerSecretException, match="keyed by a secret"):
        materialize(document)


@pytest.mark.parametrize(
    "placeholder",
    ["${{secret}}", "${{secret?}}", "${{secret:name}}", "${{secretAuto:password}}"],
)
def test_materialize_accepts_every_placeholder_form(monkeypatch, placeholder):
    monkeypatch.setenv("SECRETS__API_KEY", "value")
    assert materialize({"api_key": placeholder}) == {"api_key": "value"}


def test_materialize_leaves_embedded_placeholders_as_text():
    document = {"api_key": "prefix ${{secret}}"}
    assert materialize(document) == document


def test_materialize_trims_resolved_value(monkeypatch):
    monkeypatch.setenv("SECRETS__API_KEY", "  padded  ")
    document = {"api_key": "${{secret:k}}"}
    assert materialize(document) == {"api_key": "padded"}


_OPENAI_KEY = {"ai_providers": {"openai": {"api_key": "${{secret:openai-key}}"}}}


def test_materialize_reads_the_secret_file_first(monkeypatch, tmp_path):
    (tmp_path / "AI_PROVIDERS__OPENAI__API_KEY").write_text("sk-file\n")
    monkeypatch.setenv("TD_SECRETS_DIR", str(tmp_path))
    monkeypatch.setenv("SECRETS__AI_PROVIDERS__OPENAI__API_KEY", "sk-env")
    assert materialize(_OPENAI_KEY) == {
        "ai_providers": {"openai": {"api_key": "sk-file"}}
    }


def test_materialize_falls_back_to_env_without_the_file(monkeypatch, tmp_path):
    monkeypatch.setenv("TD_SECRETS_DIR", str(tmp_path))
    monkeypatch.setenv("SECRETS__AI_PROVIDERS__OPENAI__API_KEY", "sk-env")
    assert materialize(_OPENAI_KEY) == {
        "ai_providers": {"openai": {"api_key": "sk-env"}}
    }


def test_materialize_missing_file_and_env_still_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("TD_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("SECRETS__AI_PROVIDERS__OPENAI__API_KEY", raising=False)
    with pytest.raises(MaterializerSecretException):
        materialize(_OPENAI_KEY)


def test_a_list_of_mappings_is_addressed_by_its_first_value(monkeypatch):
    monkeypatch.setenv("SECRETS__PROVIDERS__OPENAI__API_KEY", "sk-live")
    monkeypatch.delenv("SECRETS__PROVIDERS__ANTHROPIC__API_KEY", raising=False)
    config = {
        "providers": [
            {"name": "anthropic", "api_key": "${{secret:anthropic-key?}}"},
            {"name": "openai", "api_key": "${{secret:openai-key?}}"},
        ]
    }
    assert materialize(config) == {
        "providers": [
            {"name": "anthropic", "api_key": ""},
            {"name": "openai", "api_key": "sk-live"},
        ]
    }


def test_a_list_that_cannot_be_keyed_is_addressed_by_index(monkeypatch):
    monkeypatch.setenv("SECRETS__ITEMS___1", "second")
    assert materialize({"items": ["plain", "${{secret:x}}"]}) == {
        "items": ["plain", "second"]
    }
