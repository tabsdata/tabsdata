#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.common._configuration.resolver.env import resolve


def test_resolves_unsubstituted_text_unchanged():
    assert resolve("plain content with no placeholders") == (
        "plain content with no placeholders"
    )


def test_resolves_required_env_placeholder(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_REQUIRED", "world")
    assert (
        resolve("hello ${env:TDK_TEST_RESOLVER_REQUIRED}") == "hello world"
    )


def test_missing_required_env_raises(monkeypatch):
    monkeypatch.delenv("TDK_TEST_RESOLVER_MISSING", raising=False)
    with pytest.raises(ValueError, match="TDK_TEST_RESOLVER_MISSING"):
        resolve("${env:TDK_TEST_RESOLVER_MISSING}")


def test_optional_env_placeholder_falls_back_to_empty_string(monkeypatch):
    monkeypatch.delenv("TDK_TEST_RESOLVER_OPTIONAL", raising=False)
    assert (
        resolve("prefix-${env:TDK_TEST_RESOLVER_OPTIONAL?}-suffix")
        == "prefix--suffix"
    )


def test_optional_env_placeholder_uses_value_when_set(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_OPTIONAL_SET", "value")
    assert (
        resolve("prefix-${env:TDK_TEST_RESOLVER_OPTIONAL_SET?}-suffix")
        == "prefix-value-suffix"
    )


def test_multiple_env_placeholders_resolve_left_to_right(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_A", "alpha")
    monkeypatch.setenv("TDK_TEST_RESOLVER_B", "beta")
    assert (
        resolve("${env:TDK_TEST_RESOLVER_A}/${env:TDK_TEST_RESOLVER_B}")
        == "alpha/beta"
    )


def test_resolved_value_is_trimmed(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_PADDED", "  padded  ")
    assert resolve("${env:TDK_TEST_RESOLVER_PADDED}") == "padded"


def test_final_result_is_trimmed(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_TRIM", "core")
    assert resolve("   ${env:TDK_TEST_RESOLVER_TRIM}   ") == "core"


def test_repeated_placeholder_resolves_all_occurrences(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_REPEAT", "x")
    assert (
        resolve(
            "${env:TDK_TEST_RESOLVER_REPEAT}-${env:TDK_TEST_RESOLVER_REPEAT}"
        )
        == "x-x"
    )
