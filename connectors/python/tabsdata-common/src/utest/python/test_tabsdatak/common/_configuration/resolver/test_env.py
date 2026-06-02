#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.common._configuration.resolver.env import resolve
from tabsdatak.common._configuration.resolver.error import ResolverEnvException


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
    with pytest.raises(ResolverEnvException, match="TDK_TEST_RESOLVER_MISSING"):
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


def test_adjacent_placeholders(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_ADJ_A", "x")
    monkeypatch.setenv("TDK_TEST_RESOLVER_ADJ_B", "y")
    assert (
        resolve("${env:TDK_TEST_RESOLVER_ADJ_A}${env:TDK_TEST_RESOLVER_ADJ_B}")
        == "xy"
    )


def test_placeholder_inside_word(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_INSIDE", "MID")
    assert (
        resolve("foo${env:TDK_TEST_RESOLVER_INSIDE}baz") == "fooMIDbaz"
    )


def test_placeholder_starting_with_underscore(monkeypatch):
    monkeypatch.setenv("_TDK_TEST_RESOLVER_HIDDEN", "ok")
    assert resolve("${env:_TDK_TEST_RESOLVER_HIDDEN}") == "ok"


def test_empty_value(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_EMPTY", "")
    assert resolve("a=${env:TDK_TEST_RESOLVER_EMPTY}b") == "a=b"


def test_multiline_content(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_HOST", "example.com")
    monkeypatch.setenv("TDK_TEST_RESOLVER_PORT", "8080")
    text = (
        "host: ${env:TDK_TEST_RESOLVER_HOST}\n"
        "port: ${env:TDK_TEST_RESOLVER_PORT}"
    )
    assert resolve(text) == "host: example.com\nport: 8080"


def test_value_with_embedded_newline_inserted_verbatim(monkeypatch):
    monkeypatch.setenv("TDK_TEST_RESOLVER_MULTI", "line1\nline2")
    assert (
        resolve("x=${env:TDK_TEST_RESOLVER_MULTI};")
        == "x=line1\nline2;"
    )


def test_resolution_is_not_recursive(monkeypatch):
    """A resolved value that looks like a placeholder is not re-expanded."""
    monkeypatch.setenv("TDK_TEST_RESOLVER_OUTER", "${env:TDK_TEST_RESOLVER_INNER}")
    monkeypatch.setenv("TDK_TEST_RESOLVER_INNER", "should-not-appear")
    assert (
        resolve("${env:TDK_TEST_RESOLVER_OUTER}")
        == "${env:TDK_TEST_RESOLVER_INNER}"
    )


@pytest.mark.parametrize(
    "literal",
    [
        "${env:}",  # empty var name
        "${env:FOO",  # missing closing brace
        "${ENV:FOO}",  # wrong prefix casing
        "$env:FOO}",  # missing opening brace
        "${env: FOO}",  # leading space in name
        "${env:FOO }",  # trailing space in name
        "${env:1FOO}",  # starts with digit
        "${env:FOO-BAR}",  # hyphen not allowed in POSIX env names
        "${env:FOO.BAR}",  # dot not allowed either
        "$FOO",  # plain shell-style ref is not our grammar
    ],
)
def test_invalid_placeholder_shapes_left_untouched(literal, monkeypatch):
    monkeypatch.setenv("FOO", "X")
    monkeypatch.setenv("BAR", "Y")
    assert resolve(literal) == literal.strip()
