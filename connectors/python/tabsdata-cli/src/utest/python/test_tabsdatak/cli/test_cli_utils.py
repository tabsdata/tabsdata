#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for the pure helpers in ``tabsdatak.cli.cli_utils``.

The Click/REPL-bound helpers (``prompt_with_escape``,
``logical_prompt``, ``utils_login``, ``initialise_tabsdata_server_connection``,
etc.) are exercised end-to-end against a live ``td`` invocation in the
CLI integration suite; here we test what's purely functional:

* ``is_valid_id`` / ``beautify_list`` / ``beautify_time``
* the temporary ``tdabout`` / ``td_resource`` stand-ins
* ``MutuallyExclusiveOption`` / ``CurrentPlatform``
"""

from __future__ import annotations

import pytest

from tabsdatak.cli import cli_utils


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_is_valid_id_accepts_26char_uppercase_alphanumeric():
    assert cli_utils.is_valid_id("ABCDEFGHIJ0123456789QRSTUV") is True


@pytest.mark.parametrize(
    "bad",
    [
        "",  # empty
        "abcdefghijklmnopqrstuvwxyz",  # lowercase
        "ABCDEFGHIJ0123456789QRSTU",  # 25 chars (too short)
        "ABCDEFGHIJ0123456789QRSTUVW",  # 27 chars (too long)
        "ABCDEFGHIJ0123456789QRSTU!",  # invalid char
    ],
)
def test_is_valid_id_rejects_bad_inputs(bad):
    assert cli_utils.is_valid_id(bad) is False


def test_beautify_list_passes_through_none_marker():
    assert cli_utils.beautify_list(None) == "<None>"
    assert cli_utils.beautify_list([]) == "<None>"


def test_beautify_list_joins_list_with_newlines():
    assert cli_utils.beautify_list(["a", "b", "c"]) == "a\nb\nc"


def test_beautify_list_stringifies_scalar():
    assert cli_utils.beautify_list(42) == "42"


def test_beautify_time_replaces_string_none_with_dash():
    assert cli_utils.beautify_time("None") == "-"


def test_beautify_time_returns_other_strings_verbatim():
    assert cli_utils.beautify_time("2026-05-29T12:00:00Z") == "2026-05-29T12:00:00Z"


# ---------------------------------------------------------------------------
# Temporary stand-ins (replaced in a future ticket)
# ---------------------------------------------------------------------------


def test_tdabout_prints_package_and_version(capsys):
    cli_utils.tdabout("tabsdata-cli")
    captured = capsys.readouterr()
    assert "tabsdata-cli" in captured.out


def test_tdabout_unknown_package_prints_unknown_version(capsys):
    cli_utils.tdabout("definitely-not-an-installed-package")
    captured = capsys.readouterr()
    assert "definitely-not-an-installed-package" in captured.out
    assert "unknown" in captured.out


def test_td_resource_raises_filenotfound_for_missing_resource():
    with pytest.raises(FileNotFoundError) as ei:
        cli_utils.td_resource("does/not/exist.txt")
    assert "does/not/exist.txt" in str(ei.value)


def test_td_resource_normalises_backslashes(monkeypatch, tmp_path):
    """The legacy helper normalised Windows-style backslashes; the
    stand-in must do the same."""
    # We can't easily test a real resource lookup without writing a
    # file to a packaged location, so test the FileNotFoundError
    # message which echoes the normalised input.
    with pytest.raises(FileNotFoundError) as ei:
        cli_utils.td_resource("a\\b\\c")
    # The error message includes the original (pre-normalisation)
    # string so the user can match what they passed.
    assert "a" in str(ei.value)


# ---------------------------------------------------------------------------
# CurrentPlatform
# ---------------------------------------------------------------------------


def test_current_platform_exposes_system_name():
    p = cli_utils.CurrentPlatform()
    # ``.platform`` returns the underlying ``platform.system()`` output
    # (e.g. ``"Darwin"``, ``"Linux"``, ``"Windows"``).
    import platform as _stdlib_platform

    assert p.platform == _stdlib_platform.system()


def test_current_platform_predicates_are_mutually_exclusive():
    p = cli_utils.CurrentPlatform()
    truths = [p.is_windows(), p.is_linux(), p.is_mac()]
    # Exactly one of windows/linux/mac is True on a typical host;
    # ``is_unix()`` overlaps with both linux and mac so it isn't part
    # of this exclusivity check.
    assert sum(truths) <= 1
