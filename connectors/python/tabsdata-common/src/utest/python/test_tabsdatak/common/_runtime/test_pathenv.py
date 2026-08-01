#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os

from tabsdatak.common._runtime.pathenv import (
    PATH,
    TD_AFTER_PATH,
    TD_BEFORE_PATH,
    extend_path,
    extended_path,
)


def joined(*entries: str) -> str:
    return os.pathsep.join(entries)


def test_before_is_prepended_and_after_is_appended():
    assert extended_path(joined("/usr/bin", "/bin"), "/venv/bin", "/opt/bin") == joined(
        "/venv/bin", "/usr/bin", "/bin", "/opt/bin"
    )


def test_empty_after_leaves_only_a_prefix():
    assert extended_path(joined("/usr/bin", "/bin"), "/venv/bin", "") == joined(
        "/venv/bin", "/usr/bin", "/bin"
    )


def test_absent_variables_leave_the_path_untouched():
    assert extended_path(joined("/usr/bin"), None, None) is None


def test_empty_variables_leave_the_path_untouched():
    assert extended_path(joined("/usr/bin"), "", "") is None


def test_extending_twice_is_idempotent():
    once = extended_path(joined("/usr/bin", "/bin"), "/venv/bin", "/opt/bin")
    assert extended_path(once, "/venv/bin", "/opt/bin") == once


def test_a_folder_already_present_is_not_duplicated():
    assert extended_path(joined("/venv/bin", "/usr/bin"), "/venv/bin", "") == joined(
        "/venv/bin", "/usr/bin"
    )


def test_an_absent_path_is_built_from_the_variables():
    assert extended_path(None, "/venv/bin", "/opt/bin") == joined(
        "/venv/bin", "/opt/bin"
    )


def test_empty_entries_in_the_path_are_dropped():
    assert extended_path(joined("/usr/bin", "", "/bin"), "/venv/bin", "") == joined(
        "/venv/bin", "/usr/bin", "/bin"
    )


def test_several_folders_keep_their_declared_order():
    assert extended_path(
        joined("/usr/bin"),
        joined("/venv/bin", "/venv/extra"),
        joined("/opt/one", "/opt/two"),
    ) == joined("/venv/bin", "/venv/extra", "/usr/bin", "/opt/one", "/opt/two")


def test_extend_path_rewrites_the_environment(monkeypatch):
    monkeypatch.setenv(PATH, joined("/usr/bin", "/bin"))
    monkeypatch.setenv(TD_BEFORE_PATH, "/venv/bin")
    monkeypatch.setenv(TD_AFTER_PATH, "")
    extend_path()
    assert os.environ[PATH] == joined("/venv/bin", "/usr/bin", "/bin")


def test_extend_path_without_variables_keeps_the_environment(monkeypatch):
    monkeypatch.setenv(PATH, joined("/usr/bin", "/bin"))
    monkeypatch.delenv(TD_BEFORE_PATH, raising=False)
    monkeypatch.delenv(TD_AFTER_PATH, raising=False)
    extend_path()
    assert os.environ[PATH] == joined("/usr/bin", "/bin")
