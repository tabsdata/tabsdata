#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdatax.cmake.kupgrader.common.versions import Entry, VersionError, resolve

ENTRIES = [
    Entry(version="2.2.0", upgrader=True),
    Entry(version="2.1.0", upgrader=True),
]


def test_resolve_current_is_newest_entry():
    resolution = resolve("2.2.0", ENTRIES)
    assert resolution.previous == "2.1.0"


def test_resolve_current_is_newer_than_inventory():
    resolution = resolve("2.3.0", ENTRIES)
    assert resolution.previous == "2.2.0"


def test_resolve_only_entry_is_exhausted():
    resolution = resolve("2.2.0", ENTRIES[:1])
    assert resolution.exhausted


def test_resolve_rejects_current_older_than_newest_entry():
    with pytest.raises(VersionError, match="'2.1.5' is older than .*'2.2.0'"):
        resolve("2.1.5", ENTRIES)


def test_resolve_rejects_invalid_version():
    with pytest.raises(VersionError, match="'not-a-version' is not a valid version"):
        resolve("not-a-version", ENTRIES)
