#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the lightweight bits of ``tabsdatak.common._compatibility``.

The ``__main__`` diagnostic flow (which runs a polars import + CPU
probe and prints to stderr) is a side-effect end-to-end harness; we
exercise the building blocks directly here."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError

import pytest

# noinspection PyProtectedMember
from tabsdatak.common._compatibility import (
    CompatibilityError,
    PackageVersionError,
    check_sticky_version_package,
    check_sticky_version_packages,
)


def test_compatibility_error_accepts_string_and_list():
    e1 = CompatibilityError("solo")
    e2 = CompatibilityError(["one", "two"])
    assert e1.messages == ["solo"]
    assert e2.messages == ["one", "two"]
    assert "one\ntwo" in str(e2)


def test_package_version_error_same_shape():
    e = PackageVersionError("nope")
    assert e.messages == ["nope"]


def test_check_sticky_version_package_noop_when_requires_is_falsy():
    # Empty requires list -> early return, no exception.
    check_sticky_version_package("polars", [])
    check_sticky_version_package("polars", None)


def test_check_sticky_version_package_noop_when_package_not_referenced():
    # Requires list mentions other packages, but not "polars".
    check_sticky_version_package("polars", ["pyarrow==1.0.0", "numpy>=1.0"])


def test_check_sticky_version_package_raises_when_missing(monkeypatch):
    def _raise_not_found(_pkg):
        raise PackageNotFoundError("polars")

    monkeypatch.setattr(
        "tabsdatak.common._compatibility.version",
        _raise_not_found,
    )
    with pytest.raises(PackageVersionError) as ei:
        check_sticky_version_package("polars", ["polars==1.2.3"])
    assert "not installed" in str(ei.value)
    assert "polars==1.2.3" in str(ei.value)


def test_check_sticky_version_package_raises_on_mismatch(monkeypatch):
    monkeypatch.setattr(
        "tabsdatak.common._compatibility.version",
        lambda _pkg: "0.9.0",
    )
    with pytest.raises(PackageVersionError) as ei:
        check_sticky_version_package("polars", ["polars==1.2.3"])
    msg = str(ei.value)
    assert "version mismatch" in msg
    assert "1.2.3" in msg
    assert "0.9.0" in msg


def test_check_sticky_version_package_silent_on_match(monkeypatch):
    monkeypatch.setattr(
        "tabsdatak.common._compatibility.version",
        lambda _pkg: "1.2.3",
    )
    # No exception expected.
    check_sticky_version_package("polars", ["polars==1.2.3"])


def test_check_sticky_version_packages_swallows_unexpected_errors(monkeypatch):
    """Top-level helper only re-raises PackageVersionError; other
    exceptions are swallowed (best-effort behaviour matches the
    legacy callers)."""

    def _explode(_pkg, _reqs):
        raise RuntimeError("ugh")

    monkeypatch.setattr(
        "tabsdatak.common._compatibility.requires", lambda _pkg: ["polars==1.2.3"]
    )
    monkeypatch.setattr(
        "tabsdatak.common._compatibility.check_sticky_version_package", _explode
    )
    # No raise.
    check_sticky_version_packages()
