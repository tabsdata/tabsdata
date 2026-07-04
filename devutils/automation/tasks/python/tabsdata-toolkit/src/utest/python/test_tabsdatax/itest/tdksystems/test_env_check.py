"""Tests for tabsdatax.itest.tdksystems.env_check — distribution-metadata parsing + check."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import env_check
from tabsdatax.itest.tdksystems.errors import DatagenError


def test_marker_ok_handles_known_machine_check():
    assert env_check._marker_ok('platform_machine != "aarch64"', "x86_64") is True
    assert env_check._marker_ok('platform_machine != "aarch64"', "aarch64") is False
    assert env_check._marker_ok('platform_machine == "aarch64"', "aarch64") is True


def test_marker_ok_unknown_marker_treated_as_applicable():
    # If we don't understand a marker, we err on the side of including the
    # requirement (otherwise we'd silently skip real deps).
    assert env_check._marker_ok('python_version >= "3.10"', "x86_64") is True


def test_import_name_overrides_known_aliases():
    assert env_check._import_name("psycopg2-binary") == "psycopg2"
    assert env_check._import_name("mysql-connector-python") == "mysql.connector"
    assert env_check._import_name("clickhouse-sqlalchemy") == "clickhouse_sqlalchemy"
    assert env_check._import_name("google-cloud-storage") == "google.cloud.storage"


def test_import_name_replaces_dashes():
    assert env_check._import_name("foo-bar-baz") == "foo_bar_baz"


def test_parse_requires_strips_versions_and_skips_extras():
    parsed = env_check._parse_requires([
        "sqlalchemy",
        "pandas==2.2.3",
        'ibm_db==3.2.8; platform_machine != "aarch64"',
        "boto3>=1.0",
        'pytest>=7; extra == "test"',                   # optional — skipped
        'cumulusci-plus; extra == "salesforce"',        # optional — skipped
        "",                                             # blank — skipped
    ])
    assert "sqlalchemy" in parsed
    assert "pandas" in parsed
    assert "boto3" in parsed
    assert "pytest" not in parsed
    assert "cumulusci-plus" not in parsed
    # ibm_db visibility depends on the host's machine — accept either
    # outcome but make sure if it's there, it's the bare name.
    if any("ibm" in p for p in parsed):
        assert "ibm_db" in parsed


def test_parse_requires_strips_trailing_extras_bracket():
    parsed = env_check._parse_requires(["foo[bar,baz]>=1"])
    assert parsed == ["foo"]


def test_check_requirements_silent_when_satisfied(monkeypatch):
    """When requires only references importable packages, we should not raise."""
    monkeypatch.setattr(env_check, "_requires_for", lambda _dist: ["sys"])
    env_check.check_requirements()  # 'sys' is always importable


def test_check_requirements_raises_when_packages_missing(monkeypatch):
    monkeypatch.setattr(
        env_check, "_requires_for",
        lambda _dist: ["definitely_not_a_real_package_12345"],
    )
    with pytest.raises(DatagenError, match="missing"):
        env_check.check_requirements()


def test_check_requirements_silent_when_distribution_not_installed(monkeypatch):
    """Running from a raw source checkout (no pip install) should be a
    silent no-op, not an error."""
    monkeypatch.setattr(env_check, "_requires_for", lambda _dist: None)
    env_check.check_requirements()  # should not raise
