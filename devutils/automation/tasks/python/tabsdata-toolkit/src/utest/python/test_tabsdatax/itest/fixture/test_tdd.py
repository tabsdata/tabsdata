#
# Copyright 2026 Tabsdata Inc.
#

"""The fixtures' tdd client, and how a store tdd cannot list reaches the tests:
only those that declare one of its backends fail, with tdd's own message. A
backend the store did not list has no fixture, so the skip and missing policies
apply before setup."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tabsdatax.itest.fixture import _tdd, backend as common
from tabsdatax.itest.fixture.backends.fs import FsDomain, FsSystem


def test_a_missing_tdd_says_how_to_install_it(tmp_path, monkeypatch):
    monkeypatch.setenv(_tdd.ENV_TDD_DIR, str(tmp_path))
    with pytest.raises(_tdd.TddError, match="cargo make permit"):
        _tdd.names(_tdd.SYSTEM)


def test_a_store_without_its_key_fails_with_tdds_message(tmp_path, monkeypatch):
    monkeypatch.setenv(_tdd.ENV_STORE_DIR[_tdd.SYSTEM], str(tmp_path))
    monkeypatch.delenv("_TDD_CRYPTO_KEY", raising=False)
    with pytest.raises(_tdd.TddError, match="_TDD_CRYPTO_KEY"):
        _tdd.names(_tdd.SYSTEM)


class _Item:
    def __init__(self, *marks, fixtures=("system_my_fs", "domain_inbox")):
        self._marks = marks
        self._fixtureinfo = SimpleNamespace(
            name2fixturedefs={fixture: () for fixture in fixtures}
        )

    def iter_markers(self, name):
        return iter(self._marks if name == common.MARKER else ())


def mark(name: str, cls: type):
    return getattr(pytest.mark, common.MARKER)(name, cls).mark


def test_an_unlisted_store_fails_only_the_tests_that_declare_its_backends(
    monkeypatch,
):
    monkeypatch.setitem(common.LISTING_ERRORS, common.SYSTEM, _tdd.TddError("boom"))
    with pytest.raises(pytest.fail.Exception, match="could not be listed: boom"):
        common.pytest_runtest_setup(_Item(mark("my_fs", FsSystem)))
    assert common.pytest_runtest_setup(_Item(mark("inbox", FsDomain))) is None
    assert common.pytest_runtest_setup(_Item()) is None


@pytest.fixture
def no_policy(monkeypatch):
    for var in (
        *common.ON_MISSING_ENV.values(),
        *common.SKIP_ENV.values(),
        *common.MANIFEST_ENV.values(),
    ):
        monkeypatch.delenv(var, raising=False)


def test_a_backend_without_fixture_the_manifest_does_not_offer_skips(
    no_policy, monkeypatch, tmp_path
):
    manifest = tmp_path / "domains.yaml"
    manifest.write_text("- name: inbox\n  sys: my_fs\n")
    monkeypatch.setenv(common.MANIFEST_ENV[common.DOMAIN], str(manifest))
    with pytest.raises(pytest.skip.Exception, match="does not offer"):
        common.pytest_runtest_setup(_Item(mark("unlisted", FsDomain)))


def test_a_backend_without_fixture_follows_the_missing_policy(no_policy, monkeypatch):
    item = _Item(mark("absent", FsDomain))
    with pytest.raises(pytest.fail.Exception, match="not in the tdd store"):
        common.pytest_runtest_setup(item)
    monkeypatch.setenv(common.ON_MISSING_ENV[common.DOMAIN], "skip")
    with pytest.raises(pytest.skip.Exception, match="not in the tdd store"):
        common.pytest_runtest_setup(item)


def test_a_backend_with_fixture_is_left_to_it(no_policy, monkeypatch, tmp_path):
    manifest = tmp_path / "domains.yaml"
    manifest.write_text("- name: other\n  sys: my_fs\n")
    monkeypatch.setenv(common.MANIFEST_ENV[common.DOMAIN], str(manifest))
    assert common.pytest_runtest_setup(_Item(mark("inbox", FsDomain))) is None
