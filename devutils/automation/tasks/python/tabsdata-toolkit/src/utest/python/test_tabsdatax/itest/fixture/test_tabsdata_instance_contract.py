#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest

from tabsdatax.itest.fixture.instances import tabsdata


def test_requested_consumers_reads_shorthands():
    names = ("td_tabsdata_instance", "ai_tabsdata_instance", "tmp_path")
    assert tabsdata.requested_consumers(names) == {"tdtest", "aitest"}


def test_requested_consumers_reads_prefixed_names():
    names = ("instance_tdtest", "instance_aitest", "request")
    assert tabsdata.requested_consumers(names) == {"tdtest", "aitest"}


def test_requested_consumers_ignores_unrelated_fixtures():
    assert tabsdata.requested_consumers(("tmp_path", "monkeypatch")) == set()


def test_shorthand_fails_when_consumer_undeclared(request):
    with pytest.raises(pytest.fail.Exception, match="'aitest' not declared"):
        tabsdata.enforced_instance(request, tabsdata.CONSUMER_AITEST)


def test_enforced_instance_loads_when_declared(request, monkeypatch):
    loaded = object()
    monkeypatch.setattr(
        tabsdata, "loaded_instance", lambda consumer, check: loaded
    )
    monkeypatch.setattr(
        tabsdata, "marker_consumers", lambda node: {tabsdata.CONSUMER_TDTEST: True}
    )
    assert tabsdata.enforced_instance(request, tabsdata.CONSUMER_TDTEST) is loaded


def call_meta_instance(request, monkeypatch, declared):
    loaded = {}

    def fake_loaded(consumer, check):
        instance = object()
        loaded[consumer] = instance
        return instance

    monkeypatch.setattr(tabsdata, "loaded_instance", fake_loaded)
    monkeypatch.setattr(tabsdata, "declared_consumers", lambda node: declared)
    result = tabsdata.meta_instance.__wrapped__(request)
    return result, loaded


def test_meta_instance_returns_sole_declared(request, monkeypatch):
    result, loaded = call_meta_instance(
        request, monkeypatch, {tabsdata.CONSUMER_AITEST: True}
    )
    assert result is loaded[tabsdata.CONSUMER_AITEST]


def test_meta_instance_fails_when_none_declared(request, monkeypatch):
    with pytest.raises(pytest.fail.Exception, match="exactly one declared"):
        call_meta_instance(request, monkeypatch, {})


def test_meta_instance_fails_when_multiple_declared(request, monkeypatch):
    with pytest.raises(pytest.fail.Exception, match="exactly one declared"):
        call_meta_instance(
            request,
            monkeypatch,
            {tabsdata.CONSUMER_TDTEST: True, tabsdata.CONSUMER_AITEST: True},
        )


HARNESS_PACKAGE = "tabsdatak/itest/fixture/instances"


def run_harness(pytester, name: str):
    copied = pytester.copy_example(f"{HARNESS_PACKAGE}/{name}")
    return pytester.runpytest("--collect-only", "-q", copied.name)


def violation_lines(result, needle: str) -> None:
    result.stderr.fnmatch_lines(["*contract violated*"])
    result.stderr.re_match_lines([rf".*{needle}.*"])


def test_collection_passes_when_declaration_matches_request(pytester):
    result = run_harness(pytester, "harness_ok.py")
    result.assert_outcomes()
    assert "contract violated" not in result.stdout.str()


def test_collection_fails_on_undeclared_request(pytester):
    result = run_harness(pytester, "harness_undeclared.py")
    violation_lines(
        result,
        rf"requests instance\(s\) \[{tabsdata.CONSUMER_AITEST!r}\]",
    )


def test_collection_fails_on_unused_declaration(pytester):
    result = run_harness(pytester, "harness_unused.py")
    violation_lines(
        result,
        rf"declares consumer\(s\) \[{tabsdata.CONSUMER_AITEST!r}\]",
    )


def test_collection_ignores_tests_without_markers_or_fixtures(pytester):
    result = run_harness(pytester, "harness_plain.py")
    result.assert_outcomes()
    assert "contract violated" not in result.stdout.str()


def test_collection_counts_transitive_fixture_requests(pytester):
    result = run_harness(pytester, "harness_transitive.py")
    violation_lines(result, rf"\[{tabsdata.CONSUMER_AITEST!r}\]")


def test_collection_requires_declared_consumer_in_signature(pytester):
    result = run_harness(pytester, "harness_unlisted.py")
    violation_lines(
        result,
        rf"declares consumer\(s\) \[{tabsdata.CONSUMER_TDTEST!r}\] that the test "
        rf"signature does not request",
    )


def test_collection_accepts_shorthand_and_named_for_same_consumer(pytester):
    result = run_harness(pytester, "harness_shorthand_and_named.py")
    result.assert_outcomes()
    assert "contract violated" not in result.stdout.str()


def test_collection_accepts_meta_instance_relay_via_helper(pytester):
    result = run_harness(pytester, "harness_meta_relay.py")
    result.assert_outcomes()
    assert "contract violated" not in result.stdout.str()


def test_collection_forbids_direct_meta_instance_request(pytester):
    result = run_harness(pytester, "harness_meta_direct.py")
    violation_lines(
        result,
        rf"requests `{tabsdata.INSTANCE_META_FIXTURE}` directly",
    )
