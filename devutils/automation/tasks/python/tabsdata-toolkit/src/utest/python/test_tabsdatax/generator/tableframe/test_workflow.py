#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for manifest discovery by version and kind."""

import pytest

from tabsdatax.generator.tableframe.common.workflow import (
    greatest_tabsdata_manifest,
    previous_and_current_manifests,
)


def _touch(folder, *names):
    for name in names:
        (folder / name).write_text("{}")


def test_pairs_current_with_greatest_older_version(tmp_path):
    _touch(
        tmp_path,
        "manifest_1.9.0_pl.json",
        "manifest_1.39.3_pl.json",
        "manifest_1.44.2_pl.json",
        "manifest_1.39.3_td.json",
    )
    old, new = previous_and_current_manifests(tmp_path, "pl", "1.44.2")
    assert (old.name, new.name) == ("manifest_1.39.3_pl.json", "manifest_1.44.2_pl.json")


def test_ignores_newer_versions_and_other_kinds(tmp_path):
    _touch(
        tmp_path,
        "manifest_1.39.3_td.json",
        "manifest_1.44.2_td.json",
        "manifest_1.50.0_td.json",
        "manifest_1.44.2_pl.json",
    )
    old, new = previous_and_current_manifests(tmp_path, "td", "1.44.2")
    assert (old.name, new.name) == ("manifest_1.39.3_td.json", "manifest_1.44.2_td.json")


def test_fails_without_a_manifest_for_the_current_version(tmp_path):
    _touch(tmp_path, "manifest_1.39.3_pl.json")
    with pytest.raises(SystemExit):
        previous_and_current_manifests(tmp_path, "pl", "1.44.2")


def test_fails_without_an_older_manifest(tmp_path):
    _touch(tmp_path, "manifest_1.44.2_pl.json")
    with pytest.raises(SystemExit):
        previous_and_current_manifests(tmp_path, "pl", "1.44.2")


def test_greatest_tabsdata_manifest_orders_by_version(tmp_path):
    _touch(tmp_path, "manifest_1.9.0_td.json", "manifest_1.39.3_td.json")
    assert greatest_tabsdata_manifest(tmp_path).name == "manifest_1.39.3_td.json"
