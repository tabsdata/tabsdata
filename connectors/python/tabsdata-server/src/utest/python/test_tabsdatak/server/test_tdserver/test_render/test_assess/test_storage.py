#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess.checks.storage import (
    Mount,
    overlapping,
    settled,
)

pytestmark = pytest.mark.unit


def mount(identifier: str, uri: str) -> Mount:
    return Mount(identifier=identifier, uri=uri, options={})


def test_mounts_in_separate_prefixes_do_not_overlap():
    assert not overlapping([
        mount("ROOT", "s3://one/root/"),
        mount("BUNDLES", "s3://one/bundles/"),
        mount("IMAGES", "s3://one/images/"),
    ])


def test_mounts_in_separate_buckets_do_not_overlap():
    assert not overlapping([
        mount("ROOT", "s3://one/data/"),
        mount("BUNDLES", "s3://two/data/"),
    ])


def test_two_mounts_on_one_prefix_overlap():
    found = overlapping([
        mount("ROOT", "s3://one/data/"),
        mount("BUNDLES", "s3://one/data/"),
    ])
    assert len(found) == 1
    assert "ROOT" in found[0] and "BUNDLES" in found[0]


def test_a_mount_nested_in_another_overlaps():
    found = overlapping([
        mount("ROOT", "s3://one/data/"),
        mount("IMAGES", "s3://one/data/images/"),
    ])
    assert len(found) == 1
    assert "nest" in found[0]


def test_a_prefix_that_merely_shares_a_beginning_does_not_overlap():
    assert not overlapping([
        mount("ROOT", "s3://one/data/"),
        mount("BUNDLES", "s3://one/database/"),
    ])


def test_a_mount_without_a_uri_overlaps_with_nothing():
    assert not overlapping([mount("ROOT", ""), mount("BUNDLES", "")])


def test_an_unfilled_placeholder_counts_as_no_value():
    assert settled("${{secret}}") == ""
    assert settled("${{literal:tdk_database_host}}") == ""


def test_a_supplied_value_is_kept():
    assert settled("s3://one/root/") == "s3://one/root/"
    assert settled(None) == ""
