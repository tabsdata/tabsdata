#
# Copyright 2026 Tabsdata Inc.
#

"""Redpanda's domain contract: brokers + a topic, and no URI.

A kafka domain is not an addressable endpoint, so — unlike mongodb, which ships
a real ``mongodb://`` uri — its descriptor stores no URI at all. The handle the
generic data-plane commands pass around is composed on demand instead.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2 import systems

SYS_META = {
    "HOST": "localhost",
    "PORT": "9092",
    "IMAGE": "redpandadata/redpanda:v24.3.5",
    "SCHEMA_REGISTRY_URL": "http://localhost:8081",
}


def test_domain_meta_has_no_uri():
    meta = systems.get("redpanda").domain_meta(SYS_META, "shop", "")
    assert "URI" not in meta
    assert meta["KAFKA_BROKERS"] == "localhost:9092" and meta["DB"] == "shop"


def test_mongodb_domain_meta_still_has_one():
    meta = systems.get("mongodb").domain_meta(SYS_META, "shop", "pw")
    assert meta["URI"] == "mongodb://localhost:9092/shop"


def test_data_plane_handle_is_composed_and_round_trips():
    backend = systems.get("redpanda")
    meta = backend.domain_meta(SYS_META, "shop", "")
    handle = backend.data_plane_uri(meta)
    assert backend._brokers_prefix(handle) == ("localhost:9092", "shop")
