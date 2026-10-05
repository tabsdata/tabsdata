#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka stream-source-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.kafka.backend import broker
from testi_tabsdatak.conn.kafka.harness import (
    JSON_VARIANT,
    STREAM_CFG,
    KafkaCtx,
    Variant,
    committed_manifests,
    run_plugin,
    staged_at_least,
    staged_frames,
    staged_names,
    staged_rows,
    stream_src,
    user_messages,
    variant_params,
)

from tabsdatak.conn.kafka._serdes import JsonKafkaSerde
from tabsdatak.conn.kafka.constants import KAFKA_META_TOPIC

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.xdist_group("kafka_stream_plugin_integration_seq"),
]


@pytest.mark.parametrize("variant", variant_params())
@redpanda_domain("redpanda")
def test_stream_src_plugin_round_trips_each_serde_variant(
    variant: Variant, domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    messages = variant.messages(8)
    bus = broker(domain)
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(len(messages)))
    with bus.fresh_topic() as topic:
        bus.produce(
            topic, messages, variant.producer_serde(bus, work_dir(tmp_path, "proto"))
        )
        conn, src = stream_src(
            domain,
            topic=topic,
            variant=variant,
            src_cfg=STREAM_CFG | {"tabsdata.kafka.trigger.max_rows": len(messages)},
        )
        run_plugin(ctx, conn, src)

    frames = staged_frames(ctx)
    assert frames, f"no committed manifest files produced for {variant.id}"
    frame = pl.concat(frames)
    assert variant.columns <= set(frame.columns)
    assert staged_names(ctx) == sorted(message["name"] for message in messages)


@redpanda_domain("redpanda")
def test_stream_src_plugin_output_table_is_topic(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    messages = user_messages(3)
    bus = broker(domain)
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(len(messages)))
    with bus.fresh_topic() as topic:
        bus.produce(topic, messages, JSON_VARIANT.producer_serde(bus, tmp_path))
        conn, src = stream_src(
            domain,
            topic=topic,
            variant=JSON_VARIANT,
            src_cfg=STREAM_CFG | {"tabsdata.kafka.trigger.max_rows": len(messages)},
        )
        run_plugin(ctx, conn, src)
        assert src.output_table() == topic

    manifests = [staged.manifest for staged in committed_manifests(ctx)]
    assert manifests
    assert all(len(manifest) == 1 for manifest in manifests)
    assert staged_rows(ctx) == len(messages)


@redpanda_domain("redpanda")
def test_stream_src_plugin_trigger_max_rows_stages_while_running(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    messages = user_messages(8)
    bus = broker(domain)
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(len(messages)))
    with bus.fresh_topic() as topic:
        bus.produce(topic, messages, JSON_VARIANT.producer_serde(bus, tmp_path))
        conn, src = stream_src(
            domain,
            topic=topic,
            variant=JSON_VARIANT,
            src_cfg=STREAM_CFG
            | {"tabsdata.kafka.trigger.max_rows": len(messages) // 2},
        )
        run_plugin(ctx, conn, src)

    assert ctx.satisfied
    assert staged_names(ctx) == sorted(message["name"] for message in messages)


@redpanda_domain("redpanda")
def test_stream_src_plugin_schema_change_segregates_files(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(3))
    # No schema enforcement either side, so the two message shapes reach the plugin
    # as they are and each one lands in files the other cannot join.
    unschemad = Variant(
        id="no_sr_json_no_schema",
        data_format="json",
        schema_loader=lambda: "",
        messages=lambda _count: [],
        sr=False,
        columns=set(),
        producer_serde=lambda *_args: JsonKafkaSerde(json_schema=None),
    )
    aged = [{"name": "alice", "age": 1}, {"name": "bob", "age": 2}]
    located = [{"name": "carol", "city": "NYC"}]
    bus = broker(domain)
    with bus.fresh_topic() as topic:
        bus.produce(topic, aged + located, unschemad.producer_serde(bus, tmp_path))
        conn, src = stream_src(
            domain, topic=topic, variant=unschemad, src_cfg=STREAM_CFG
        )
        run_plugin(ctx, conn, src)

    names_by_shape: dict[frozenset[str], list[str]] = {}
    for frame in staged_frames(ctx):
        names_by_shape.setdefault(frozenset(frame.columns), []).extend(frame["name"])
    assert {shape: sorted(names) for shape, names in names_by_shape.items()} == {
        frozenset({KAFKA_META_TOPIC, "name", "age"}): ["alice", "bob"],
        frozenset({KAFKA_META_TOPIC, "name", "city"}): ["carol"],
    }
