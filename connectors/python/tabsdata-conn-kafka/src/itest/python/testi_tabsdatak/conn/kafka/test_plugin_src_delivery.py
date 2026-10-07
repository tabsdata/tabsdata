#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka stream-source-plugin integration tests of the delivery guarantees: what
is acknowledged, what is read exactly once, what becomes a dead letter."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.kafka.backend import broker, group_id, src_conn
from testi_tabsdatak.conn.kafka.harness import (
    STREAM_CFG,
    KafkaCtx,
    PluginRun,
    avro_schema,
    batch_of,
    confluent_avro,
    json_schema,
    run_plugin,
    staged_at_least,
    staged_frames,
    staged_names,
    staged_rows,
    user_messages,
)

from tabsdatak.conn.kafka import ConfluentSchemaRegistry, KafkaStreamSrc
from tabsdatak.conn.kafka.constants import KAFKA_META_TOPIC
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.xdist_group("kafka_stream_plugin_integration_seq"),
]

EAGER = batch_of(1)


def _json_src(topic: str, *, dlq_topic: str | None = None, **cfg) -> KafkaStreamSrc:
    return KafkaStreamSrc(
        topic=topic,
        data_format="json",
        schema=json_schema(),
        dlq_topic=dlq_topic,
        src_cfg=STREAM_CFG | cfg,
    )


def _keys(count: int) -> list[bytes]:
    return [f"key-{index}".encode() for index in range(count)]


@redpanda_domain("redpanda")
def test_every_partition_is_acknowledged_up_to_what_was_read(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    messages = user_messages(12)
    group = group_id()
    ctx = KafkaCtx(
        work_dir(tmp_path), until=staged_at_least(len(messages)), thresholds=EAGER
    )
    with bus.fresh_topic(partitions=3) as topic:
        bus.produce(topic, messages, keys=_keys(len(messages)))
        run_plugin(ctx, src_conn(domain, group=group), _json_src(topic))

        assert ctx.satisfied
        assert staged_names(ctx) == sorted(message["name"] for message in messages)
        ends = bus.ends(topic, 3)
        assert len(ends) > 1
        assert bus.acknowledged(group, topic, 3) == ends


@redpanda_domain("redpanda")
def test_two_consumers_of_one_group_stage_every_message_exactly_once(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    first_wave, second_wave = user_messages(8), user_messages(16)[8:]
    total = len(first_wave) + len(second_wave)
    group = group_id()
    contexts: list[KafkaCtx] = []

    def everything_staged(_ctx: KafkaCtx) -> bool:
        return sum(staged_rows(ctx) for ctx in contexts) >= total

    with bus.fresh_topic(partitions=4) as topic:
        bus.produce(topic, first_wave, keys=_keys(len(first_wave)))
        src = _json_src(topic)
        for name in ("a", "b"):
            contexts.append(
                KafkaCtx(
                    work_dir(tmp_path, name), until=everything_staged, thresholds=EAGER
                )
            )
        first = PluginRun(contexts[0], src_conn(domain, group=group), src).start()
        time.sleep(6)
        second = PluginRun(contexts[1], src_conn(domain, group=group), src).start()
        time.sleep(6)
        bus.produce(topic, second_wave, keys=_keys(len(second_wave)))
        first.join()
        second.join()

        names = [name for ctx in contexts for name in staged_names(ctx)]
        assert sorted(names) == sorted(
            message["name"] for message in first_wave + second_wave
        )
        assert bus.acknowledged(group, topic, 4) == bus.ends(topic, 4)


@redpanda_domain("redpanda")
def test_dead_letters_are_acknowledged_and_can_be_read_back(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    good = user_messages(3)
    group = group_id()
    ctx = KafkaCtx(
        work_dir(tmp_path), until=staged_at_least(len(good)), thresholds=EAGER
    )
    with bus.fresh_topic() as topic, bus.fresh_topic() as dead:
        bus.produce(topic, good[:2])
        bus.produce_raw(topic, [b"not json", b"{broken"])
        bus.produce(topic, [{"name": "incomplete"}])
        bus.produce(topic, good[2:])
        run_plugin(ctx, src_conn(domain, group=group), _json_src(topic, dlq_topic=dead))

        assert ctx.satisfied
        assert staged_names(ctx) == sorted(message["name"] for message in good)
        assert bus.acknowledged(group, topic, 1) == {0: 6}
        letters = bus.read_back(dead, 3)
        assert sorted(letter["offset"] for letter in letters) == [2, 3, 4]
        by_offset = {letter["offset"]: letter for letter in letters}
        assert by_offset[2]["original_value"] == "not json"
        assert by_offset[3]["original_value"] == "{broken"
        assert "incomplete" in by_offset[4]["original_value"]
        assert all(letter["topic"] == topic for letter in letters)


@redpanda_domain("redpanda")
def test_a_stretch_of_dead_letters_alone_moves_the_offset(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    group = group_id()
    with bus.fresh_topic() as topic, bus.fresh_topic() as dead:
        bus.produce_raw(topic, [b"bad"] * 4)
        ctx = KafkaCtx(
            work_dir(tmp_path),
            until=lambda _ctx: bus.acknowledged(group, topic, 1) == {0: 4},
            thresholds=EAGER,
        )
        run_plugin(
            ctx,
            src_conn(domain, group=group),
            _json_src(topic, dlq_topic=dead, **{"tabsdata.kafka.buffer.max_secs": 1}),
        )

        assert ctx.satisfied
        assert staged_rows(ctx) == 0
        assert len(bus.read_back(dead, 4)) == 4


@redpanda_domain("redpanda")
def test_rows_that_do_not_fit_the_table_types_are_dead_letters(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    group = group_id()
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(2), thresholds=EAGER)
    with bus.fresh_topic() as topic, bus.fresh_topic() as dead:
        bus.produce(topic, [{"a": 1}, {"a": "x"}, {"a": 2}])
        src = KafkaStreamSrc(
            topic=topic,
            data_format="json",
            schema="",
            dlq_topic=dead,
            src_cfg=STREAM_CFG,
        )
        run_plugin(ctx, src_conn(domain, group=group), src)

        assert ctx.satisfied
        frame = pl.concat(staged_frames(ctx))
        assert sorted(frame["a"].to_list()) == [1, 2]
        (letter,) = bus.read_back(dead, 1)
        assert letter["record"] == {"a": "x"}
        assert letter["original_value"] is None
        assert letter["offset"] == 1
        assert bus.acknowledged(group, topic, 1) == {0: 3}


@redpanda_domain("redpanda")
def test_a_regex_subscription_labels_every_row_with_its_topic(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    prefix = f"td_it_rx{uuid.uuid4().hex[:8]}"
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(4), thresholds=EAGER)
    with bus.fresh_topic(f"{prefix}_a") as a, bus.fresh_topic(f"{prefix}_b") as b:
        bus.produce(a, user_messages(2))
        bus.produce(b, user_messages(4)[2:])
        run_plugin(ctx, src_conn(domain), _json_src(f"^{prefix}_.*"))

        assert ctx.satisfied
        frame = pl.concat(staged_frames(ctx))
        labels = dict(zip(frame["name"].to_list(), frame[KAFKA_META_TOPIC].to_list()))
        assert labels == {"name_1": a, "name_2": a, "name_3": b, "name_4": b}


@redpanda_domain("redpanda")
def test_a_registry_outage_stops_the_stream_without_acknowledging(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    bus = broker(domain)
    group = group_id()
    ctx = KafkaCtx(work_dir(tmp_path), until=staged_at_least(1), thresholds=EAGER)
    unreachable = ConfluentSchemaRegistry(url="http://127.0.0.1:9", credentials=None)
    with bus.fresh_topic() as topic:
        bus.produce(topic, user_messages(2), confluent_avro(bus, tmp_path))
        src = KafkaStreamSrc(
            topic=topic,
            data_format="avro",
            schema=avro_schema(),
            src_cfg=STREAM_CFG,
        )
        with pytest.raises(TabsdataException) as raised:
            conn = src_conn(domain, group=group, schema_registry=unreachable)
            run_plugin(ctx, conn, src)

        assert raised.value.api_error is KafkaErrorCode.KAFKA_27
        assert bus.acknowledged(group, topic, 1) == {}
        assert staged_rows(ctx) == 0
