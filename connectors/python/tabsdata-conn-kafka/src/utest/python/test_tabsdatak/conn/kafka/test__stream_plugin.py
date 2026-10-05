#
# Copyright 2026 Tabsdata Inc.
#

"""``_stream_plugin.py`` -- StreamSrcDef shape, run-config mapping of
the ``src_cfg`` keys, and ``run()`` over the shared stream pipeline
(consumer patched out)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream import _plugin

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._floor import unsafe_seconds

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import KafkaStreamSrc, KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_plugin import (
    KAFKA_STREAM_SRC,
    KafkaStreamSrcPlugin,
    _build_kafka_run_config,
)
from tabsdatak.conn.kafka.constants import KAFKA_META_TOPIC
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException
from tabsdatak.spi import StreamStageSrc

pytestmark = pytest.mark.kafka


def _creds() -> UserPassword:
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _conn() -> KafkaStreamSrcConn:
    return KafkaStreamSrcConn(
        servers="broker:9092",
        credentials=_creds(),
        group_id="g1",
    )


def _src(**overrides) -> KafkaStreamSrc:
    base = dict(
        topic="events",
        data_format="json",
        schema="",
    )
    base.update(overrides)
    return KafkaStreamSrc(**base)


# --- StreamSrcDef shape --------------------------------------------


def test_kafka_stream_src_def_shape():
    from tabsdatak.conn.kafka._explorer import KafkaConnectionExplorer

    assert KAFKA_STREAM_SRC.type_ == "kafka-topic-in"
    assert KAFKA_STREAM_SRC.src_version == "v1"
    assert KAFKA_STREAM_SRC.conn is KafkaStreamSrcConn
    assert KAFKA_STREAM_SRC.src is KafkaStreamSrc
    assert KAFKA_STREAM_SRC.plugin is KafkaStreamSrcPlugin
    assert KAFKA_STREAM_SRC.explorer is KafkaConnectionExplorer
    assert KAFKA_STREAM_SRC.publisher_src is StreamStageSrc


def test_kafka_stream_src_def_cardinality_always_one():
    assert KAFKA_STREAM_SRC.cardinality(_src()) == 1


# --- run config ----------------------------------------------------


def test_the_buffer_bytes_default_yields_to_the_source_config():
    cfg = _build_kafka_run_config(
        _src(src_cfg={"tabsdata.kafka.buffer.max_bytes": 1024})
    )
    assert cfg.buffer_max_bytes == 1024


def test_run_config_defaults():
    cfg = KafkaStreamSrcPlugin().build_run_config(_src())
    assert cfg.poll_max_secs == 2
    assert cfg.connect_timeout_secs == 16
    assert cfg.trigger_max_secs == 60
    assert cfg.trigger_max_bytes is None
    assert cfg.trigger_max_rows is None
    assert (cfg.buffer_max_rows, cfg.buffer_max_bytes, cfg.buffer_max_secs) == (
        10_000,
        None,
        60,
    )
    assert cfg.poll_idle_secs == 0
    assert cfg.logging_level == logging.ERROR


def test_run_config_reads_the_buffer_and_trigger_keys():
    src = _src(
        src_cfg={
            "tabsdata.kafka.poll.max_secs": 5,
            "kafka.connect_timeout_secs": 30,
            "tabsdata.kafka.buffer.max_rows": 50,
            "tabsdata.kafka.buffer.max_bytes": 4096,
            "tabsdata.kafka.buffer.max_secs": 7,
            "tabsdata.kafka.trigger.max_secs": 120,
            "tabsdata.kafka.trigger.max_bytes": 64 * 1024 * 1024,
            "tabsdata.kafka.trigger.max_rows": 1000,
            "kafka.logging_level": logging.DEBUG,
        }
    )
    cfg = KafkaStreamSrcPlugin().build_run_config(src)
    assert cfg.poll_max_secs == 5
    assert cfg.connect_timeout_secs == 30
    assert cfg.trigger_max_secs == 120
    assert cfg.trigger_max_bytes == 64 * 1024 * 1024
    assert cfg.trigger_max_rows == 1000
    assert cfg.logging_level == logging.DEBUG


def test_run_config_decodes_a_marked_trigger_max_secs():
    src = _src(src_cfg={"tabsdata.kafka.trigger.max_secs": unsafe_seconds(3)})
    assert KafkaStreamSrcPlugin().build_run_config(src).trigger_max_secs == 3


@pytest.mark.parametrize("seconds", [0, 15, 59])
def test_a_trigger_max_secs_below_the_floor_is_rejected(seconds):
    with pytest.raises(TabsdataException) as raised:
        _src(src_cfg={"tabsdata.kafka.trigger.max_secs": seconds})
    assert raised.value.api_error is KafkaErrorCode.KAFKA_5


# --- Plugin.run wiring ---------------------------------------------


@patch("confluent_kafka.Consumer")
def test_plugin_run_subscribes_to_topic(consumer_cls, tmp_path):
    consumer = MagicMock()
    consumer.consume.return_value = []
    consumer_cls.return_value = consumer

    ctx = _make_ctx(tmp_path, active_calls=[False])
    plugin = KafkaStreamSrcPlugin()
    plugin.run(ctx, _conn(), _src())

    consumer_cls.assert_called_once()
    assert consumer.subscribe.call_args.args == (["events"],)
    consumer.close.assert_called_once()


@patch("confluent_kafka.Consumer")
def test_plugin_run_kafka_connection_failure_raises_kafka_6(consumer_cls, tmp_path):
    consumer = MagicMock()
    consumer.list_topics.side_effect = RuntimeError("broker down")
    consumer_cls.return_value = consumer

    ctx = _make_ctx(tmp_path, active_calls=[True])
    plugin = KafkaStreamSrcPlugin()
    with pytest.raises(TabsdataException) as ei:
        plugin.run(ctx, _conn(), _src())
    assert ei.value.api_error is KafkaErrorCode.KAFKA_6


@patch("confluent_kafka.Producer")
@patch("confluent_kafka.Consumer")
def test_plugin_run_builds_dlq_producer_when_dlq_topic_set(
    consumer_cls,
    producer_cls,
    tmp_path,
):
    consumer = MagicMock()
    consumer.consume.return_value = []
    consumer_cls.return_value = consumer
    producer = MagicMock()
    producer_cls.return_value = producer

    ctx = _make_ctx(tmp_path, active_calls=[False])
    plugin = KafkaStreamSrcPlugin()
    plugin.run(ctx, _conn(), _src(dlq_topic="events_dlq"))

    producer_cls.assert_called_once()
    producer.list_topics.assert_called_once()
    producer.flush.assert_called()


@patch("confluent_kafka.Consumer")
def test_plugin_run_no_dlq_topic_skips_producer(consumer_cls, tmp_path):
    consumer = MagicMock()
    consumer.consume.return_value = []
    consumer_cls.return_value = consumer

    ctx = _make_ctx(tmp_path, active_calls=[False])
    plugin = KafkaStreamSrcPlugin()
    with patch("confluent_kafka.Producer") as producer_cls:
        plugin.run(ctx, _conn(), _src(dlq_topic=None))
        producer_cls.assert_not_called()


@patch("confluent_kafka.Consumer")
def test_plugin_run_applies_logging_level_to_connector_namespace(
    consumer_cls,
    tmp_path,
):
    """``kafka.logging_level`` must apply to the whole
    ``tabsdatak.conn.kafka`` namespace, not just the plugin
    module; that's how the ingester + serdes loggers pick it up
    (parallels the CDC convention)."""
    consumer = MagicMock()
    consumer.consume.return_value = []
    consumer_cls.return_value = consumer

    pkg_logger = logging.getLogger("tabsdatak.conn.kafka")
    prev = pkg_logger.level
    pkg_logger.setLevel(logging.NOTSET)
    try:
        ctx = _make_ctx(tmp_path, active_calls=[False])
        plugin = KafkaStreamSrcPlugin()
        plugin.run(
            ctx,
            _conn(),
            _src(src_cfg={"kafka.logging_level": logging.DEBUG}),
        )
        assert pkg_logger.level == logging.DEBUG
    finally:
        pkg_logger.setLevel(prev)


# --- Staging over the shared pipeline --------------------------------


def _message(value: dict, offset: int, partition: int = 0):
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = json.dumps(value).encode("utf-8")
    message.topic.return_value = "events"
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _batches(polls: list) -> list[list]:
    batches, batch = [], []
    for item in polls:
        if item is None:
            if batch:
                batches.append(batch)
                batch = []
            batches.append([])
        else:
            batch.append(item)
    if batch:
        batches.append(batch)
    return batches


def _run(
    tmp_path, polls, *, active_calls, src_cfg=None, dlq_topic=None, consumer=None
):
    from confluent_kafka import TopicPartition

    consumer = consumer or MagicMock()
    consumer.consume.side_effect = polls if callable(polls) else _batches(polls)
    consumer.assignment.return_value = [TopicPartition("events", p) for p in range(8)]
    producer = MagicMock()
    producer.flush.return_value = 0
    ctx = _make_ctx(tmp_path, active_calls=active_calls)
    ctx.first_stage = dlq_topic is None
    src_cfg = {"tabsdata.kafka.trigger.max_secs": unsafe_seconds(0)} | (src_cfg or {})
    with (
        patch("confluent_kafka.Consumer", return_value=consumer),
        patch("confluent_kafka.Producer", return_value=producer),
    ):
        KafkaStreamSrcPlugin().run(
            ctx, _conn(), _src(src_cfg=src_cfg, dlq_topic=dlq_topic)
        )
    return ctx, consumer


def _committed_offsets(consumer) -> list[dict[int, int]]:
    return [
        {tp.partition: tp.offset for tp in call.kwargs["offsets"]}
        for call in consumer.commit.call_args_list
    ]


def _staged_ids(ctx) -> list[list[list[int]]]:
    return [[frame["a"].to_list() for frame in files] for files in _staged_frames(ctx)]


def _staged_frames(ctx) -> list[list[pl.DataFrame]]:
    return [
        [pl.from_arrow(pq.read_table(path)) for path in manifest[0]]
        for manifest in ctx.manifests
    ]


def test_a_schema_change_adds_a_file_to_the_same_manifest(tmp_path):
    polls = [_message({"a": 1}, 1), _message({"a": 2, "z": 9}, 2), None, None]
    ctx, consumer = _run(tmp_path, polls, active_calls=[True, False])

    ((first, second),) = _staged_frames(ctx)
    assert first.columns == [KAFKA_META_TOPIC, "a"]
    assert second.columns == [KAFKA_META_TOPIC, "a", "z"]
    assert _committed_offsets(consumer) == [{0: 3}]


def test_each_stage_commits_exactly_what_it_staged(tmp_path):
    polls = [_message({"a": 1}, 1), _message({"a": 2}, 2), None]
    polls += [_message({"a": 3}, 3), None, None, None]
    ctx, consumer = _run(tmp_path, polls, active_calls=[True, True, False])

    assert _staged_ids(ctx) == [[[1, 2]], [[3]]]
    assert _committed_offsets(consumer) == [{0: 3}, {0: 4}]


def test_every_partition_read_is_committed(tmp_path):
    polls = [
        _message({"a": 1}, 10, partition=0),
        _message({"a": 2}, 20, partition=1),
        _message({"a": 3}, 11, partition=0),
        None,
        None,
    ]
    ctx, consumer = _run(tmp_path, polls, active_calls=[True, False])

    assert _staged_ids(ctx) == [[[1, 2, 3]]]
    assert _committed_offsets(consumer) == [{0: 12, 1: 21}]


def test_a_run_of_dead_letters_alone_acknowledges_their_offsets(tmp_path):
    bad = MagicMock()
    bad.error.return_value = None
    bad.value.return_value = b"not json"
    bad.key.return_value = None
    bad.timestamp.return_value = (0, 0)
    bad.topic.return_value = "events"
    bad.partition.return_value = 0
    bad.offset.return_value = 41
    polls = [bad, None]

    ctx, consumer = _run(
        tmp_path, polls, active_calls=[True, False], dlq_topic="events_dlq"
    )

    assert ctx.manifests == []
    assert _committed_offsets(consumer) == [{0: 42}]


def test_lost_partitions_restart_the_consumer_and_the_run_goes_on(tmp_path):
    from confluent_kafka import TopicPartition

    consumer = MagicMock()
    good = _message({"a": 1}, 7)
    answers = iter([good, None])

    def consume(**_kwargs):
        if not consumer.close.called:
            on_lost = consumer.subscribe.call_args.kwargs["on_lost"]
            on_lost(consumer, [TopicPartition("events", 0)])
            return []
        message = next(answers, None)
        return [message] if message is not None else []

    ctx, consumer = _run(
        tmp_path, consume, active_calls=[True, True, True, False], consumer=consumer
    )

    assert consumer.close.call_count == 2
    assert len(ctx.manifests) == 1
    assert _committed_offsets(consumer) == [{0: 8}]


def test_a_signalled_stop_stages_and_commits_nothing(tmp_path, monkeypatch):
    preset = StopSignal()
    preset.set()
    monkeypatch.setattr(_plugin.StopSignal, "install", lambda: preset)
    polls = [_message({"a": 1}, 1), None]
    ctx, consumer = _run(tmp_path, polls, active_calls=[True, True])

    assert ctx.manifests == []
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


# --- Test helpers --------------------------------------------------


def _make_ctx(work_dir: Path, *, active_calls: list[bool]):
    """Minimal :class:`StreamSrcPluginCtx` stand-in. ``active_calls`` is
    read in order on each ``active`` access; once exhausted the last
    value is repeated. ``stage`` records each manifest and invokes the
    commit callback, as the runtime does."""
    calls = iter(active_calls)
    last = [active_calls[-1] if active_calls else False]

    class _Ctx:
        def __init__(self) -> None:
            self.work_dir = work_dir
            self.flush_interval_mins = 0
            self.first_stage = True
            self.stream_state = None
            self.manifests: list = []

        @property
        def active(self) -> bool:
            try:
                v = next(calls)
                last[0] = v
                return v
            except StopIteration:
                return last[0]

        def stage(self, tables_files, new_state) -> None:
            self.manifests.append(tables_files)
            new_state()

    return _Ctx()
