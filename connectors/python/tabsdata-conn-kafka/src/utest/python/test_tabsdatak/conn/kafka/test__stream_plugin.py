"""``_stream_plugin.py`` -- StreamSrcDef shape, rollover-cfg
resolution, and ``run()`` wiring (consumer patched out)."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import KafkaConn, KafkaStreamSrc

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_plugin import (
    KAFKA_STREAM_SRC,
    KafkaStreamSrcPlugin,
    _resolve_rollover_cfg,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.kafka


def _creds() -> UserPassword:
    return UserPassword(user=Secret.lit("u"), password=Secret.lit("p"))


def _src(**overrides) -> KafkaStreamSrc:
    base = dict(
        conn=KafkaConn(
            servers="broker:9092", credentials=_creds(), group_id="g1",
        ),
        topic="events",
        data_format="json",
        schema="",
    )
    base.update(overrides)
    return KafkaStreamSrc(**base)


# --- StreamSrcDef shape --------------------------------------------


def test_kafka_stream_src_def_shape():
    assert KAFKA_STREAM_SRC.src_type == "kafka"
    assert KAFKA_STREAM_SRC.src_version == "v1"
    assert KAFKA_STREAM_SRC.conn is KafkaConn
    assert KAFKA_STREAM_SRC.src is KafkaStreamSrc
    assert KAFKA_STREAM_SRC.plugin is KafkaStreamSrcPlugin
    assert KAFKA_STREAM_SRC.explorer is None
    assert KAFKA_STREAM_SRC.uses_manifest is True


def test_kafka_stream_src_def_cardinality_always_one():
    assert KAFKA_STREAM_SRC.cardinality(_src()) == 1


# --- _resolve_rollover_cfg -----------------------------------------


def test_resolve_rollover_cfg_defaults():
    cfg = _resolve_rollover_cfg(_src())
    assert cfg["consume_timeout_secs"] == 2
    assert cfg["connection_timeout_secs"] == 16
    assert cfg["time_rollover_secs"] == 60
    assert cfg["size_rollover_mb"] is None
    assert cfg["messages_rollover"] is None
    assert cfg["logging_level"] == logging.ERROR


def test_resolve_rollover_cfg_overrides():
    src = _src(src_cfg={
        "kafka.consume_timeout_seconds": 5,
        "kafka.connection_timeout_seconds": 30,
        "kafka.time_rollover_seconds": 120,
        "kafka.size_rollover_mb": 64,
        "kafka.messages_rollover": 1000,
        "kafka.logging_level": logging.DEBUG,
    })
    cfg = _resolve_rollover_cfg(src)
    assert cfg["consume_timeout_secs"] == 5
    assert cfg["connection_timeout_secs"] == 30
    assert cfg["time_rollover_secs"] == 120
    assert cfg["size_rollover_mb"] == 64
    assert cfg["messages_rollover"] == 1000
    assert cfg["logging_level"] == logging.DEBUG


# --- Plugin.run wiring ---------------------------------------------


@patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler")
@patch("confluent_kafka.Consumer")
def test_plugin_run_subscribes_to_topic(consumer_cls, _sigterm, tmp_path):
    consumer = MagicMock()
    consumer.poll.return_value = None
    consumer_cls.return_value = consumer

    ctx = _make_ctx(tmp_path, active_calls=[False])
    plugin = KafkaStreamSrcPlugin()
    plugin.run(ctx, _src())

    consumer_cls.assert_called_once()
    consumer.subscribe.assert_called_once_with(["events"])
    consumer.close.assert_called_once()


@patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler")
@patch("confluent_kafka.Consumer")
def test_plugin_run_kafka_connection_failure_raises_kafka_6(
    consumer_cls, _sigterm, tmp_path,
):
    consumer = MagicMock()
    consumer.list_topics.side_effect = RuntimeError("broker down")
    consumer_cls.return_value = consumer

    ctx = _make_ctx(tmp_path, active_calls=[True])
    plugin = KafkaStreamSrcPlugin()
    with pytest.raises(TabsdataException) as ei:
        plugin.run(ctx, _src())
    assert ei.value.api_error is KafkaErrorCode.KAFKA_6


@patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler")
@patch("confluent_kafka.Producer")
@patch("confluent_kafka.Consumer")
def test_plugin_run_builds_dlq_producer_when_dlq_topic_set(
    consumer_cls, producer_cls, _sigterm, tmp_path,
):
    consumer = MagicMock()
    consumer.poll.return_value = None
    consumer_cls.return_value = consumer
    producer = MagicMock()
    producer_cls.return_value = producer

    ctx = _make_ctx(tmp_path, active_calls=[False])
    plugin = KafkaStreamSrcPlugin()
    plugin.run(ctx, _src(dlq_topic="events_dlq"))

    producer_cls.assert_called_once()
    producer.list_topics.assert_called_once()
    producer.flush.assert_called()


@patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler")
@patch("confluent_kafka.Consumer")
def test_plugin_run_no_dlq_topic_skips_producer(
    consumer_cls, _sigterm, tmp_path,
):
    consumer = MagicMock()
    consumer.poll.return_value = None
    consumer_cls.return_value = consumer

    ctx = _make_ctx(tmp_path, active_calls=[False])
    plugin = KafkaStreamSrcPlugin()
    with patch("confluent_kafka.Producer") as producer_cls:
        plugin.run(ctx, _src(dlq_topic=None))
        producer_cls.assert_not_called()


# --- logging_level wiring ------------------------------------------


@patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler")
@patch("confluent_kafka.Consumer")
def test_plugin_run_applies_logging_level_to_connector_namespace(
    consumer_cls, _sigterm, tmp_path,
):
    """``kafka.logging_level`` must apply to the whole
    ``tabsdatak.conn.kafka`` namespace, not just the plugin
    module; that's how the consumer + serdes loggers pick it up
    (parallels the ``run_cdc`` convention)."""
    consumer = MagicMock()
    consumer.poll.return_value = None
    consumer_cls.return_value = consumer

    pkg_logger = logging.getLogger("tabsdatak.conn.kafka")
    prev = pkg_logger.level
    pkg_logger.setLevel(logging.NOTSET)
    try:
        ctx = _make_ctx(tmp_path, active_calls=[False])
        plugin = KafkaStreamSrcPlugin()
        plugin.run(
            ctx,
            _src(src_cfg={"kafka.logging_level": logging.DEBUG}),
        )
        assert pkg_logger.level == logging.DEBUG
    finally:
        pkg_logger.setLevel(prev)


# --- Test helpers --------------------------------------------------


def _make_ctx(work_dir: Path, *, active_calls: list[bool]):
    """Mock :class:`StreamSrcPluginCtx`. ``active_calls`` is read
    in order on each ``is_active()`` invocation; once exhausted the
    last value is repeated."""
    ctx = MagicMock()
    ctx.work_dir = work_dir
    calls = iter(active_calls)
    last = [active_calls[-1] if active_calls else False]

    def _is_active():
        try:
            v = next(calls)
            last[0] = v
            return v
        except StopIteration:
            return last[0]

    ctx.is_active.side_effect = _is_active
    ctx.trigger_monitor = MagicMock()
    ctx.trigger = MagicMock()
    ctx.stage = MagicMock(return_value=MagicMock())
    ctx.commit = MagicMock()
    return ctx
