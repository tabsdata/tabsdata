"""Kafka stream-source plugin glue.

Mirrors the layering of the CDC sides (``_cdc_plugin.py`` /
``_cdc_impl.py``): the plugin lives here, the consumer / buffer
sit in ``_stream_impl.py``, and the wire-format (de)serialisers
in :mod:`tabsdatak.conn.kafka._serdes`.

Each rollover composes a single-table :class:`TableManifest`,
hands it to ``ctx.stage``, and commits the returned
:class:`StageRef` -- exactly like the CDC sources.

* :class:`KafkaStreamSrcPlugin` -- wires
  :class:`KafkaStreamSrc` to a polling consumer + manifest-based
  staging.
* :data:`KAFKA_STREAM_SRC` -- entry-point :class:`StreamSrcDef`.
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone
from typing import cast

from tabsdatak.conn.kafka import (
    KafkaConn,
    KafkaStreamSrc,
    SrcCfgKey,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes import (
    build_schema_registry_client,
    build_serde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    DEFAULT_CONNECTION_TIMEOUT_SECS,
    DEFAULT_CONSUME_TIMEOUT_SECS,
    DEFAULT_TIME_ROLLOVER_SECS,
    KafkaConsumer,
    RolloverReason,
    build_kafka_configs,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.spi import (
    ParquetFile,
    StageRef,
    StreamSrcDef,
    StreamSrcPlugin,
    StreamSrcPluginCtx,
    TableManifest,
)


logger = logging.getLogger(__name__)


# Allowed ``KafkaStreamSrc.src_cfg`` keys -- mirrors the literal
# annotation on :data:`tabsdatak.conn.kafka.SrcCfgKey` so a typo
# in the runtime key spelling is caught by Pyright.
_KEY_CONSUME_TIMEOUT: SrcCfgKey = "kafka.consume_timeout_seconds"
_KEY_CONNECTION_TIMEOUT: SrcCfgKey = "kafka.connection_timeout_seconds"
_KEY_LOGGING_LEVEL: SrcCfgKey = "kafka.logging_level"
_KEY_TIME_ROLLOVER: SrcCfgKey = "kafka.time_rollover_seconds"
_KEY_SIZE_ROLLOVER_MB: SrcCfgKey = "kafka.size_rollover_mb"
_KEY_MESSAGES_ROLLOVER: SrcCfgKey = "kafka.messages_rollover"


def _resolve_rollover_cfg(src: KafkaStreamSrc) -> dict:
    """Pull the rollover / timeout knobs off ``src.src_cfg`` with
    sensible defaults. Centralised here so ``KafkaStreamSrcPlugin.run``
    stays focused on lifecycle."""
    cfg = dict(src.src_cfg or {})
    return {
        "consume_timeout_secs": int(
            cfg.get(_KEY_CONSUME_TIMEOUT, DEFAULT_CONSUME_TIMEOUT_SECS),
        ),
        "connection_timeout_secs": int(
            cfg.get(_KEY_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_SECS),
        ),
        "time_rollover_secs": int(
            cfg.get(_KEY_TIME_ROLLOVER, DEFAULT_TIME_ROLLOVER_SECS),
        ),
        "size_rollover_mb": cfg.get(_KEY_SIZE_ROLLOVER_MB),
        "messages_rollover": cfg.get(_KEY_MESSAGES_ROLLOVER),
        "logging_level": int(cfg.get(_KEY_LOGGING_LEVEL, logging.ERROR)),
    }


def _setup_sigterm_handler() -> None:
    """Translate SIGTERM into ``KeyboardInterrupt`` so the consume
    loop unwinds cleanly through its ``finally`` (final flush +
    commit). Only installs on the main thread; the integration
    tests patch this out when running the plugin on a worker."""
    def _handler(_signum, _frame):  # noqa: ANN001 -- signal callback
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGTERM, _handler)
    except ValueError:
        # Off the main thread -- no-op, the test ctx is expected
        # to drive shutdown via ``is_active()`` instead.
        pass


class KafkaStreamSrcPlugin(StreamSrcPlugin[KafkaStreamSrc]):
    """Wires a :class:`KafkaStreamSrc` to a polling consumer +
    manifest-based staging."""

    def run(
        self,
        ctx: StreamSrcPluginCtx,
        stream_src: KafkaStreamSrc,
    ) -> None:
        from confluent_kafka import Consumer

        conn = cast(KafkaConn, stream_src.conn)
        cfg = _resolve_rollover_cfg(stream_src)
        # Set the level on the whole connector namespace so the
        # plugin, consumer wrapper, serdes and the kafka-config
        # builder all report at the same verbosity (mirrors the
        # ``run_cdc`` convention on ``tabsdatak.conn.common.sql.cdc``).
        logging.getLogger("tabsdatak.conn.kafka").setLevel(
            cfg["logging_level"],
        )
        logger.info(
            "kafka-stream: starting (topic=%r, format=%r)",
            stream_src.topic, stream_src.data_format,
        )

        kafka_configs = build_kafka_configs(
            conn=conn, group_id=stream_src.group_id,
        )
        try:
            consumer = Consumer(kafka_configs)
            consumer.list_topics(timeout=cfg["connection_timeout_secs"])
        except Exception as exc:
            raise KafkaErrorCode.KAFKA_6.exception(
                cause=exc, servers=kafka_configs.get("bootstrap.servers"),
            )

        consumer.subscribe([stream_src.topic])

        dlq_producer = self._build_dlq_producer(
            kafka_configs=kafka_configs,
            dlq_topic=stream_src.dlq_topic,
            connection_timeout_secs=cfg["connection_timeout_secs"],
        )

        # SR client (Confluent / Glue); ``None`` for the no-SR
        # path. The Protobuf SR variant wants a second client for
        # the deserializer (see ``ProtobufSrKafkaSerde``); the
        # plugin builds it lazily inside ``build_serde``.
        sr_client = build_schema_registry_client(
            schema_registry=conn.schema_registry,
        )
        sr_client_alt = None
        if sr_client is not None and stream_src.data_format not in (
            "json", "avro",
        ):
            # Only Protobuf SR needs the second client.
            sr_client_alt = build_schema_registry_client(
                schema_registry=conn.schema_registry,
            )

        serde = build_serde(
            data_format=stream_src.data_format,
            schema=stream_src.schema,
            schema_registry=conn.schema_registry,
            working_dir=str(ctx.work_dir),
            sr_client=sr_client,
            sr_client_alt=sr_client_alt,
        )

        kafka_consumer = KafkaConsumer(
            topic=stream_src.topic,
            consumer=consumer,
            serde=serde,
            size_rollover_mb=cfg["size_rollover_mb"],
            messages_rollover=cfg["messages_rollover"],
            dlq_producer=dlq_producer,
            dlq_topic=stream_src.dlq_topic,
        )

        _setup_sigterm_handler()
        logger.info("kafka-stream: entering poll loop")
        try:
            self._consume_loop(
                ctx=ctx,
                stream_src=stream_src,
                kafka_consumer=kafka_consumer,
                cfg=cfg,
            )
        finally:
            kafka_consumer.close()
            if dlq_producer is not None:
                try:
                    dlq_producer.flush()
                except Exception as exc:  # pragma: no cover
                    logger.warning("Failed to flush DLQ producer: %s", exc)
            logger.info("kafka-stream: stopped")

    @staticmethod
    def _build_dlq_producer(
        *,
        kafka_configs: dict,
        dlq_topic: str | None,
        connection_timeout_secs: int,
    ):
        if dlq_topic is None:
            return None
        from confluent_kafka import Producer

        try:
            producer = Producer(kafka_configs)
            producer.list_topics(timeout=connection_timeout_secs)
            return producer
        except Exception as exc:
            raise KafkaErrorCode.KAFKA_7.exception(
                cause=exc, servers=kafka_configs.get("bootstrap.servers"),
            )

    def _consume_loop(
        self,
        *,
        ctx: StreamSrcPluginCtx,
        stream_src: KafkaStreamSrc,
        kafka_consumer: KafkaConsumer,
        cfg: dict,
    ) -> None:
        """Poll + rollover loop. Stages each completed buffer as a
        single-table :class:`TableManifest`; exits cleanly on
        ``ctx.is_active() == False`` or when ``stream_src.end`` is
        reached.
        """
        deadline_window_start = time.time()
        reset_on_next_consume = False
        try:
            while ctx.is_active():
                # Honour the source's end-time gate -- the legacy
                # connector exits the loop here once we're past it.
                if stream_src.end is not None and (
                    datetime.now(timezone.utc) > stream_src.end
                ):
                    logger.info(
                        "kafka-stream: end-time reached, exiting loop "
                        "(end=%s)", stream_src.end,
                    )
                    break
                ctx.trigger_monitor()
                rollover_reason, buffer = kafka_consumer.consume(
                    timeout_secs=cfg["consume_timeout_secs"],
                    reset_buffer=reset_on_next_consume,
                )
                logger.debug(
                    "kafka-stream: poll completed (reason=%s, msgs=%d, "
                    "size_mb=%d)",
                    rollover_reason.value,
                    buffer.message_count,
                    buffer.size_mb,
                )
                elapsed = time.time() - deadline_window_start
                max_timeout = cfg["time_rollover_secs"] - elapsed
                if rollover_reason != RolloverReason.Time or max_timeout <= 0:
                    deadline_window_start = self._handle_rollover(
                        ctx=ctx,
                        stream_src=stream_src,
                        kafka_consumer=kafka_consumer,
                        buffer=buffer,
                        rollover_reason=rollover_reason,
                    )
                reset_on_next_consume = max_timeout <= 0
        except KeyboardInterrupt:
            logger.info("kafka-stream: interrupted, exiting loop")
        finally:
            # Final flush so the trailing partial batch is staged.
            final_buffer = kafka_consumer.buffer
            if final_buffer is not None and final_buffer.message_count:
                logger.info(
                    "kafka-stream: shutdown flush (%d msgs)",
                    final_buffer.message_count,
                )
                self._handle_rollover(
                    ctx=ctx,
                    stream_src=stream_src,
                    kafka_consumer=kafka_consumer,
                    buffer=final_buffer,
                    rollover_reason=RolloverReason.Time,
                )

    @staticmethod
    def _handle_rollover(
        *,
        ctx: StreamSrcPluginCtx,
        stream_src: KafkaStreamSrc,
        kafka_consumer: KafkaConsumer,
        buffer,
        rollover_reason: RolloverReason,
    ) -> float:
        """Write the buffer to parquet, build a :class:`TableManifest`
        for the topic's output table, stage + commit through the
        plugin ctx, and commit the Kafka consumer offset.

        Returns the new ``deadline_window_start`` for the time-based
        rollover bookkeeping in the loop above."""
        if buffer.is_flushed or buffer.message_count == 0:
            return time.time()
        parquet_path = buffer.to_parquet(dir_path=str(ctx.work_dir))
        if parquet_path is None:
            return time.time()
        # ``TableManifest.files`` does an ``isinstance(..., ParquetFile)``
        # check; ``Path``-cast won't pass. Wrap explicitly.
        manifest = [TableManifest(
            table=stream_src.output_table(),
            files=[ParquetFile(parquet_path)],
        )]
        stage_ref: StageRef = ctx.stage(manifest)
        ctx.commit(stage_ref)
        kafka_consumer.commit()
        if rollover_reason != RolloverReason.SchemaChange:
            # Schema-change rollovers don't fire downstream
            # immediately; the legacy connector matches this
            # behaviour so trigger callers don't react to a half
            # batch.
            ctx.trigger()
        logger.info(
            "kafka-stream: staged + committed (reason=%s, msgs=%d)",
            rollover_reason.value, buffer.message_count,
        )
        return time.time()


KAFKA_STREAM_SRC = StreamSrcDef(
    conn=KafkaConn,
    src_type="kafka",
    src_version="v1",
    src=KafkaStreamSrc,
    # One Kafka topic -> one output table; cardinality is always 1.
    cardinality=lambda src: 1,
    # Unlike the legacy connector, the new port uses the
    # manifest-stage contract the CDC sources use.
    uses_manifest=True,
    plugin=KafkaStreamSrcPlugin,
    explorer=None,
)
