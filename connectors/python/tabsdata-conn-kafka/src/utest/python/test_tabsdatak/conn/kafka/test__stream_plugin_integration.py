"""End-to-end :class:`KafkaStreamSrcPlugin` integration tests
against a real Redpanda broker (+ embedded Schema Registry).

The ``kafka_config`` fixture provisions Redpanda the same way the
legacy ``tabsdata-kafka`` tests do (see
``test_tabsdata.common.backend.kafka``). Each test gets a fresh
topic + per-test consumer group via :func:`fresh_kafka_topic`.

Per-function unit coverage of the serdes / consumer / buffer
lives in ``test__serdes_*.py`` / ``test__stream_impl.py``; plugin
wiring (no broker) in ``test__stream_plugin.py``. This module
exercises the full plugin loop end-to-end against the broker and
schema registry: produce → consume → deserialise → buffer → stage
+ commit through a real :class:`StreamSrcPluginCtx`.

The parametrised matrix covers all six supported wire-format /
schema-registry combinations (no-SR + Confluent SR for JSON, Avro
and Protobuf). AWS Glue paths are exercised by the unit serde
tests against a stubbed boto3 client -- AWS isn't available
locally.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import (
    ConfluentSchemaRegistry,
    KafkaStreamSrcConn,
    KafkaStreamSrc,
    Protobuf,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes import (
    AvroKafkaSerde,
    AvroSrKafkaSerde,
    JsonKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufKafkaSerde,
    ProtobufSrKafkaSerde,
)
from tabsdatak.conn.kafka._stream_plugin import KafkaStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx


logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.kafka, pytest.mark.xdist_group("kafka-stream_plugin_integration_seq")]


_SCHEMAS_DIR = Path(__file__).parent.parent.parent.parent.parent / (
    "resources/test_tabsdatak/conn/kafka/schemas"
)
PROTOBUF_MESSAGE_NAME = "PersonV1"


# ---------------------------------------------------------------------------
# Test ctx (mirrors the postgres CDC integration ctx)
# ---------------------------------------------------------------------------


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _KafkaCtx(StreamSrcPluginCtx):
    """Real :class:`StreamSrcPluginCtx` implementation. ``active_polls``
    caps how many times the plugin can read ``active`` before it
    returns ``False`` -- that's the test's exit lever for the
    otherwise-infinite consume loop."""

    def __init__(self, work_dir: Path, *, active_polls: int) -> None:
        self._work_dir = work_dir
        self._active_left = active_polls
        self._state: dict[str, Any] = {}
        self._tabsdata_owns_state = False
        self.staged: list[_StagedManifest] = []
        self._stage_counter = 0
        self._lock = threading.Lock()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "kafka-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "kafka-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

    @property
    def active(self) -> bool:
        with self._lock:
            if self._active_left <= 0:
                return False
            self._active_left -= 1
            return True

    @property
    def first_stage(self) -> bool:
        with self._lock:
            return not self.staged

    @property
    def stream_state(self) -> dict | None:
        with self._lock:
            if not self._tabsdata_owns_state:
                return None
            return dict(self._state) if self._state else None

    @property
    def flush_interval_mins(self) -> int:
        return 0

    def stage(self, tables_files, new_state):
        with self._lock:
            self.staged.append(
                _StagedManifest(manifest=list(tables_files), committed=True)
            )
            # Tabsdata-owned state persists the dict (read back via
            # ``stream_state``); a plain callable (external system owns the
            # offset) leaves ``stream_state`` None and is invoked for its
            # side effect.
            if isinstance(new_state, dict):
                self._state = dict(new_state)
                self._tabsdata_owns_state = True
            else:
                self._tabsdata_owns_state = False
                new_state()


# ---------------------------------------------------------------------------
# Broker helpers
# ---------------------------------------------------------------------------


def _admin_config(kafka_config) -> dict:
    return {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }


def _create_topic(kafka_config, topic: str) -> None:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient(_admin_config(kafka_config))
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
    )
    for t, f in futures.items():
        try:
            f.result(timeout=16)
        except Exception as e:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


def _delete_topic(kafka_config, topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(_admin_config(kafka_config))
    futures = admin.delete_topics([topic])
    for _t, f in futures.items():
        try:
            f.result(timeout=16)
        except Exception:  # noqa: BLE001
            pass


def _produce(kafka_config, topic: str, serde, messages: list[dict]) -> None:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import (
        MessageField, SerializationContext,
    )

    producer = Producer(_admin_config(kafka_config))
    ctx = SerializationContext(topic, MessageField.VALUE)
    serialize = serde.serializer()
    for msg in messages:
        producer.produce(topic, value=serialize(msg, ctx))
        producer.poll(0)
    producer.flush()


@pytest.fixture
def fresh_kafka_topic(kafka_config):
    """Per-test: yields a fresh underscored topic name and cleans
    it up at the end. ``TableNameSpec`` rejects hyphens so we
    can't use ``uuid4()`` strings raw -- prefix + hex-only suffix."""
    topic = f"td_test_{uuid.uuid4().hex[:12]}"
    _create_topic(kafka_config, topic)
    try:
        yield topic
    finally:
        _delete_topic(kafka_config, topic)


# ---------------------------------------------------------------------------
# Serde-matrix parametrisation
# ---------------------------------------------------------------------------


def _avro_schema() -> str:
    return (_SCHEMAS_DIR / "user_v1.avsc").read_text()


def _json_schema() -> str:
    return (_SCHEMAS_DIR / "user.json").read_text()


def _proto_schema() -> str:
    return (_SCHEMAS_DIR / "person_v1.proto").read_text()


def _avro_messages(n: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "favorite_number": i, "favorite_color": "blue"}
        for i in range(1, n + 1)
    ]


def _json_messages(n: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "favorite_number": i, "favorite_color": "blue"}
        for i in range(1, n + 1)
    ]


def _proto_messages(n: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "id": i, "email": f"name_{i}@provider.com"}
        for i in range(1, n + 1)
    ]


def _build_confluent_sr_client(kafka_config):
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient({
        "url": kafka_config.schema_registry_url,
        "basic.auth.user.info": f"{kafka_config.user}:{kafka_config.password}",
    })


@dataclass
class _Variant:
    """One row of the serde matrix. Carries everything the test
    needs to (a) build a producer-side serde, (b) configure the
    ``KafkaStreamSrc`` data_format + schema, (c) describe the
    expected output columns."""

    id: str
    data_format: Any
    schema_loader: Any  # () -> str
    messages: Any  # (n) -> list[dict]
    sr: str | None  # None | "confluent"
    columns: set[str]
    build_producer_serde: Any  # (kafka_config, working_dir) -> serde


def _producer_no_sr_avro(_kafka_config, _working_dir):
    return AvroKafkaSerde(avro_schema=_avro_schema())


def _producer_no_sr_json(_kafka_config, _working_dir):
    return JsonKafkaSerde(json_schema=_json_schema())


def _producer_no_sr_protobuf(_kafka_config, working_dir):
    return ProtobufKafkaSerde(
        protobuf_schema=_proto_schema(),
        message_name=PROTOBUF_MESSAGE_NAME,
        working_dir=str(working_dir),
    )


def _producer_confluent_avro(kafka_config, _working_dir):
    return AvroSrKafkaSerde(
        schema_registry_client=_build_confluent_sr_client(kafka_config),
        avro_schema=_avro_schema(),
    )


def _producer_confluent_json(kafka_config, _working_dir):
    return JsonSrKafkaSerde(
        schema_registry_client=_build_confluent_sr_client(kafka_config),
        json_schema=_json_schema(),
    )


def _producer_confluent_protobuf(kafka_config, working_dir):
    return ProtobufSrKafkaSerde(
        serializer_registry_client=_build_confluent_sr_client(kafka_config),
        deserializer_registry_client=_build_confluent_sr_client(kafka_config),
        protobuf_schema=_proto_schema(),
        message_name=PROTOBUF_MESSAGE_NAME,
        working_dir=str(working_dir),
    )


_VARIANTS = [
    _Variant(
        id="no_sr_json",
        data_format="json",
        schema_loader=_json_schema,
        messages=_json_messages,
        sr=None,
        columns={"name", "favorite_number", "favorite_color"},
        build_producer_serde=_producer_no_sr_json,
    ),
    _Variant(
        id="no_sr_avro",
        data_format="avro",
        schema_loader=_avro_schema,
        messages=_avro_messages,
        sr=None,
        columns={"name", "favorite_number", "favorite_color"},
        build_producer_serde=_producer_no_sr_avro,
    ),
    _Variant(
        id="no_sr_protobuf",
        data_format=Protobuf(message_name=PROTOBUF_MESSAGE_NAME),
        schema_loader=_proto_schema,
        messages=_proto_messages,
        sr=None,
        columns={"name", "id", "email"},
        build_producer_serde=_producer_no_sr_protobuf,
    ),
    _Variant(
        id="confluent_sr_json",
        data_format="json",
        schema_loader=_json_schema,
        messages=_json_messages,
        sr="confluent",
        columns={"name", "favorite_number", "favorite_color"},
        build_producer_serde=_producer_confluent_json,
    ),
    _Variant(
        id="confluent_sr_avro",
        data_format="avro",
        schema_loader=_avro_schema,
        messages=_avro_messages,
        sr="confluent",
        columns={"name", "favorite_number", "favorite_color"},
        build_producer_serde=_producer_confluent_avro,
    ),
    _Variant(
        id="confluent_sr_protobuf",
        data_format=Protobuf(message_name=PROTOBUF_MESSAGE_NAME),
        schema_loader=_proto_schema,
        messages=_proto_messages,
        sr="confluent",
        columns={"name", "id", "email"},
        build_producer_serde=_producer_confluent_protobuf,
    ),
]


def _kafka_creds(kafka_config) -> UserPassword:
    return UserPassword(
        user=Secret.lit(kafka_config.user),
        password=Secret.lit(kafka_config.password),
    )


def _confluent_sr(kafka_config) -> ConfluentSchemaRegistry:
    return ConfluentSchemaRegistry(
        url=kafka_config.schema_registry_url,
        credentials=_kafka_creds(kafka_config),
    )


def _make_src(
    *,
    kafka_config,
    topic: str,
    variant: _Variant,
    group_id: str,
    src_cfg: dict | None = None,
) -> tuple[KafkaStreamSrcConn, KafkaStreamSrc]:
    conn = KafkaStreamSrcConn(
        servers=kafka_config.bootstrap_servers,
        credentials=_kafka_creds(kafka_config),
        group_id=group_id,
        schema_registry=(
            _confluent_sr(kafka_config) if variant.sr == "confluent" else None
        ),
    )
    src = KafkaStreamSrc(
        topic=topic,
        data_format=variant.data_format,
        schema=variant.schema_loader(),
        src_cfg=src_cfg,
    )
    return conn, src


# ---------------------------------------------------------------------------
# Plugin thread runner
# ---------------------------------------------------------------------------


@contextmanager
def _patched_sigterm():
    """``KafkaStreamSrcPlugin._setup_sigterm_handler`` calls
    ``signal.signal`` which only works on the main thread; the
    test drives the plugin on a worker. The implementation
    already no-ops on ``ValueError``, but patching it out keeps
    the spurious warning out of the test log."""
    with patch(
        "tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler",
    ):
        yield


def _run_plugin_in_thread(
    ctx: _KafkaCtx,
    conn: KafkaStreamSrcConn,
    src: KafkaStreamSrc,
    timeout_s: float = 60.0,
) -> None:
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_sigterm():
                KafkaStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)

    t = threading.Thread(target=_go, name="kafka-stream-plugin", daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        pytest.fail(
            f"KafkaStreamSrcPlugin.run did not exit within {timeout_s}s",
        )
    if err:
        raise err[0]


def _all_staged_files(ctx: _KafkaCtx) -> list[Path]:
    files: list[Path] = []
    for sm in ctx.staged:
        if not sm.committed:
            continue
        for table_files in sm.manifest:
            files.extend(Path(p) for p in table_files)
    return files


# ---------------------------------------------------------------------------
# Serde-matrix happy path: every variant round-trips through the plugin
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variant", _VARIANTS, ids=lambda v: v.id)
def test_plugin_round_trips_each_serde_variant(
    variant, fresh_kafka_topic, kafka_config, tmp_path,
):
    work = tmp_path / "work"
    work.mkdir()
    proto_work = tmp_path / "proto"
    proto_work.mkdir()
    ctx = _KafkaCtx(work, active_polls=12)

    serde = variant.build_producer_serde(kafka_config, proto_work)
    msgs = variant.messages(8)
    _produce(kafka_config, fresh_kafka_topic, serde, msgs)

    conn, src = _make_src(
        kafka_config=kafka_config,
        topic=fresh_kafka_topic,
        variant=variant,
        group_id=f"g_{uuid.uuid4().hex[:8]}",
        src_cfg={
            "kafka.consume_timeout_seconds": 2,
            "kafka.time_rollover_seconds": 1,
            "kafka.messages_rollover": 8,
        },
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    files = _all_staged_files(ctx)
    assert files, f"no committed manifest files produced for {variant.id}"
    df = pl.concat([pl.from_arrow(pq.read_table(p)) for p in files])
    assert variant.columns.issubset(set(df.columns)), (
        f"expected columns {variant.columns} <= {set(df.columns)} "
        f"for {variant.id}"
    )
    assert df.height == len(msgs), (
        f"{variant.id}: expected {len(msgs)} rows, got {df.height}"
    )


# ---------------------------------------------------------------------------
# Concrete plugin-mechanic coverage (no-SR JSON baseline)
# ---------------------------------------------------------------------------


def test_plugin_output_table_is_topic(
    fresh_kafka_topic, kafka_config, tmp_path,
):
    """``output_table()`` is the topic name; the staged manifest is
    positional (slot 0 == that single output table)."""
    work = tmp_path / "work"
    work.mkdir()
    ctx = _KafkaCtx(work, active_polls=12)

    variant = next(v for v in _VARIANTS if v.id == "no_sr_json")
    serde = variant.build_producer_serde(kafka_config, tmp_path)
    _produce(kafka_config, fresh_kafka_topic, serde, variant.messages(3))

    conn, src = _make_src(
        kafka_config=kafka_config,
        topic=fresh_kafka_topic,
        variant=variant,
        group_id=f"g_{uuid.uuid4().hex[:8]}",
        src_cfg={
            "kafka.consume_timeout_seconds": 2,
            "kafka.time_rollover_seconds": 1,
            "kafka.messages_rollover": 3,
        },
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    # The topic -> single output table mapping now lives on the src
    # (the positional manifest carries no table name); slot 0 is that
    # output table.
    assert src.output_table() == fresh_kafka_topic
    staged = [sm for sm in ctx.staged if sm.committed and any(sm.manifest)]
    assert staged, "expected a committed batch with files for the topic"


def test_plugin_messages_rollover_creates_multiple_parquet_batches(
    fresh_kafka_topic, kafka_config, tmp_path,
):
    """With ``messages_rollover=N`` and ``2N`` messages produced,
    the plugin should stage at least two committed batches."""
    work = tmp_path / "work"
    work.mkdir()
    ctx = _KafkaCtx(work, active_polls=20)

    variant = next(v for v in _VARIANTS if v.id == "no_sr_json")
    serde = variant.build_producer_serde(kafka_config, tmp_path)
    _produce(kafka_config, fresh_kafka_topic, serde, variant.messages(8))

    conn, src = _make_src(
        kafka_config=kafka_config,
        topic=fresh_kafka_topic,
        variant=variant,
        group_id=f"g_{uuid.uuid4().hex[:8]}",
        src_cfg={
            "kafka.consume_timeout_seconds": 2,
            "kafka.time_rollover_seconds": 1,
            "kafka.messages_rollover": 4,
        },
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    committed = [sm for sm in ctx.staged if sm.committed]
    assert len(committed) >= 2, (
        f"messages_rollover=4 over 8 messages should produce >=2 batches, "
        f"got {len(committed)}"
    )


def test_plugin_schema_change_rollover_segregates_buffers(
    fresh_kafka_topic, kafka_config, tmp_path,
):
    """No-SR JSON keys the buffer on the sorted tuple of message
    field names. Producing two batches with incompatible field
    sets should yield (a) two distinct staged batches, (b) each
    batch's parquet file containing only its own field set."""
    work = tmp_path / "work"
    work.mkdir()
    ctx = _KafkaCtx(work, active_polls=20)

    serde = JsonKafkaSerde(json_schema=None)  # no schema enforcement
    # Two incompatible shapes -- different top-level field names.
    batch_a = [{"name": "alice", "age": 1}, {"name": "bob", "age": 2}]
    batch_b = [{"name": "carol", "city": "NYC"}]
    _produce(kafka_config, fresh_kafka_topic, serde, batch_a + batch_b)

    conn, src = _make_src(
        kafka_config=kafka_config,
        topic=fresh_kafka_topic,
        variant=_Variant(
            id="no_sr_json_no_schema",
            data_format="json",
            schema_loader=lambda: "",
            messages=lambda _n: [],
            sr=None,
            columns=set(),
            build_producer_serde=lambda *_a, **_kw: serde,
        ),
        group_id=f"g_{uuid.uuid4().hex[:8]}",
        src_cfg={
            "kafka.consume_timeout_seconds": 2,
            "kafka.time_rollover_seconds": 1,
        },
    )

    _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)

    committed = [sm for sm in ctx.staged if sm.committed]
    assert len(committed) >= 2, (
        f"schema change should produce >=2 batches, got {len(committed)}"
    )
    # Each parquet file should contain exactly one of the two
    # shape families (no cross-contamination).
    file_shape_pairs = []
    for sm in committed:
        for table_files in sm.manifest:
            for p in table_files:
                cols = set(pl.from_arrow(pq.read_table(p)).columns)
                file_shape_pairs.append(cols)
    assert {"name", "age"} in file_shape_pairs
    assert {"name", "city"} in file_shape_pairs
