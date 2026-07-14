#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka connector integration tests: drive ``KafkaStreamSrcPlugin.run`` directly
against a tdx-provisioned ``redpanda`` backend with a fabricated
``StreamSrcPluginCtx`` -- unlike ``test__plugin_e2e.py``, which goes through a
running tabsdata instance.

Kafka is a streaming source: ``run`` is an infinite poll+rollover loop, so the
fabricated ``_KafkaCtx`` caps ``active`` after ``active_polls`` reads and the
plugin is driven on a worker thread with a join timeout. The parametrised matrix
covers all six supported wire-format / schema-registry combinations (no-SR +
Confluent SR for JSON, Avro and Protobuf) -- the tdx redpanda backend runs the
Confluent-compatible registry the domain exposes as ``schema_registry_url``. It
is unauthenticated (plaintext, no SASL), so credentials are omitted.

Moved here from ``src/utest`` (where it ran against a self-provisioned broker);
per-function serde/consumer/buffer unit coverage stays in ``src/utest``.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import (
    ConfluentSchemaRegistry,
    KafkaStreamSrc,
    KafkaStreamSrcConn,
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

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.xdist_group("kafka_stream_plugin_integration_seq"),
]

_SCHEMAS_DIR = (
    Path(__file__).parents[4] / "resources/testi_tabsdatak/conn/kafka/schemas"
)
PROTOBUF_MESSAGE_NAME = "PersonV1"


# ---------------------------------------------------------------------------
# tdx redpanda backend -> the `kafka_config`-shaped view the helpers consume
# ---------------------------------------------------------------------------


@dataclass
class _KafkaConfig:
    bootstrap_servers: str
    user: str
    password: str
    schema_registry_url: str | None


def _kafka_config(domain: RedpandaDomain) -> _KafkaConfig:
    return _KafkaConfig(
        bootstrap_servers=f"{domain.host}:{domain.port}",
        user=domain.user or "",
        password=domain.password or "",
        schema_registry_url=domain.schema_registry_url,
    )


# ---------------------------------------------------------------------------
# Fabricated StreamSrcPluginCtx (the plugin's infinite-loop exit lever)
# ---------------------------------------------------------------------------


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _KafkaCtx(StreamSrcPluginCtx):
    """``active_polls`` caps how many times the plugin reads ``active`` before it
    returns ``False`` -- the test's exit lever for the otherwise-infinite loop."""

    def __init__(self, work_dir: Path, *, active_polls: int) -> None:
        self._work_dir = work_dir
        self._active_left = active_polls
        self._state: dict[str, Any] = {}
        self._tabsdata_owns_state = False
        self.staged: list[_StagedManifest] = []
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

    def set_attr(self, attr, val):
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
            if isinstance(new_state, dict):
                self._state = dict(new_state)
                self._tabsdata_owns_state = True
            else:
                self._tabsdata_owns_state = False
                new_state()


# ---------------------------------------------------------------------------
# Broker helpers (plaintext when the backend is unauthenticated)
# ---------------------------------------------------------------------------


def _admin_config(cfg: _KafkaConfig) -> dict:
    conf = {"bootstrap.servers": cfg.bootstrap_servers}
    if cfg.user:
        conf.update({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": cfg.user,
            "sasl.password": cfg.password,
        })
    return conf


def _create_topic(cfg: _KafkaConfig, topic: str) -> None:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient(_admin_config(cfg))
    for _t, fut in admin.create_topics([
        NewTopic(topic, num_partitions=1, replication_factor=1)
    ]).items():
        try:
            fut.result(timeout=16)
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def _delete_topic(cfg: _KafkaConfig, topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(_admin_config(cfg))
    for _t, fut in admin.delete_topics([topic]).items():
        try:
            fut.result(timeout=16)
        except Exception:  # noqa: BLE001
            pass


def _produce(cfg: _KafkaConfig, topic: str, serde, messages: list[dict]) -> None:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import MessageField, SerializationContext

    producer = Producer(_admin_config(cfg))
    ctx = SerializationContext(topic, MessageField.VALUE)
    serialize = serde.serializer()
    for msg in messages:
        producer.produce(topic, value=serialize(msg, ctx))
        producer.poll(0)
    producer.flush()


@contextmanager
def _fresh_topic(cfg: _KafkaConfig):
    topic = f"td_it_{uuid.uuid4().hex[:12]}"
    _create_topic(cfg, topic)
    try:
        yield topic
    finally:
        _delete_topic(cfg, topic)


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
    return _avro_messages(n)


def _proto_messages(n: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "id": i, "email": f"name_{i}@provider.com"}
        for i in range(1, n + 1)
    ]


def _build_confluent_sr_client(cfg: _KafkaConfig):
    from confluent_kafka.schema_registry import SchemaRegistryClient

    conf = {"url": cfg.schema_registry_url}
    if cfg.user:
        conf["basic.auth.user.info"] = f"{cfg.user}:{cfg.password}"
    return SchemaRegistryClient(conf)


@dataclass
class _Variant:
    id: str
    data_format: Any
    schema_loader: Any  # () -> str
    messages: Any  # (n) -> list[dict]
    sr: str | None  # None | "confluent"
    columns: set[str]
    build_producer_serde: Any  # (cfg, working_dir) -> serde


def _producer_no_sr_avro(_cfg, _working_dir):
    return AvroKafkaSerde(avro_schema=_avro_schema())


def _producer_no_sr_json(_cfg, _working_dir):
    return JsonKafkaSerde(json_schema=_json_schema())


def _producer_no_sr_protobuf(_cfg, working_dir):
    return ProtobufKafkaSerde(
        protobuf_schema=_proto_schema(),
        message_name=PROTOBUF_MESSAGE_NAME,
        working_dir=str(working_dir),
    )


def _producer_confluent_avro(cfg, _working_dir):
    return AvroSrKafkaSerde(
        schema_registry_client=_build_confluent_sr_client(cfg),
        avro_schema=_avro_schema(),
    )


def _producer_confluent_json(cfg, _working_dir):
    return JsonSrKafkaSerde(
        schema_registry_client=_build_confluent_sr_client(cfg),
        json_schema=_json_schema(),
    )


def _producer_confluent_protobuf(cfg, working_dir):
    return ProtobufSrKafkaSerde(
        serializer_registry_client=_build_confluent_sr_client(cfg),
        deserializer_registry_client=_build_confluent_sr_client(cfg),
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


def _variant_params():
    """One parametrize entry per serde variant. The Protobuf-over-Confluent-SR
    variant is xfailed: its deserializer fails against this Redpanda registry with
    "Error parsing message with type 'google.protobuf.FileDescriptorProto'" (a
    protobuf-SR round-trip quirk). Protobuf works no-SR, and SR works for JSON/Avro,
    so the failure is confined to that one intersection; also unit-covered."""
    params = []
    for v in _VARIANTS:
        marks = []
        if v.id == "confluent_sr_protobuf":
            marks = [
                pytest.mark.xfail(
                    reason=(
                        "Protobuf over Confluent SR fails to deserialise against the "
                        "tdx Redpanda registry (FileDescriptorProto parse error); "
                        "tracked in TD-2809"
                    ),
                    strict=False,
                )
            ]
        params.append(pytest.param(v, id=v.id, marks=marks))
    return params


def _kafka_creds(cfg: _KafkaConfig) -> UserPassword | None:
    # tdx redpanda is unauthenticated -> no credentials on the connection.
    if not cfg.user:
        return None
    return UserPassword(user=Secret(cfg.user), password=Secret(cfg.password))


def _confluent_sr(cfg: _KafkaConfig) -> ConfluentSchemaRegistry:
    return ConfluentSchemaRegistry(
        url=cfg.schema_registry_url,
        credentials=_kafka_creds(cfg),
    )


def _make_src(
    *,
    cfg: _KafkaConfig,
    topic: str,
    variant: _Variant,
    group_id: str,
    src_cfg: dict | None = None,
) -> tuple[KafkaStreamSrcConn, KafkaStreamSrc]:
    conn = KafkaStreamSrcConn(
        servers=cfg.bootstrap_servers,
        credentials=_kafka_creds(cfg),
        group_id=group_id,
        schema_registry=(_confluent_sr(cfg) if variant.sr == "confluent" else None),
    )
    src = KafkaStreamSrc(
        topic=topic,
        data_format=variant.data_format,
        schema=variant.schema_loader(),
        src_cfg=src_cfg,
    )
    return conn, src


# ---------------------------------------------------------------------------
# Plugin thread runner (the loop blocks; drive it on a worker with a timeout)
# ---------------------------------------------------------------------------


@contextmanager
def _patched_sigterm():
    # `_setup_sigterm_handler` calls signal.signal, main-thread only; the test
    # drives the plugin on a worker. It no-ops on ValueError anyway; patch it out
    # to keep the warning out of the log.
    with patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler"):
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

    thread = threading.Thread(target=_go, name="kafka-stream-plugin", daemon=True)
    thread.start()
    thread.join(timeout=timeout_s)
    if thread.is_alive():
        pytest.fail(f"KafkaStreamSrcPlugin.run did not exit within {timeout_s}s")
    if err:
        raise err[0]


def _all_staged_files(ctx: _KafkaCtx) -> list[Path]:
    files: list[Path] = []
    for staged in ctx.staged:
        if not staged.committed:
            continue
        for table_files in staged.manifest:
            files.extend(Path(p) for p in table_files)
    return files


# ---------------------------------------------------------------------------
# Serde-matrix happy path: every variant round-trips through the plugin
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variant", _variant_params())
@redpanda_domain("redpanda")
def test_plugin_round_trips_each_serde_variant(
    variant, domain_redpanda: RedpandaDomain, tmp_path: Path
):
    cfg = _kafka_config(domain_redpanda)
    work = tmp_path / "work"
    work.mkdir()
    proto_work = tmp_path / "proto"
    proto_work.mkdir()
    ctx = _KafkaCtx(work, active_polls=12)
    with _fresh_topic(cfg) as topic:
        serde = variant.build_producer_serde(cfg, proto_work)
        msgs = variant.messages(8)
        _produce(cfg, topic, serde, msgs)

        conn, src = _make_src(
            cfg=cfg,
            topic=topic,
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
        f"expected columns {variant.columns} <= {set(df.columns)} for {variant.id}"
    )
    assert df.height == len(msgs), (
        f"{variant.id}: expected {len(msgs)} rows, got {df.height}"
    )


# ---------------------------------------------------------------------------
# Concrete plugin-mechanic coverage (no-SR JSON baseline)
# ---------------------------------------------------------------------------


@redpanda_domain("redpanda")
def test_plugin_output_table_is_topic(domain_redpanda: RedpandaDomain, tmp_path: Path):
    cfg = _kafka_config(domain_redpanda)
    work = tmp_path / "work"
    work.mkdir()
    ctx = _KafkaCtx(work, active_polls=12)
    variant = next(v for v in _VARIANTS if v.id == "no_sr_json")
    with _fresh_topic(cfg) as topic:
        serde = variant.build_producer_serde(cfg, tmp_path)
        _produce(cfg, topic, serde, variant.messages(3))
        conn, src = _make_src(
            cfg=cfg,
            topic=topic,
            variant=variant,
            group_id=f"g_{uuid.uuid4().hex[:8]}",
            src_cfg={
                "kafka.consume_timeout_seconds": 2,
                "kafka.time_rollover_seconds": 1,
                "kafka.messages_rollover": 3,
            },
        )
        _run_plugin_in_thread(ctx, conn, src, timeout_s=60.0)
        assert src.output_table() == topic
    staged = [sm for sm in ctx.staged if sm.committed and any(sm.manifest)]
    assert staged, "expected a committed batch with files for the topic"


@redpanda_domain("redpanda")
def test_plugin_messages_rollover_creates_multiple_parquet_batches(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    cfg = _kafka_config(domain_redpanda)
    work = tmp_path / "work"
    work.mkdir()
    ctx = _KafkaCtx(work, active_polls=20)
    variant = next(v for v in _VARIANTS if v.id == "no_sr_json")
    with _fresh_topic(cfg) as topic:
        serde = variant.build_producer_serde(cfg, tmp_path)
        _produce(cfg, topic, serde, variant.messages(8))
        conn, src = _make_src(
            cfg=cfg,
            topic=topic,
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


@redpanda_domain("redpanda")
def test_plugin_schema_change_rollover_segregates_buffers(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    cfg = _kafka_config(domain_redpanda)
    work = tmp_path / "work"
    work.mkdir()
    ctx = _KafkaCtx(work, active_polls=20)
    serde = JsonKafkaSerde(json_schema=None)  # no schema enforcement
    batch_a = [{"name": "alice", "age": 1}, {"name": "bob", "age": 2}]
    batch_b = [{"name": "carol", "city": "NYC"}]
    variant = _Variant(
        id="no_sr_json_no_schema",
        data_format="json",
        schema_loader=lambda: "",
        messages=lambda _n: [],
        sr=None,
        columns=set(),
        build_producer_serde=lambda *_a, **_kw: serde,
    )
    with _fresh_topic(cfg) as topic:
        _produce(cfg, topic, serde, batch_a + batch_b)
        conn, src = _make_src(
            cfg=cfg,
            topic=topic,
            variant=variant,
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
    file_shapes = []
    for sm in committed:
        for table_files in sm.manifest:
            for p in table_files:
                file_shapes.append(set(pl.from_arrow(pq.read_table(p)).columns))
    assert {"name", "age"} in file_shapes
    assert {"name", "city"} in file_shapes
