#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka stream-source-plugin integration tests using fake context"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.kafka.backend import Broker, broker, confluent_sr, src_conn

from tabsdatak.conn.kafka import KafkaStreamSrc, KafkaStreamSrcConn, Protobuf

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes import (
    AvroKafkaSerde,
    AvroSrKafkaSerde,
    JsonKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufKafkaSerde,
    ProtobufSrKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_plugin import KafkaStreamSrcPlugin
from tabsdatak.spi import StreamSrcPluginCtx

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.xdist_group("kafka_stream_plugin_integration_seq"),
]

SCHEMAS = Path(__file__).parents[4] / "resources/testi_tabsdatak/conn/kafka/schemas"
PROTOBUF_MESSAGE = "PersonV1"
PLUGIN_TIMEOUT_SECONDS = 60.0
ROLLOVER_CFG = {"kafka.consume_timeout_seconds": 2, "kafka.time_rollover_seconds": 1}


@dataclass
class _StagedManifest:
    manifest: list
    committed: bool = False


class _KafkaCtx(StreamSrcPluginCtx):
    """``active_polls`` caps how many times the plugin reads ``active`` before it
    returns ``False`` -- the test's exit lever for the otherwise-infinite loop."""

    def __init__(self, work: Path, *, active_polls: int) -> None:
        self._work_dir = work
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


def _avro_schema() -> str:
    return (SCHEMAS / "user_v1.avsc").read_text()


def _json_schema() -> str:
    return (SCHEMAS / "user.json").read_text()


def _proto_schema() -> str:
    return (SCHEMAS / "person_v1.proto").read_text()


def _user_messages(count: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "favorite_number": i, "favorite_color": "blue"}
        for i in range(1, count + 1)
    ]


def _person_messages(count: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "id": i, "email": f"name_{i}@provider.com"}
        for i in range(1, count + 1)
    ]


def _no_sr_avro(_broker: Broker, _work: Path):
    return AvroKafkaSerde(avro_schema=_avro_schema())


def _no_sr_json(_broker: Broker, _work: Path):
    return JsonKafkaSerde(json_schema=_json_schema())


def _no_sr_protobuf(_broker: Broker, work: Path):
    return ProtobufKafkaSerde(
        protobuf_schema=_proto_schema(),
        message_name=PROTOBUF_MESSAGE,
        working_dir=str(work),
    )


def _confluent_avro(sr: Broker, _work: Path):
    return AvroSrKafkaSerde(
        schema_registry_client=sr.schema_registry_client(),
        avro_schema=_avro_schema(),
    )


def _confluent_json(sr: Broker, _work: Path):
    return JsonSrKafkaSerde(
        schema_registry_client=sr.schema_registry_client(),
        json_schema=_json_schema(),
    )


def _confluent_protobuf(sr: Broker, work: Path):
    return ProtobufSrKafkaSerde(
        serializer_registry_client=sr.schema_registry_client(),
        deserializer_registry_client=sr.schema_registry_client(),
        protobuf_schema=_proto_schema(),
        message_name=PROTOBUF_MESSAGE,
        working_dir=str(work),
    )


@dataclass
class _Variant:
    id: str
    data_format: Any
    schema_loader: Any  # () -> str
    messages: Any  # (count) -> list[dict]
    sr: bool
    columns: set[str]
    producer_serde: Any  # (broker, work) -> serde


USER_COLUMNS = {"name", "favorite_number", "favorite_color"}
PERSON_COLUMNS = {"name", "id", "email"}

VARIANTS = [
    _Variant(
        id="no_sr_json",
        data_format="json",
        schema_loader=_json_schema,
        messages=_user_messages,
        sr=False,
        columns=USER_COLUMNS,
        producer_serde=_no_sr_json,
    ),
    _Variant(
        id="no_sr_avro",
        data_format="avro",
        schema_loader=_avro_schema,
        messages=_user_messages,
        sr=False,
        columns=USER_COLUMNS,
        producer_serde=_no_sr_avro,
    ),
    _Variant(
        id="no_sr_protobuf",
        data_format=Protobuf(message_name=PROTOBUF_MESSAGE),
        schema_loader=_proto_schema,
        messages=_person_messages,
        sr=False,
        columns=PERSON_COLUMNS,
        producer_serde=_no_sr_protobuf,
    ),
    _Variant(
        id="confluent_sr_json",
        data_format="json",
        schema_loader=_json_schema,
        messages=_user_messages,
        sr=True,
        columns=USER_COLUMNS,
        producer_serde=_confluent_json,
    ),
    _Variant(
        id="confluent_sr_avro",
        data_format="avro",
        schema_loader=_avro_schema,
        messages=_user_messages,
        sr=True,
        columns=USER_COLUMNS,
        producer_serde=_confluent_avro,
    ),
    _Variant(
        id="confluent_sr_protobuf",
        data_format=Protobuf(message_name=PROTOBUF_MESSAGE),
        schema_loader=_proto_schema,
        messages=_person_messages,
        sr=True,
        columns=PERSON_COLUMNS,
        producer_serde=_confluent_protobuf,
    ),
]

JSON_VARIANT = next(v for v in VARIANTS if v.id == "no_sr_json")


def _variant_params():
    """One parametrize entry per serde variant. The Protobuf-over-Confluent-SR
    variant is xfailed: its deserializer fails against this Redpanda registry with
    "Error parsing message with type 'google.protobuf.FileDescriptorProto'" (a
    protobuf-SR round-trip quirk). Protobuf works no-SR, and SR works for JSON/Avro,
    so the failure is confined to that one intersection; also unit-covered."""
    params = []
    for variant in VARIANTS:
        marks = []
        if variant.id == "confluent_sr_protobuf":
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
        params.append(pytest.param(variant, id=variant.id, marks=marks))
    return params


def _stream_src(
    domain: RedpandaDomain, *, topic: str, variant: _Variant, src_cfg: dict
) -> tuple[KafkaStreamSrcConn, KafkaStreamSrc]:
    conn = src_conn(
        domain, schema_registry=confluent_sr(domain) if variant.sr else None
    )
    src = KafkaStreamSrc(
        topic=topic,
        data_format=variant.data_format,
        schema=variant.schema_loader(),
        src_cfg=src_cfg,
    )
    return conn, src


@contextmanager
def _patched_sigterm():
    # `_setup_sigterm_handler` calls signal.signal, main-thread only; the test drives
    # the plugin on a worker. It no-ops on ValueError anyway; patch it out to keep the
    # warning out of the log.
    with patch("tabsdatak.conn.kafka._stream_plugin._setup_sigterm_handler"):
        yield


def _run_plugin(ctx: _KafkaCtx, conn: KafkaStreamSrcConn, src: KafkaStreamSrc) -> None:
    """``run`` is an infinite poll+rollover loop, so it is driven on a worker thread
    and joined with a timeout; ``ctx.active`` is what ends it."""
    err: list[BaseException] = []

    def _go():
        try:
            with _patched_sigterm():
                KafkaStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)

    thread = threading.Thread(target=_go, name="kafka-stream-plugin", daemon=True)
    thread.start()
    thread.join(timeout=PLUGIN_TIMEOUT_SECONDS)
    if thread.is_alive():
        pytest.fail(f"plugin run did not exit within {PLUGIN_TIMEOUT_SECONDS}s")
    if err:
        raise err[0]


def _committed(ctx: _KafkaCtx) -> list[_StagedManifest]:
    return [staged for staged in ctx.staged if staged.committed]


def _staged_frames(ctx: _KafkaCtx) -> list[pl.DataFrame]:
    return [
        pl.from_arrow(pq.read_table(file))
        for staged in _committed(ctx)
        for table_files in staged.manifest
        for file in table_files
    ]


@pytest.mark.parametrize("variant", _variant_params())
@redpanda_domain("redpanda")
def test_stream_src_plugin_round_trips_each_serde_variant(
    variant: _Variant, domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    messages = variant.messages(8)
    bus = broker(domain)
    ctx = _KafkaCtx(work_dir(tmp_path), active_polls=12)
    with bus.fresh_topic() as topic:
        bus.produce(
            topic, messages, variant.producer_serde(bus, work_dir(tmp_path, "proto"))
        )
        conn, src = _stream_src(
            domain,
            topic=topic,
            variant=variant,
            src_cfg=ROLLOVER_CFG | {"kafka.messages_rollover": len(messages)},
        )
        _run_plugin(ctx, conn, src)

    frames = _staged_frames(ctx)
    assert frames, f"no committed manifest files produced for {variant.id}"
    frame = pl.concat(frames)
    assert variant.columns <= set(frame.columns)
    assert frame.height == len(messages)


@redpanda_domain("redpanda")
def test_stream_src_plugin_output_table_is_topic(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    messages = _user_messages(3)
    bus = broker(domain)
    ctx = _KafkaCtx(work_dir(tmp_path), active_polls=12)
    with bus.fresh_topic() as topic:
        bus.produce(topic, messages, JSON_VARIANT.producer_serde(bus, tmp_path))
        conn, src = _stream_src(
            domain,
            topic=topic,
            variant=JSON_VARIANT,
            src_cfg=ROLLOVER_CFG | {"kafka.messages_rollover": len(messages)},
        )
        _run_plugin(ctx, conn, src)
        assert src.output_table() == topic

    assert [staged for staged in _committed(ctx) if any(staged.manifest)]


@redpanda_domain("redpanda")
def test_stream_src_plugin_messages_rollover_stages_several_batches(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    messages = _user_messages(8)
    bus = broker(domain)
    ctx = _KafkaCtx(work_dir(tmp_path), active_polls=20)
    with bus.fresh_topic() as topic:
        bus.produce(topic, messages, JSON_VARIANT.producer_serde(bus, tmp_path))
        conn, src = _stream_src(
            domain,
            topic=topic,
            variant=JSON_VARIANT,
            src_cfg=ROLLOVER_CFG | {"kafka.messages_rollover": len(messages) // 2},
        )
        _run_plugin(ctx, conn, src)

    assert len(_committed(ctx)) >= 2


@redpanda_domain("redpanda")
def test_stream_src_plugin_schema_change_rollover_segregates_buffers(
    domain_redpanda: RedpandaDomain, tmp_path: Path
):
    domain = domain_redpanda
    ctx = _KafkaCtx(work_dir(tmp_path), active_polls=20)
    # No schema enforcement either side, so the two message shapes reach the plugin
    # as they are and each one closes the buffer the other cannot join.
    unschemad = _Variant(
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
        conn, src = _stream_src(
            domain, topic=topic, variant=unschemad, src_cfg=ROLLOVER_CFG
        )
        _run_plugin(ctx, conn, src)

    assert len(_committed(ctx)) >= 2
    shapes = [set(frame.columns) for frame in _staged_frames(ctx)]
    assert {"name", "age"} in shapes
    assert {"name", "city"} in shapes
