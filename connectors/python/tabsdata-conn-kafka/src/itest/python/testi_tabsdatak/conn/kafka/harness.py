#
# Copyright 2026 Tabsdata Inc.
#

"""Shared harness of the Kafka stream-source-plugin integration tests: the fake
context that ends the plugin's loop, the serde variants and the staged-file readers."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain
from testi_tabsdatak.conn.kafka.backend import Broker, confluent_sr, src_conn

# noinspection PyProtectedMember
from tabsdatak.api import BatchThresholds, StreamThresholds
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

SCHEMAS = Path(__file__).parents[4] / "resources/testi_tabsdatak/conn/kafka/schemas"
PROTOBUF_MESSAGE = "PersonV1"
PLUGIN_TIMEOUT_SECONDS = 60.0
CAPTURE_TIMEOUT_SECONDS = 45.0
STREAM_CFG = {"tabsdata.kafka.poll.max_secs": 2}


def batch_of(rows: int | None = None) -> StreamThresholds:
    """Batches a second apart at most, of `rows` rows when given."""
    return StreamThresholds(
        batch=BatchThresholds(min_secs=1, max_secs=1, max_rows=rows)
    )


@dataclass
class StagedManifest:
    manifest: list
    committed: bool = False


class KafkaCtx(StreamSrcPluginCtx):
    """``active`` holds the plugin's poll loop open until ``until`` holds for what
    has been staged, or the capture deadline passes -- the test's exit lever for the
    otherwise-infinite loop. ``satisfied`` tells the two apart."""

    def __init__(
        self,
        work: Path,
        *,
        until: Callable[[KafkaCtx], bool],
        thresholds: StreamThresholds | None = None,
    ) -> None:
        self._work_dir = work
        self._thresholds = thresholds or batch_of()
        self._until = until
        self._deadline = time.monotonic() + CAPTURE_TIMEOUT_SECONDS
        self.satisfied = False
        self._state: dict[str, Any] = {}
        self._tabsdata_owns_state = False
        self.staged: list[StagedManifest] = []
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
        if self._until(self):
            self.satisfied = True
            return False
        return time.monotonic() < self._deadline

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
    def thresholds(self) -> StreamThresholds:
        return self._thresholds

    def stage(self, tables_files, new_state):
        with self._lock:
            self.staged.append(
                StagedManifest(manifest=list(tables_files), committed=True)
            )
            if isinstance(new_state, dict):
                self._state = dict(new_state)
                self._tabsdata_owns_state = True
            else:
                self._tabsdata_owns_state = False
                new_state()

    def checkpoint(self, new_state):
        with self._lock:
            self._state = dict(new_state)
            self._tabsdata_owns_state = True


def avro_schema() -> str:
    return (SCHEMAS / "user_v1.avsc").read_text()


def json_schema() -> str:
    return (SCHEMAS / "user.json").read_text()


def proto_schema() -> str:
    return (SCHEMAS / "person_v1.proto").read_text()


def user_messages(count: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "favorite_number": i, "favorite_color": "blue"}
        for i in range(1, count + 1)
    ]


def person_messages(count: int) -> list[dict]:
    return [
        {"name": f"name_{i}", "id": i, "email": f"name_{i}@provider.com"}
        for i in range(1, count + 1)
    ]


def no_sr_avro(_broker: Broker, _work: Path):
    return AvroKafkaSerde(avro_schema=avro_schema())


def no_sr_json(_broker: Broker, _work: Path):
    return JsonKafkaSerde(json_schema=json_schema())


def no_sr_protobuf(_broker: Broker, work: Path):
    return ProtobufKafkaSerde(
        protobuf_schema=proto_schema(),
        message_name=PROTOBUF_MESSAGE,
        working_dir=str(work),
    )


def confluent_avro(sr: Broker, _work: Path):
    return AvroSrKafkaSerde(
        schema_registry_client=sr.schema_registry_client(),
        avro_schema=avro_schema(),
    )


def confluent_json(sr: Broker, _work: Path):
    return JsonSrKafkaSerde(
        schema_registry_client=sr.schema_registry_client(),
        json_schema=json_schema(),
    )


def confluent_protobuf(sr: Broker, work: Path):
    return ProtobufSrKafkaSerde(
        serializer_registry_client=sr.schema_registry_client(),
        deserializer_registry_client=sr.schema_registry_client(),
        protobuf_schema=proto_schema(),
        message_name=PROTOBUF_MESSAGE,
        working_dir=str(work),
    )


@dataclass
class Variant:
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
    Variant(
        id="no_sr_json",
        data_format="json",
        schema_loader=json_schema,
        messages=user_messages,
        sr=False,
        columns=USER_COLUMNS,
        producer_serde=no_sr_json,
    ),
    Variant(
        id="no_sr_avro",
        data_format="avro",
        schema_loader=avro_schema,
        messages=user_messages,
        sr=False,
        columns=USER_COLUMNS,
        producer_serde=no_sr_avro,
    ),
    Variant(
        id="no_sr_protobuf",
        data_format=Protobuf(message_name=PROTOBUF_MESSAGE),
        schema_loader=proto_schema,
        messages=person_messages,
        sr=False,
        columns=PERSON_COLUMNS,
        producer_serde=no_sr_protobuf,
    ),
    Variant(
        id="confluent_sr_json",
        data_format="json",
        schema_loader=json_schema,
        messages=user_messages,
        sr=True,
        columns=USER_COLUMNS,
        producer_serde=confluent_json,
    ),
    Variant(
        id="confluent_sr_avro",
        data_format="avro",
        schema_loader=avro_schema,
        messages=user_messages,
        sr=True,
        columns=USER_COLUMNS,
        producer_serde=confluent_avro,
    ),
    Variant(
        id="confluent_sr_protobuf",
        data_format=Protobuf(message_name=PROTOBUF_MESSAGE),
        schema_loader=proto_schema,
        messages=person_messages,
        sr=True,
        columns=PERSON_COLUMNS,
        producer_serde=confluent_protobuf,
    ),
]

JSON_VARIANT = next(v for v in VARIANTS if v.id == "no_sr_json")


def variant_params():
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


def stream_src(
    domain: RedpandaDomain, *, topic: str, variant: Variant, src_cfg: dict
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


class PluginRun:
    """``run`` is an infinite poll + stage loop, so it is driven on a worker thread
    and joined with a timeout; ``ctx.active`` is what ends it."""

    def __init__(self, ctx: KafkaCtx, conn: KafkaStreamSrcConn, src: KafkaStreamSrc):
        self.errors: list[BaseException] = []

        def go():
            try:
                KafkaStreamSrcPlugin().run(ctx=ctx, conn=conn, stream_src=src)
            except BaseException as exc:  # noqa: BLE001
                self.errors.append(exc)

        self.thread = threading.Thread(
            target=go, name="kafka-stream-plugin", daemon=True
        )

    def start(self) -> PluginRun:
        self.thread.start()
        return self

    def join(self) -> None:
        self.thread.join(timeout=PLUGIN_TIMEOUT_SECONDS)
        if self.thread.is_alive():
            pytest.fail(f"plugin run did not exit within {PLUGIN_TIMEOUT_SECONDS}s")
        if self.errors:
            raise self.errors[0]


def run_plugin(ctx: KafkaCtx, conn: KafkaStreamSrcConn, src: KafkaStreamSrc) -> None:
    PluginRun(ctx, conn, src).start().join()


def committed_manifests(ctx: KafkaCtx) -> list[StagedManifest]:
    return [staged for staged in ctx.staged if staged.committed]


def staged_frames(ctx: KafkaCtx) -> list[pl.DataFrame]:
    return [
        pl.from_arrow(pq.read_table(file))
        for staged in committed_manifests(ctx)
        for table_files in staged.manifest
        for file in table_files
    ]


def staged_rows(ctx: KafkaCtx) -> int:
    return sum(frame.height for frame in staged_frames(ctx))


def staged_at_least(count: int) -> Callable[[KafkaCtx], bool]:
    return lambda ctx: staged_rows(ctx) >= count


def staged_names(ctx: KafkaCtx) -> list[str]:
    return sorted(name for frame in staged_frames(ctx) for name in frame["name"])
