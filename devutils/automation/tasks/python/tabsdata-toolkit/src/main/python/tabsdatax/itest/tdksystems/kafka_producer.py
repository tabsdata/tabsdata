#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka / Redpanda producer for loading a model into a kafka domain.

Model → topics: one topic per entity (table), named ``<domain>.<entity>``;
each generated row becomes one message. ``--ser`` selects the wire encoding —
``json`` | ``avro`` | ``protobuf`` — and the matching schema for every entity
is written to the (required) ``--schemas DIR`` as ``<entity>.schema.json`` /
``<entity>.avsc`` / ``<entity>.proto``.

Without ``--use-schema-registry`` each message carries the bare serialized
payload (plain JSON / single-object Avro / protobuf bytes). With it, the
schema is registered in the Redpanda schema registry and messages are
produced in Confluent wire format (magic byte + schema id) via the registry
serializers.

Heavy deps (``confluent-kafka`` and, for those encodings, ``fastavro`` /
``protobuf``) are imported lazily so the rest of tdx runs without them;
install the ``kafka`` extra (``pip install 'tabsdata-toolkit[kafka]'``) to use this.

The schema-derivation and serialization helpers are pure and unit-tested; the
admin/produce/registry calls require a live broker + registry.
"""

from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Callable

from .emit import Column, _is_decimal, _is_int, _norm
from .errors import DatagenError

SER_FORMATS = ("json", "avro", "protobuf")
_SCHEMA_EXT = {"json": "schema.json", "avro": "avsc", "protobuf": "proto"}


# ---------------------------------------------------------------------------
# Type classification — one knob feeding all three schema mappers + coercion.
# The generated json rows are already type-erased to JSON natives (ints,
# floats, bools, ISO-date strings), so every temporal/decimal type collapses
# to the JSON representation actually present in the data.
# ---------------------------------------------------------------------------


def _kind_of(sql_type: str) -> str:
    """Return one of: int | long | bool | float | double | string."""
    t = _norm(sql_type)
    width = _is_int(t)
    if width is not None:
        return "long" if width == "int64" else "int"
    if t in ("boolean", "bool"):
        return "bool"
    if t in ("float", "real"):
        return "float"
    if t == "double":
        return "double"
    if _is_decimal(t):
        return "double"  # json data carries decimals as float
    return "string"  # varchar/char/text + date/time/datetime (ISO str)


def _safe_name(name: str) -> str:
    """A valid Avro/Protobuf identifier derived from a table name."""
    s = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not s or s[0].isdigit():
        s = "_" + s
    return s


# ---------------------------------------------------------------------------
# Schema generation (pure)
# ---------------------------------------------------------------------------

_AVRO = {
    "int": "int",
    "long": "long",
    "bool": "boolean",
    "float": "float",
    "double": "double",
    "string": "string",
}
_JSON = {
    "int": "integer",
    "long": "integer",
    "bool": "boolean",
    "float": "number",
    "double": "number",
    "string": "string",
}
_PROTO = {
    "int": "int32",
    "long": "int64",
    "bool": "bool",
    "float": "float",
    "double": "double",
    "string": "string",
}


def avro_schema(table: str, columns: list[Column]) -> dict:
    fields = []
    for c in columns:
        at = _AVRO[_kind_of(c.sql_type)]
        if c.nullable:
            fields.append({"name": c.name, "type": ["null", at], "default": None})
        else:
            fields.append({"name": c.name, "type": at})
    return {"type": "record", "name": _safe_name(table), "fields": fields}


def json_schema(table: str, columns: list[Column]) -> dict:
    props: dict = {}
    required: list[str] = []
    for c in columns:
        jt = _JSON[_kind_of(c.sql_type)]
        props[c.name] = {"type": [jt, "null"]} if c.nullable else {"type": jt}
        if not c.nullable:
            required.append(c.name)
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": table,
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": False,
    }


def proto_schema(table: str, columns: list[Column]) -> str:
    lines = ['syntax = "proto3";', "", f"message {_safe_name(table)} {{"]
    for i, c in enumerate(columns, start=1):
        lines.append(f"  {_PROTO[_kind_of(c.sql_type)]} {c.name} = {i};")
    lines.append("}")
    return "\n".join(lines) + "\n"


def schema_text(table: str, columns: list[Column], ser: str) -> str:
    """The schema file body for `ser` (used by write_schema_file + tests)."""
    if ser == "json":
        return json.dumps(json_schema(table, columns), indent=2) + "\n"
    if ser == "avro":
        return json.dumps(avro_schema(table, columns), indent=2) + "\n"
    if ser == "protobuf":
        return proto_schema(table, columns)
    raise DatagenError(f"unknown --ser {ser!r}; expected one of {SER_FORMATS}")


def write_schema_file(
    schemas_dir: Path, table: str, columns: list[Column], ser: str
) -> Path:
    path = Path(schemas_dir) / f"{table}.{_SCHEMA_EXT[ser]}"
    path.write_text(schema_text(table, columns, ser))
    return path


def topic_name(domain: str, table: str) -> str:
    """One topic per entity, namespaced by the domain topic name."""
    return f"{domain}.{table}" if domain else table


# ---------------------------------------------------------------------------
# Lazy dependency loading
# ---------------------------------------------------------------------------


def _require(mod: str, extra_hint: str):
    try:
        return __import__(mod)
    except ImportError as exc:
        raise DatagenError(
            f"{mod} is required for kafka loads ({extra_hint}). "
            f"Install the kafka extra: pip install 'tabsdata-toolkit[kafka]'"
        ) from exc


# ---------------------------------------------------------------------------
# Plain (no-registry) serializers — return bytes for one row
# ---------------------------------------------------------------------------


def _plain_json_serializer(table, columns) -> Callable[[dict], bytes]:
    return lambda row: json.dumps(row, default=str, separators=(",", ":")).encode()


def _plain_avro_serializer(table, columns) -> Callable[[dict], bytes]:
    fastavro = _require("fastavro", "avro encoding")
    schema = fastavro.parse_schema(avro_schema(table, columns))

    def ser(row: dict) -> bytes:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, _avro_record(row, columns))
        return buf.getvalue()

    return ser


def _avro_record(row: dict, columns: list[Column]) -> dict:
    """Project a row onto the avro field set (nullable unions accept None)."""
    return {c.name: row.get(c.name) for c in columns}


def _plain_protobuf_serializer(table, columns) -> Callable[[dict], bytes]:
    msg_cls = _dynamic_proto_class(table, columns)

    def ser(row: dict) -> bytes:
        msg = msg_cls()
        for c in columns:
            v = row.get(c.name)
            if v is None:
                continue
            setattr(msg, c.name, _proto_coerce(v, c.sql_type))
        return msg.SerializeToString()

    return ser


def _proto_coerce(v, sql_type):
    k = _kind_of(sql_type)
    if k in ("int", "long"):
        return int(v)
    if k == "bool":
        return bool(v)
    if k in ("float", "double"):
        return float(v)
    return str(v)


def _dynamic_proto_class(table: str, columns: list[Column]):
    """Build a protobuf message class from columns at runtime (no protoc)."""
    _require("google", "protobuf encoding")
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    _PB = {
        "int": descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
        "long": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "bool": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
        "float": descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT,
        "double": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
        "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    }
    msg_name = _safe_name(table)
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = f"{msg_name}.proto"
    fdp.syntax = "proto3"
    desc = fdp.message_type.add()
    desc.name = msg_name
    for i, c in enumerate(columns, start=1):
        f = desc.field.add()
        f.name = c.name
        f.number = i
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        f.type = _PB[_kind_of(c.sql_type)]
    pool = descriptor_pool.DescriptorPool()
    file_desc = pool.Add(fdp)
    msg_desc = file_desc.message_types_by_name[msg_name]
    try:  # protobuf >= 3.20
        return message_factory.GetMessageClass(msg_desc)
    except AttributeError:  # older protobuf
        return message_factory.MessageFactory(pool).GetPrototype(msg_desc)


_PLAIN_SERIALIZERS = {
    "json": _plain_json_serializer,
    "avro": _plain_avro_serializer,
    "protobuf": _plain_protobuf_serializer,
}


# ---------------------------------------------------------------------------
# Schema-registry serializers (Confluent wire format)
# ---------------------------------------------------------------------------


def _registry_serializer(ser: str, table, columns, sr_client):
    """Return a callable (row, ctx) -> bytes using the Confluent serializers,
    which register the schema and prepend the magic byte + schema id."""
    sr = __import__("confluent_kafka.schema_registry", fromlist=["__name__"])  # noqa: F841
    if ser == "json":
        from confluent_kafka.schema_registry.json_schema import JSONSerializer

        schema_str = json.dumps(json_schema(table, columns))
        s = JSONSerializer(schema_str, sr_client, conf={"auto.register.schemas": True})
        return lambda row, ctx: s(row, ctx)
    if ser == "avro":
        from confluent_kafka.schema_registry.avro import AvroSerializer

        schema_str = json.dumps(avro_schema(table, columns))
        s = AvroSerializer(sr_client, schema_str, conf={"auto.register.schemas": True})
        return lambda row, ctx: s(_avro_record(row, columns), ctx)
    if ser == "protobuf":
        from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

        msg_cls = _dynamic_proto_class(table, columns)
        s = ProtobufSerializer(
            msg_cls,
            sr_client,
            conf={"auto.register.schemas": True, "use.deprecated.format": False},
        )

        def ser_fn(row, ctx):
            msg = msg_cls()
            for c in columns:
                v = row.get(c.name)
                if v is not None:
                    setattr(msg, c.name, _proto_coerce(v, c.sql_type))
            return s(msg, ctx)

        return ser_fn
    raise DatagenError(f"unknown --ser {ser!r}; expected one of {SER_FORMATS}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def produce_model(
    *,
    structured: list[tuple[str, list[Column], list[dict]]],
    brokers: str,
    domain: str,
    ser: str,
    schemas_dir: str | Path,
    use_schema_registry: bool = False,
    registry_url: str | None = None,
) -> dict[str, int]:
    """Write schema files, create one topic per entity, and produce rows.

    Returns ``{topic: produced_row_count}``. Requires a live broker (and, with
    `use_schema_registry`, a reachable registry)."""
    if ser not in SER_FORMATS:
        raise DatagenError(f"--ser must be one of {SER_FORMATS} (got {ser!r})")
    if not brokers:
        raise DatagenError("no kafka brokers (domain meta has no KAFKA_BROKERS)")
    schemas_dir = Path(schemas_dir)
    schemas_dir.mkdir(parents=True, exist_ok=True)

    # 1. Schema files (always written, regardless of registry use).
    for table, cols, _rows in structured:
        write_schema_file(schemas_dir, table, cols, ser)

    # 2. Broker clients + topics (one per entity).
    confluent = _require("confluent_kafka", "kafka produce")
    Producer = confluent.Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    producer = Producer({"bootstrap.servers": brokers})
    admin = AdminClient({"bootstrap.servers": brokers})
    topics = {table: topic_name(domain, table) for table, _, _ in structured}
    _create_topics(admin, NewTopic, list(topics.values()))

    # 3. Serializers.
    sr_client = None
    if use_schema_registry:
        if not registry_url:
            raise DatagenError(
                "--use-schema-registry needs a schema registry URL, but the "
                "domain meta has no SCHEMA_REGISTRY_URL (re-create the redpanda "
                "system so it publishes the registry)"
            )
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr_client = SchemaRegistryClient({"url": registry_url})
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

    # 4. Produce.
    counts: dict[str, int] = {}
    for table, cols, rows in structured:
        topic = topics[table]
        if use_schema_registry:
            serialize = _registry_serializer(ser, table, cols, sr_client)
            ctx = SerializationContext(topic, MessageField.VALUE)

            def encode(row, _s=serialize, _c=ctx):
                return _s(row, _c)

        else:
            plain = _PLAIN_SERIALIZERS[ser](table, cols)
            encode = plain
        for row in rows:
            producer.produce(topic, value=encode(row), key=_row_key(row, cols))
        producer.flush()
        counts[topic] = len(rows)
    return counts


def _row_key(row: dict, columns: list[Column]) -> bytes | None:
    """Use the first column's value as the partition key (often the id)."""
    if not columns:
        return None
    v = row.get(columns[0].name)
    return None if v is None else str(v).encode()


def _create_topics(admin, NewTopic, names: list[str]) -> None:
    futures = admin.create_topics([
        NewTopic(n, num_partitions=1, replication_factor=1) for n in names
    ])
    for name, fut in futures.items():
        try:
            fut.result()
        except Exception as exc:  # noqa: BLE001 — tolerate "already exists"
            if "already exists" not in str(exc).lower():
                raise DatagenError(f"could not create topic {name}: {exc}")
