"""Kafka serdes -- wire-format (de)serialisers for the Kafka
stream source.

Three flavours × three formats:

* No schema registry (``_no_sr``) -- ``AvroKafkaSerde`` /
  ``JsonKafkaSerde`` / ``ProtobufKafkaSerde``. Schema lives
  client-side; the buffer's compatibility check uses the
  on-wire field-name subset.
* Confluent SR (``_confluent``) -- ``AvroSrKafkaSerde`` /
  ``JsonSrKafkaSerde`` / ``ProtobufSrKafkaSerde``. The
  Confluent client owns schema-id tagging; compatibility is
  exact id equality.
* AWS Glue SR (``_glue``) -- ``AvroGlueKafkaSerde`` /
  ``JsonGlueKafkaSerde`` / ``ProtobufGlueKafkaSerde``. Glue
  message header carries a 17-byte schema-version id (UUID).

The :func:`build_serde` dispatcher picks the right concrete
serde for a ``(format, schema_registry)`` pair; it's the one
entry point the connector's plugin uses.
"""

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._base import (
    KafkaSerde,
    MsgFormatSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._builder import (
    build_schema_registry_client,
    build_serde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._confluent import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
    SrKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._glue import (
    AvroGlueKafkaSerde,
    GlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._no_sr import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    NonSrKafkaSerde,
    ProtobufKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._protobuf import get_protobuf_class

__all__ = [
    "AvroGlueKafkaSerde",
    "AvroKafkaSerde",
    "AvroSrKafkaSerde",
    "GlueKafkaSerde",
    "JsonGlueKafkaSerde",
    "JsonKafkaSerde",
    "JsonSrKafkaSerde",
    "KafkaSerde",
    "MsgFormatSpec",
    "NonSrKafkaSerde",
    "ProtobufGlueKafkaSerde",
    "ProtobufKafkaSerde",
    "ProtobufSrKafkaSerde",
    "SchemaIdCompatibles",
    "SchemaIdSpec",
    "SrKafkaSerde",
    "build_schema_registry_client",
    "build_serde",
    "get_protobuf_class",
]
