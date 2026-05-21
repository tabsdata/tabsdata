"""Schema registry shared across CDC ingesters.

Holds one ordered list of ``SchemaVersion`` per tracked table.
Versions are de-duplicated by their raw-schema hash so a redundant
schema-change event does not allocate a new entry."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import (
    RawSchema,
    SchemaVersion,
    build_pa_schema,
    raw_schema_hash,
)
from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow
from tabsdatak.conn.common.sql.cdc._model import TrackedTable
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

logger = logging.getLogger(__name__)

INITIAL_SCHEMA_VERSION = 0


class SchemaRegistry:
    """Per-ingester registry of ``SchemaVersion`` entries.

    Two ingress paths:
      * ``register_from_log`` -- a DDL signal observed in the
        change stream; the raw types come from the source's
        wire format and are mapped to SQLAlchemy via ``type_mapper``.
      * ``register_from_catalog`` -- the initial snapshot, taken
        from SQLAlchemy's catalog introspection at startup.
    """

    def __init__(
        self,
        type_mapper: Callable[[Any], Any],
        cdc_format: CdcFormat,
    ):
        self._type_mapper = type_mapper
        self._cdc_format = cdc_format
        self._versions: dict[str, list[SchemaVersion]] = {}
        self._hashes: dict[str, dict[str, SchemaVersion]] = {}

    def register_from_log(
        self,
        table: str,
        raw_schema: RawSchema,
    ) -> SchemaVersion:
        sch = raw_schema_hash(raw_schema)

        if (hashes := self._hashes.get(table)) and (sv := hashes.get(sch)):
            return sv

        columns = tuple(
            TrackedColumn(
                name=raw_column_name,
                satype=(sa_type := self._type_mapper(raw_column_type)),
                patype=sqlalchemy_to_arrow(sa_type),
            )
            for raw_column_name, _, raw_column_type in raw_schema
        )
        pa_schema = build_pa_schema(columns, self._cdc_format)
        table_versions = self._versions.get(table)
        version_ordinal = (
            len(table_versions)
            if table_versions is not None
            else INITIAL_SCHEMA_VERSION
        )
        sv = SchemaVersion(
            version=version_ordinal,
            raw=raw_schema,
            hash=sch,
            columns=columns,
            pa_schema=pa_schema,
        )

        self._versions.setdefault(table, []).append(sv)
        self._hashes.setdefault(table, {})[sch] = sv

        logger.info(
            "Registered schema version %d for table %s (hash=%s, columns=%d)",
            version_ordinal, table, sch, len(columns),
        )

        return sv

    def register_from_catalog(
        self,
        table: str,
        tracked_table: TrackedTable,
    ) -> SchemaVersion:
        raw_schema = tuple(
            (col.name, repr(col.satype), col.satype)
            for col in tracked_table.columns
        )
        sch = raw_schema_hash(raw_schema)

        columns = tuple(
            TrackedColumn(
                name=catalog_column.name,
                satype=catalog_column.satype,
                patype=catalog_column.patype,
                oname=catalog_column.oname,
                sa_name=catalog_column.sa_name,
                sa_oname=catalog_column.sa_oname,
            )
            for catalog_column in tracked_table.columns
        )
        pa_schema = build_pa_schema(columns, self._cdc_format)
        sv = SchemaVersion(
            version=INITIAL_SCHEMA_VERSION,
            raw=raw_schema,
            hash=sch,
            columns=columns,
            pa_schema=pa_schema,
        )

        self._versions[table] = [sv]
        self._hashes[table] = {sch: sv}

        logger.info(
            "Registered catalog schema version 0 for table %s (hash=%s, columns=%d)",
            table, sch, len(columns),
        )

        return sv
