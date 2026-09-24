#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._dictionary import (
    LargeColumn,
    RawSchema,
    SchemaVersion,
    build_pa_schema,
    raw_schema_hash,
)
from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow
from tabsdatak.conn.common.sql.cdc._model import TrackedTable
from tabsdatak.conn.common.sql.cdc.dialect import LargeColumnKind
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

INITIAL_SCHEMA_VERSION = 0

LargeColumnClassifier = Callable[[Any], LargeColumnKind | None]


class SchemaRegistry:
    def __init__(
        self,
        type_mapper: Callable[[Any], Any],
        large_column_classifier: LargeColumnClassifier,
        cdc_format: CdcFormat,
    ):
        self._type_mapper = type_mapper
        self._large_column_classifier = large_column_classifier
        self._cdc_format = cdc_format
        self._versions: dict[str, list[SchemaVersion]] = {}
        self._hashes: dict[str, dict[str, SchemaVersion]] = {}

    def large_columns(
        self, columns: tuple[TrackedColumn, ...]
    ) -> tuple[LargeColumn, ...]:
        return tuple(
            LargeColumn(name=column.name, kind=kind)
            for column in columns
            if (kind := self._large_column_classifier(column.satype)) is not None
        )

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
            large_columns=self.large_columns(columns),
        )

        self._versions.setdefault(table, []).append(sv)
        self._hashes.setdefault(table, {})[sch] = sv

        logger.info(
            f"Registered schema version {version_ordinal} for table {table} "
            f"(hash={sch} - columns={len(columns)})"
        )

        return sv

    def register_from_catalog(
        self,
        table: str,
        tracked_table: TrackedTable,
    ) -> SchemaVersion:
        raw_schema = tuple(
            (col.name, repr(col.satype), col.satype) for col in tracked_table.columns
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
            large_columns=self.large_columns(columns),
        )

        self._versions[table] = [sv]
        self._hashes[table] = {sch: sv}

        logger.info(
            f"Registered catalog schema version 0 for table {table} "
            f"(hash={sch}, columns={len(columns)})"
        )

        return sv
