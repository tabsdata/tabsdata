#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import datetime, tzinfo
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.elements import quoted_name

from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcOffset,
    TableObject,
    TrackedColumn,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.dialect import (
    CdcIngesterType,
    Dialect,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat, QualifiedTablesSpec
from tabsdatak.spi import StreamStateSpec

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Key under which the CDC offset dict lives inside the per-flow stream
# state, for Tabsdata-owned-offset ingesters (read back via
# ``ctx.stream_state[CDC_OFFSET_STATE_KEY]``).
CDC_OFFSET_STATE_KEY = "cdc.offset"


class CdcIngester(ABC):
    def __init__(self, tables: QualifiedTablesSpec):
        self._tables = tables
        self._tracked_tables: dict[str, TrackedTable] = {}
        self._schema_registry: SchemaRegistry | None = None
        self._server_tz: tzinfo | None = None
        # Resume mark seeded by :meth:`seed_offset` before the stream
        # opens; subclasses retype it to their concrete offset class.
        self._mark: CdcOffset | None = None

    @property
    @abstractmethod
    def ingester_type(self) -> CdcIngesterType: ...

    @property
    def tables(self) -> QualifiedTablesSpec:
        return self._tables

    @property
    def tracked_tables(self) -> dict[str, TrackedTable]:
        return self._tracked_tables

    @property
    def schema_registry(self) -> SchemaRegistry | None:
        return self._schema_registry

    @property
    def server_tz(self) -> tzinfo | None:
        return self._server_tz

    def localize_values(self, values: dict | None) -> dict | None:
        if values is None or self._server_tz is None:
            return values
        for key, value in values.items():
            if isinstance(value, datetime) and value.tzinfo is None:
                values[key] = value.replace(tzinfo=self._server_tz)
        return values

    def open_session(self) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @property
    @abstractmethod
    def dialect(self) -> Dialect: ...

    def resolve_identifier(self, name: quoted_name) -> str:
        if name.quote:
            return str(name)
        return self.dialect.default_case_fold(str(name))

    def gather_table_identity(
        self,
        tablespec: str,
        inspector: sa.engine.reflection.Inspector,
    ) -> TrackedTable:
        try:
            schema, table = self.dialect.parse_qualified_table(tablespec)
        except ValueError as error:
            raise SqlCdcErrorCode.CDC_1.exception(
                cause=error,
                spec=tablespec,
            )

        db_schema = self.resolve_identifier(schema)
        db_table = self.resolve_identifier(table)

        sa_dialect = inspector.bind.dialect
        normalized_schema = sa_dialect.normalize_name(
            quoted_name(db_schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(quoted_name(db_table, quote=False))

        if not inspector.has_table(
            normalized_table,
            schema=normalized_schema,
        ):
            raise SqlCdcErrorCode.CDC_2.exception(
                schema=db_schema,
                table=db_table,
            )

        table_object = TableObject(
            schema=db_schema,
            table=db_table,
        )
        return TrackedTable(
            data_table=table_object,
            cdc_table=table_object,
        )

    def gather_table_structure(
        self,
        tracked_table: TrackedTable,
        inspector: sa.engine.reflection.Inspector,
        before_img_prefix: str | None = None,
    ) -> None:
        sa_dialect = inspector.bind.dialect
        db_schema = tracked_table.data_table.schema
        db_table = tracked_table.data_table.table

        normalized_schema = sa_dialect.normalize_name(
            quoted_name(db_schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(quoted_name(db_table, quote=False))

        sa_columns = inspector.get_columns(
            normalized_table,
            schema=normalized_schema,
        )
        if not sa_columns:
            raise SqlCdcErrorCode.CDC_3.exception(
                schema=db_schema,
                table=db_table,
            )

        with inspector.bind.connect() as connection:
            db_columns = self.dialect.get_catalog_columns(
                connection,
                db_schema,
                db_table,
            )

        if len(db_columns) != len(sa_columns):
            raise SqlCdcErrorCode.CDC_7.exception(
                schema=db_schema,
                table=db_table,
                catalog_count=len(db_columns),
                inspector_count=len(sa_columns),
            )

        for db_column, sa_column in zip(db_columns, sa_columns):
            if db_column.upper() != sa_column["name"].upper():
                raise SqlCdcErrorCode.CDC_8.exception(
                    schema=db_schema,
                    table=db_table,
                    catalog_column=db_column,
                    inspector_column=sa_column["name"],
                )

        columns = []
        for db_column, sa_column in zip(db_columns, sa_columns):
            if self.dialect.is_large_column(sa_column["type"]):
                logger.info(
                    f"Excluding large column '{db_column}' "
                    f"({type(sa_column['type']).__name__}) "
                    f"from {db_schema}.{db_table}"
                )
                continue
            oname = f"{before_img_prefix}{db_column}" if before_img_prefix else None
            columns.append(
                TrackedColumn(
                    name=db_column,
                    satype=sa_column["type"],
                    oname=oname,
                    sa_name=sa_dialect.normalize_name(db_column),
                    sa_oname=(sa_dialect.normalize_name(oname) if oname else None),
                )
            )

        tracked_table.columns = columns
        tracked_table.build_pa_dtypes()
        tracked_table.pk = self.gather_table_pk(
            inspector,
            normalized_table,
            normalized_schema,
            sa_columns,
            db_columns,
        )

    @staticmethod
    def gather_table_pk(
        inspector: sa.engine.reflection.Inspector,
        normalized_table: str,
        normalized_schema: str,
        sa_columns: list[ReflectedColumn],
        db_columns: list[str],
    ) -> list[str]:
        pk_info = inspector.get_pk_constraint(
            table_name=normalized_table,
            schema=normalized_schema,
        )
        inspector_pk = pk_info.get("constrained_columns", []) if pk_info else []
        inspector_to_catalog = {
            sa_col["name"]: db_col for sa_col, db_col in zip(sa_columns, db_columns)
        }
        return [inspector_to_catalog.get(pk, pk) for pk in inspector_pk]

    def init_schema_registry(self, cdc_format: CdcFormat) -> None:
        ingester_type = self.ingester_type

        def cdc_type_mapper(raw_type: Any) -> Any:
            return self.dialect.cdc_to_sa(ingester_type, raw_type)

        self._schema_registry = SchemaRegistry(
            type_mapper=cdc_type_mapper,
            cdc_format=cdc_format,
        )

    def prepare_inspector(self, inspector: sa.engine.reflection.Inspector) -> None:
        pass

    def resolve_tablespecs(self) -> list[str]:
        tablespecs = self._tables
        if isinstance(tablespecs, str):
            tablespecs = [tablespecs]
        return tablespecs

    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        inspector: Inspector = sa.inspect(session)
        self.prepare_inspector(inspector)
        result: dict[str, TrackedTable] = {}

        for tablespec in self.resolve_tablespecs():
            tracked_table = self.gather_table_identity(tablespec, inspector)
            self.gather_table_structure(tracked_table, inspector)
            result[tracked_table.qualified] = tracked_table

        self._tracked_tables = result
        self.init_schema_registry(cdc_format)
        for qualified, tracked_table in result.items():
            self._schema_registry.register_from_catalog(qualified, tracked_table)

        return result

    def harvest_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        inspector: Inspector = sa.inspect(session)
        self.prepare_inspector(inspector)
        result: dict[str, TrackedTable] = {}

        for tablespec in self.resolve_tablespecs():
            tracked_table = self.gather_table_identity(tablespec, inspector)
            result[tracked_table.qualified] = tracked_table

        self._tracked_tables = result
        self.init_schema_registry(cdc_format)

        return result

    @abstractmethod
    def base_offset(self) -> CdcOffset: ...

    @abstractmethod
    def commit_offset(self, offset: CdcOffset) -> None: ...

    @abstractmethod
    def poll_changes(self, session: Any) -> Iterable[CdcChange]: ...

    # ------------------------------------------------------------------
    # Offset ownership: Tabsdata-owned (default) vs external-owned.
    #
    # By default Tabsdata keeps the resume offset -- the run loop
    # persists it (:meth:`commit_state`) and resumes from it
    # (:meth:`resume_offset` -> :meth:`offset_from_state`). Ingesters
    # whose source keeps its own consumer cursor (e.g. a Postgres
    # replication slot) override :meth:`commit_state` to commit there and
    # :meth:`resume_offset` to let the source resume itself.
    # ------------------------------------------------------------------

    def commit_state(self, offset: CdcOffset) -> StreamStateSpec:
        """The :data:`~tabsdatak.spi.StreamStateSpec` to hand
        ``ctx.stage`` for this batch. Default: a
        :data:`~tabsdatak.spi.StreamStateDict` that Tabsdata persists,
        keyed under :data:`CDC_OFFSET_STATE_KEY`. External-owned
        ingesters override this to return a
        :data:`~tabsdatak.spi.CommitStateFn` instead."""
        return {CDC_OFFSET_STATE_KEY: offset.write()}

    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> CdcOffset | None:
        """The offset to resume this run from, or ``None`` to seed from
        the configured ``start_from``.

        Default (Tabsdata-owned): ``None`` on the first stage (cold start
        -> ``start_from``), otherwise reconstruct from the persisted
        ``stream_state`` via :meth:`offset_from_state`. External-owned
        ingesters override this to always return ``None`` (the source
        resumes its own cursor)."""
        if first_stage or not stream_state:
            return None
        return self.offset_from_state(stream_state[CDC_OFFSET_STATE_KEY])

    def offset_from_state(self, offset_state: str) -> CdcOffset:
        """Reconstruct the resume offset from the Tabsdata-persisted
        ``offset_state`` string (the value :meth:`commit_state` stored,
        i.e. ``offset.write()``) -- typically ``<Offset>.read(offset_state)``.
        Tabsdata-owned ingesters implement this; external-owned ones
        override :meth:`resume_offset` and need not."""
        raise NotImplementedError(
            f"{type(self).__name__} keeps no Tabsdata-owned offset state"
        )

    def seed_offset(self, offset: CdcOffset | None) -> None:
        """Seed the resume mark before the stream opens, so
        ``open_session`` / ``init_stream`` continues from ``offset``
        instead of ``start_from``. ``None`` leaves the mark untouched
        (cold start / source-managed resume)."""
        if offset is not None:
            self._mark = offset
