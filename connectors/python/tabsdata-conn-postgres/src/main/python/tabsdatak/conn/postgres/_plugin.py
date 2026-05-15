from __future__ import annotations

from typing import Any, cast

from tabsdatak.conn.postgres import PostgresConn, PostgresDest, PostgresSrc
from tabsdatak.conn.postgres.error import PostgresErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcContext,
    SrcDef,
    SrcPlugin,
    TableFileSpec,
    TableMode,
)

# All sqlalchemy, conn-common-sql and DBAPI-driver imports are made
# inside the plugin method bodies below. The conn-postgres package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class PostgresSrcPlugin(SrcPlugin[PostgresSrc]):
    def read_in(
        self,
        ctx: SrcContext,
        src: PostgresSrc,
    ) -> list[list[TableFileSpec]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory
        from tabsdatak.conn.postgres._adapter import (
            POSTGRES_READ_DEFAULTS,
            PostgresReadCfg,
        )

        read_cfg = PostgresReadCfg(
            conn=cast(PostgresConn, src.conn),
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            query_params=src.query_params or None,
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            POSTGRES_READ_DEFAULTS,
        )

        try:
            return engine.read(read_cfg, ctx.work_dir())
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise PostgresErrorCode.POSTGRES_7.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise PostgresErrorCode.POSTGRES_8.exception(
                cause=e,
                query_index=-1,
            )


class PostgresDestPlugin(DestPlugin[PostgresDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: PostgresDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.postgres import _adapter as _postgres_sql
        from tabsdatak.conn.postgres._adapter import PostgresWriteCfg

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (POSTGRES_10).
        if len(dest.tables) != len(tables):
            raise PostgresErrorCode.POSTGRES_10.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = PostgresWriteCfg(
            conn=cast(PostgresConn, dest.conn),
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _postgres_sql.POSTGRES_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise PostgresErrorCode.POSTGRES_11.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise PostgresErrorCode.POSTGRES_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class PostgresExplorer(Explorer[PostgresConn]):
    def metadata(self, conn: PostgresConn) -> Any:
        # TODO
        pass

    def data(self, conn: PostgresConn, query: str) -> Any:
        # TODO
        pass


POSTGRES_SRC = SrcDef(
    conn=PostgresConn,
    src_type="postgres",
    src_version="v1",
    src=PostgresSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=PostgresSrcPlugin,
    explorer=PostgresExplorer,
)

POSTGRES_DEST = DestDef(
    conn=PostgresConn,
    dest_type="postgres",
    dest_version="v1",
    dest=PostgresDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=PostgresDestPlugin,
    explorer=PostgresExplorer,
)
