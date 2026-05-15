from __future__ import annotations

from typing import Any, cast

from tabsdatak.conn.mssql import MSSQLConn, MSSQLDest, MSSQLSrc
from tabsdatak.conn.mssql.error import MssqlErrorCode
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
# inside the plugin method bodies below. The conn-mssql package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class MSSQLSrcPlugin(SrcPlugin[MSSQLSrc]):
    def read_in(
        self,
        ctx: SrcContext,
        src: MSSQLSrc,
    ) -> list[list[TableFileSpec]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory
        from tabsdatak.conn.mssql._adapter import MSSQL_READ_DEFAULTS, MSSQLReadCfg

        read_cfg = MSSQLReadCfg(
            conn=cast(MSSQLConn, src.conn),
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            query_params=src.query_params or None,
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            MSSQL_READ_DEFAULTS,
        )

        try:
            return engine.read(read_cfg, ctx.work_dir())
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise MssqlErrorCode.MSSQL_6.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise MssqlErrorCode.MSSQL_7.exception(
                cause=e,
                query_index=-1,
            )


class MSSQLDestPlugin(DestPlugin[MSSQLDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: MSSQLDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.mssql import _adapter as _mssql_sql
        from tabsdatak.conn.mssql._adapter import MSSQLWriteCfg

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (MSSQL_9).
        if len(dest.tables) != len(tables):
            raise MssqlErrorCode.MSSQL_9.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = MSSQLWriteCfg(
            conn=cast(MSSQLConn, dest.conn),
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _mssql_sql.MSSQL_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise MssqlErrorCode.MSSQL_10.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise MssqlErrorCode.MSSQL_8.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class MSSQLExplorer(Explorer[MSSQLConn]):
    def metadata(self, conn: MSSQLConn) -> Any:
        # TODO
        pass

    def data(self, conn: MSSQLConn, query: str) -> Any:
        # TODO
        pass


MSSQL_SRC = SrcDef(
    conn=MSSQLConn,
    src_type="mssql",
    src_version="v1",
    src=MSSQLSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=MSSQLSrcPlugin,
    explorer=MSSQLExplorer,
)

MSSQL_DEST = DestDef(
    conn=MSSQLConn,
    dest_type="mssql",
    dest_version="v1",
    dest=MSSQLDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=MSSQLDestPlugin,
    explorer=MSSQLExplorer,
)
