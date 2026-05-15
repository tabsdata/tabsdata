from __future__ import annotations

from typing import Any, cast

from tabsdatak.conn.mysql import MySQLConn, MySQLDest, MySQLSrc
from tabsdatak.conn.mysql.error import MysqlErrorCode
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
# inside the plugin method bodies below. The conn-mysql package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class MySQLSrcPlugin(SrcPlugin[MySQLSrc]):
    def read_in(
        self,
        ctx: SrcContext,
        src: MySQLSrc,
    ) -> list[list[TableFileSpec]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory
        from tabsdatak.conn.mysql._adapter import MYSQL_READ_DEFAULTS, MySQLReadCfg

        read_cfg = MySQLReadCfg(
            conn=cast(MySQLConn, src.conn),
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            query_params=src.query_params or None,
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            MYSQL_READ_DEFAULTS,
        )

        try:
            return engine.read(read_cfg, ctx.work_dir())
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise MysqlErrorCode.MYSQL_7.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise MysqlErrorCode.MYSQL_8.exception(
                cause=e,
                query_index=-1,
            )


class MySQLDestPlugin(DestPlugin[MySQLDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: MySQLDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.mysql import _adapter as _mysql_sql
        from tabsdatak.conn.mysql._adapter import MySQLWriteCfg

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (MYSQL_10).
        if len(dest.tables) != len(tables):
            raise MysqlErrorCode.MYSQL_10.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = MySQLWriteCfg(
            conn=cast(MySQLConn, dest.conn),
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _mysql_sql.MYSQL_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise MysqlErrorCode.MYSQL_11.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise MysqlErrorCode.MYSQL_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class MySQLExplorer(Explorer[MySQLConn]):
    def metadata(self, conn: MySQLConn) -> Any:
        # TODO
        pass

    def data(self, conn: MySQLConn, query: str) -> Any:
        # TODO
        pass


MYSQL_SRC = SrcDef(
    conn=MySQLConn,
    src_type="mysql",
    src_version="v1",
    src=MySQLSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=MySQLSrcPlugin,
    explorer=MySQLExplorer,
)

MYSQL_DEST = DestDef(
    conn=MySQLConn,
    dest_type="mysql",
    dest_version="v1",
    dest=MySQLDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=MySQLDestPlugin,
    explorer=MySQLExplorer,
)
