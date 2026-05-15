from __future__ import annotations

from typing import Any, cast

from tabsdatak.conn.db2 import Db2Conn, Db2Dest, Db2Src
from tabsdatak.conn.db2.error import Db2ErrorCode
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
# inside the plugin method bodies below. The conn-db2 package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class Db2SrcPlugin(SrcPlugin[Db2Src]):
    def read_in(
        self,
        ctx: SrcContext,
        src: Db2Src,
    ) -> list[list[TableFileSpec]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory
        from tabsdatak.conn.db2._adapter import DB2_READ_DEFAULTS, Db2ReadCfg

        read_cfg = Db2ReadCfg(
            conn=cast(Db2Conn, src.conn),
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            query_params=src.query_params or None,
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            DB2_READ_DEFAULTS,
        )

        try:
            return engine.read(read_cfg, ctx.work_dir())
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise Db2ErrorCode.DB2_7.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise Db2ErrorCode.DB2_8.exception(
                cause=e,
                query_index=-1,
            )


class Db2DestPlugin(DestPlugin[Db2Dest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: Db2Dest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.db2 import _adapter as _db2_sql
        from tabsdatak.conn.db2._adapter import Db2WriteCfg

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (DB2_10).
        if len(dest.tables) != len(tables):
            raise Db2ErrorCode.DB2_10.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = Db2WriteCfg(
            conn=cast(Db2Conn, dest.conn),
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _db2_sql.DB2_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise Db2ErrorCode.DB2_11.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise Db2ErrorCode.DB2_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class Db2Explorer(Explorer[Db2Conn]):
    def metadata(self, conn: Db2Conn) -> Any:
        # TODO
        pass

    def data(self, conn: Db2Conn, query: str) -> Any:
        # TODO
        pass


DB2_SRC = SrcDef(
    conn=Db2Conn,
    src_type="db2",
    src_version="v1",
    src=Db2Src,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=Db2SrcPlugin,
    explorer=Db2Explorer,
)

DB2_DEST = DestDef(
    conn=Db2Conn,
    dest_type="db2",
    dest_version="v1",
    dest=Db2Dest,
    cardinality=lambda dest: len(dest.tables),
    plugin=Db2DestPlugin,
    explorer=Db2Explorer,
)
