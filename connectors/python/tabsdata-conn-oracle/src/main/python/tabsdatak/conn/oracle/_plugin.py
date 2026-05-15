from __future__ import annotations

from typing import Any, cast

from tabsdatak.conn.oracle import OracleConn, OracleDest, OracleSrc
from tabsdatak.conn.oracle.error import OracleErrorCode
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
# inside the plugin method bodies below. The conn-oracle package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class OracleSrcPlugin(SrcPlugin[OracleSrc]):
    def read_in(
        self,
        ctx: SrcContext,
        src: OracleSrc,
    ) -> list[list[TableFileSpec]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory
        from tabsdatak.conn.oracle._adapter import ORACLE_READ_DEFAULTS, OracleReadCfg

        read_cfg = OracleReadCfg(
            conn=cast(OracleConn, src.conn),
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            query_params=src.query_params or None,
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            ORACLE_READ_DEFAULTS,
        )

        try:
            return engine.read(read_cfg, ctx.work_dir())
        except OperationalError as e:
            raise OracleErrorCode.ORACLE_7.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            raise OracleErrorCode.ORACLE_8.exception(
                cause=e,
                query_index=-1,
            )


class OracleDestPlugin(DestPlugin[OracleDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: OracleDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.oracle import _adapter as _oracle_sql
        from tabsdatak.conn.oracle._adapter import OracleWriteCfg

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (ORACLE_10).
        if len(dest.tables) != len(tables):
            raise OracleErrorCode.ORACLE_10.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = OracleWriteCfg(
            conn=cast(OracleConn, dest.conn),
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _oracle_sql.ORACLE_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise OracleErrorCode.ORACLE_11.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            raise OracleErrorCode.ORACLE_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class OracleExplorer(Explorer[OracleConn]):
    def metadata(self, conn: OracleConn) -> Any:
        # TODO
        pass

    def data(self, conn: OracleConn, query: str) -> Any:
        # TODO
        pass


ORACLE_SRC = SrcDef(
    conn=OracleConn,
    src_type="oracle",
    src_version="v1",
    src=OracleSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=OracleSrcPlugin,
    explorer=OracleExplorer,
)

ORACLE_DEST = DestDef(
    conn=OracleConn,
    dest_type="oracle",
    dest_version="v1",
    dest=OracleDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=OracleDestPlugin,
    explorer=OracleExplorer,
)
