import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, cast

from polars import DataFrame
from sqlalchemy import Connection
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_delay,
    wait_fixed,
)

# noinspection PyProtectedMember
import tabsdatak.conn.common.sql._engine.constants as const

# noinspection PyProtectedMember
from tabsdatak.conn.common import _config

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import SqlWriteCfg, SqlWriteEngine
from tabsdatak.spi import TableFileSpec

# --- Retry ----------------------------------------------------------

logger = logging.getLogger(__name__)

RETRY_TIMEOUT_S = 5 * 60
RETRY_WAIT_S = 5


def _retry() -> Retrying:
    return Retrying(
        stop=stop_after_delay(RETRY_TIMEOUT_S),
        wait=wait_fixed(RETRY_WAIT_S),
        retry=retry_if_exception_type(OperationalError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


WriteLoader = Callable[..., None]


class SqlAlchemyWriteEngine(SqlWriteEngine, ABC):
    @abstractmethod
    def loader(self) -> WriteLoader: ...

    def write(
        self,
        dest: SqlWriteCfg,
        tables: Iterable[TableFileSpec],
    ) -> None:
        write_loader = self.loader()
        files = list(tables)
        if len(files) != len(dest.target_tables):
            raise ValueError(
                f"tables count {len(files)} must equal target_tables "
                f"count {len(dest.target_tables)}"
            )

        def _run() -> None:
            logger.debug("sql-writer: establishing engine")
            engine = dest.create_sql_alchemy_engine()
            try:
                if dest.transactional:
                    with Session(engine) as session, session.begin():
                        conn = session.connection()
                        self._run_writes(write_loader, conn, dest, files)
                else:
                    with engine.connect() as conn:
                        self._run_writes(write_loader, conn, dest, files)
                        conn.commit()
            finally:
                logger.debug("sql-writer: disposing engine")
                engine.dispose()

        _retry()(_run)

    @staticmethod
    def _run_writes(
        write_loader: WriteLoader,
        conn: Any,
        dest: SqlWriteCfg,
        files: list[TableFileSpec],
    ):
        chunk_size = _config.get_cfg(
            dest.cfg,
            const.WRITE_CHUNK_SIZE_KEY,
            const.WRITE_CHUNK_SIZE_DEFAULT,
        )
        total = len(dest.target_tables)
        logger.info("sql-writer: writing %d table(s)", total)
        for i, (parquet_path, table) in enumerate(
            zip(files, dest.target_tables),
            1,
        ):
            logger.info("sql-writer: writing table %d/%d: %r", i, total, table)
            write_loader(
                connection=conn,
                parquet_file=Path(parquet_path),
                table=table,
                if_table_exists=dest.if_table_exists,
                chunk_size=chunk_size,
            )
            logger.debug("sql-writer: finished table %r", table)
        logger.info("sql-writer: finished writing %d table(s)", total)


def _polars_write(
    *,
    connection: Connection,
    parquet_file: Path,
    table: str,
    if_table_exists: Literal["append", "replace"],
    chunk_size: int,
) -> None:
    import polars as pl
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_file)
    wrote_any = False
    for batch in pf.iter_batches(batch_size=chunk_size):
        df = cast(DataFrame, pl.from_arrow(batch))
        df.write_database(
            table_name=table,
            connection=connection,
            if_table_exists=if_table_exists,
        )
        if_table_exists = "append"
        wrote_any = True

    if not wrote_any:
        # Empty file: still create / preserve the table with the
        # caller's if_table_exists semantics by writing an empty
        # frame matching the file's schema.
        df = cast(DataFrame, pl.from_arrow(pf.read()))
        df.write_database(
            table_name=table,
            connection=connection,
            if_table_exists=if_table_exists,
        )


class PolarsWriteEngine(SqlAlchemyWriteEngine):
    def loader(self) -> WriteLoader:
        return _polars_write
