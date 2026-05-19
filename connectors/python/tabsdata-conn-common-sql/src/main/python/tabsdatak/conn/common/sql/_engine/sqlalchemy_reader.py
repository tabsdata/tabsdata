import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Mapping

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
from tabsdatak.conn.common.sql._engine.reader import SqlReadCfg, SqlReadEngine

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.type_convertion import resolve_schema_overrides
from tabsdatak.spi import ParquetFile, TableFileSpec

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


ReadLoader = Callable[..., list[Path]]


class SqlAlchemyReadEngine(SqlReadEngine, ABC):
    @abstractmethod
    def loader(self) -> ReadLoader: ...

    def read(self, src: SqlReadCfg, work_dir: Path) -> list[list[TableFileSpec]]:
        read_loader = self.loader()

        # Empty dict -> None (per the public contract; saves the
        # loaders from having to special-case the empty case).
        parameters: Mapping[str, Any] | None = src.query_params or None

        def _run() -> list[list[TableFileSpec]]:
            logger.debug("sql-reader: establishing engine")
            engine = src.create_sql_alchemy_engine()
            try:
                results: list[list[TableFileSpec]] = []
                if src.transactional:
                    with Session(engine) as session, session.begin():
                        conn = session.connection()
                        self._run_queries(
                            read_loader, conn, src, work_dir, parameters, results
                        )
                else:
                    with engine.connect() as conn:
                        self._run_queries(
                            read_loader, conn, src, work_dir, parameters, results
                        )
                return results
            finally:
                logger.debug("sql-reader: disposing engine")
                engine.dispose()

        return _retry()(_run)

    @staticmethod
    def _run_queries(
        read_loader: Any,
        conn: Any,
        src: SqlReadCfg,
        work_dir: Path,
        parameters: Mapping[str, Any] | None,
        results: list[list[TableFileSpec]],
    ):
        chunk_size = _config.get_cfg(
            src.cfg,
            const.READ_CHUNK_SIZE_KEY,
            const.READ_CHUNK_SIZE_DEFAULT,
        )
        total = len(src.queries)
        logger.info("sql-reader: reading %d quer(ies)", total)
        for i, query in enumerate(src.queries):
            logger.info("sql-reader: reading query %d/%d", i + 1, total)
            overrides = (
                resolve_schema_overrides(src.schema_overrides[i])
                if src.schema_overrides is not None
                else None
            )
            files = read_loader(
                connection=conn,
                query=query,
                work_dir=work_dir,
                slot=i,
                chunk_size=chunk_size,
                schema_overrides=overrides,
                parameters=parameters,
            )
            logger.debug("sql-reader: query %d/%d produced %d chunk(s)", i + 1, total, len(files))
            results.append([ParquetFile(p) for p in files])
        logger.info("sql-reader: finished reading %d quer(ies)", total)


def _pandas_read(
    *,
    connection: Connection,
    query: str,
    work_dir: Path,
    slot: int,
    chunk_size: int,
    schema_overrides: Mapping[str, Any] | None,
    parameters: Mapping[str, Any] | None = None,
) -> list[Path]:
    import pandas as pd
    from sqlalchemy import text

    files: list[Path] = []
    iterator = pd.read_sql_query(
        text(query),
        con=connection,
        chunksize=chunk_size,
        dtype=dict(schema_overrides) if schema_overrides else None,
        params=dict(parameters) if parameters else None,
    )
    for chunk_idx, frame in enumerate(iterator):
        out = work_dir / f"slot{slot}_chunk{chunk_idx}.parquet"
        frame.to_parquet(out, engine="pyarrow")
        files.append(out)

    return files


class PandasReadEngine(SqlAlchemyReadEngine):
    def loader(self) -> ReadLoader:
        return _pandas_read


def _polars_read(
    *,
    connection: Any,
    query: str,
    work_dir: Path,
    slot: int,
    chunk_size: int,
    schema_overrides: Mapping[str, Any] | None,
    parameters: Mapping[str, Any] | None = None,
) -> list[Path]:
    import polars as pl
    from sqlalchemy import text

    execute_options: dict[str, Any] = {}
    if parameters:
        execute_options["parameters"] = dict(parameters)

    files: list[Path] = []
    streamed_conn = connection.execution_options(stream_results=True)
    batches = pl.read_database(
        text(query),
        connection=streamed_conn,
        iter_batches=True,
        batch_size=chunk_size,
        schema_overrides=schema_overrides,
        execute_options=execute_options or None,
    )
    for chunk_idx, batch in enumerate(batches):
        out = work_dir / f"slot{slot}_chunk{chunk_idx}.parquet"
        batch.write_parquet(out, use_pyarrow=True)
        files.append(out)

    return files


class PolarsReadEngine(SqlAlchemyReadEngine):
    def loader(self) -> ReadLoader:
        return _polars_read
