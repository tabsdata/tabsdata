#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import Any, Callable, Mapping

import polars as pl
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, SessionTransaction

# noinspection PyProtectedMember
import tabsdatak.conn.common.sql._engine.constants as const
from tabsdatak._handoff import RestartRead

# noinspection PyProtectedMember
from tabsdatak.conn.common import _config
from tabsdatak.conn.common.sql._engine.reader import (
    SqlReadCfg,
    SqlReadEngine,
    SqlReadFaults,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.type_convertion import resolve_schema_overrides
from tabsdatak.conn.common.sql._metadata import sql_metadata
from tabsdatak.spi import FrameMetadata, LazyFrameInput, MetadataSpec

logger = logging.getLogger(__name__)

RETRY_TIMEOUT_S = 5 * 60
RETRY_WAIT_S = 5


ReadLoader = Callable[..., Iterator[pl.DataFrame]]


class SqlAlchemyReadEngine(SqlReadEngine, ABC):
    @abstractmethod
    def loader(self) -> ReadLoader: ...

    def read(
        self,
        cfg: SqlReadCfg,
        faults: SqlReadFaults,
        *,
        drop_metadata: bool = False,
    ) -> list[Iterable[LazyFrameInput]]:
        reading = Reading(cfg, self.loader(), faults, drop_metadata)
        return [reading.slot(index) for index in range(len(cfg.queries))]


class Reading:
    def __init__(
        self,
        cfg: SqlReadCfg,
        loader: ReadLoader,
        faults: SqlReadFaults,
        drop_metadata: bool,
    ) -> None:
        self.cfg = cfg
        self.loader = loader
        self.faults = faults
        self.drop_metadata = drop_metadata
        self.parameters: Mapping[str, Any] | None = cfg.initial_values or None
        self.chunk_size = _config.get_cfg(
            cfg.cfg,
            const.READ_CHUNK_SIZE_KEY,
            const.READ_CHUNK_SIZE_DEFAULT,
        )
        self.engine: Engine | None = None
        self.session: Session | None = None
        self.transaction: SessionTransaction | None = None
        self.connection: Connection | None = None

    def open(self) -> Connection:
        if self.connection is not None:
            return self.connection
        logger.debug("sql-reader: establishing engine")
        self.engine = self.cfg.create_sql_alchemy_engine()
        if self.cfg.transactional:
            self.session = Session(self.engine)
            self.transaction = self.session.begin()
            self.connection = self.session.connection()
        else:
            self.connection = self.engine.connect()
        return self.connection

    def close(self, completed: bool) -> None:
        try:
            if self.session is not None:
                if completed and self.transaction is not None:
                    self.transaction.commit()
                self.session.close()
            elif self.connection is not None:
                self.connection.close()
        finally:
            if self.engine is not None:
                logger.debug("sql-reader: disposing engine")
                self.engine.dispose()
            self.engine = None
            self.session = None
            self.transaction = None
            self.connection = None

    def metadata(self, query: str) -> FrameMetadata | None:
        if self.drop_metadata:
            return None
        columns: MetadataSpec = {**sql_metadata(query, self.cfg.initial_values)}
        return FrameMetadata(columns=columns)

    def overrides(self, index: int) -> Mapping[str, Any] | None:
        if self.cfg.schema_overrides is None:
            return None
        return resolve_schema_overrides(self.cfg.schema_overrides[index])

    def slot(self, index: int) -> Iterator[LazyFrameInput]:
        query = self.cfg.queries[index]
        total = len(self.cfg.queries)
        metadata = self.metadata(query)
        completed = False
        try:
            logger.info("sql-reader: reading query %d/%d", index + 1, total)
            chunks = 0
            for batch in self.loader(
                connection=self.open(),
                query=query,
                chunk_size=self.chunk_size,
                schema_overrides=self.overrides(index),
                parameters=self.parameters,
            ):
                chunks += 1
                yield LazyFrameInput(batch.lazy(), metadata=metadata)
            logger.debug(
                "sql-reader: query %d/%d produced %d chunk(s)",
                index + 1,
                total,
                chunks,
            )
            completed = True
        except OperationalError as error:
            raise RestartRead(
                self.faults.connection(error),
                wait=RETRY_WAIT_S,
                within=RETRY_TIMEOUT_S,
            ) from error
        except SQLAlchemyError as error:
            raise self.faults.query(error, index) from error
        finally:
            if not completed or index == total - 1:
                self.close(completed)


def _pandas_read(
    *,
    connection: Connection,
    query: str,
    chunk_size: int,
    schema_overrides: Mapping[str, Any] | None,
    parameters: Mapping[str, Any] | None = None,
) -> Iterator[pl.DataFrame]:
    import pandas as pd
    from sqlalchemy import text

    for frame in pd.read_sql_query(
        text(query),
        con=connection.execution_options(stream_results=True),
        chunksize=chunk_size,
        params=dict(parameters) if parameters else None,
    ):
        yield pl.from_pandas(
            frame, schema_overrides=dict(schema_overrides) if schema_overrides else None
        )


class PandasReadEngine(SqlAlchemyReadEngine):
    def loader(self) -> ReadLoader:
        return _pandas_read


def _polars_read(
    *,
    connection: Any,
    query: str,
    chunk_size: int,
    schema_overrides: Mapping[str, Any] | None,
    parameters: Mapping[str, Any] | None = None,
) -> Iterator[pl.DataFrame]:
    from sqlalchemy import text

    execute_options: dict[str, Any] = {}
    if parameters:
        execute_options["parameters"] = dict(parameters)

    yield from pl.read_database(
        text(query),
        connection=connection.execution_options(stream_results=True),
        iter_batches=True,
        batch_size=chunk_size,
        schema_overrides=schema_overrides,
        execute_options=execute_options or None,
    )


class PolarsReadEngine(SqlAlchemyReadEngine):
    def loader(self) -> ReadLoader:
        return _polars_read
