#
# Copyright 2026 Tabsdata Inc.
#

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import Engine

from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec
from tabsdatak.error import ErrorCode, TabsdataException
from tabsdatak.spi import Conn, LazyFrameInput


@dataclass(kw_only=True)
class SqlReadCfg[C: Conn](ABC):
    conn: C
    queries: list[str]
    transactional: bool
    schema_overrides: SchemaOverrideSpec | None
    initial_values: Mapping[str, Any] | None
    cfg: CfgSpec | None

    def create_sql_alchemy_engine(self) -> Engine:
        """
        Implemented by subclasses that use SqlAlchemy.
        """
        raise NotImplementedError()


@dataclass(frozen=True)
class SqlReadFaults:
    unreachable: ErrorCode
    failed: ErrorCode
    uri: str

    def connection(self, cause: Exception) -> TabsdataException:
        return self.unreachable.exception(cause=cause, uri=self.uri)

    def query(self, cause: Exception, index: int) -> TabsdataException:
        return self.failed.exception(cause=cause, query_index=index)


class SqlReadEngine(ABC):
    @abstractmethod
    def read(
        self,
        cfg: SqlReadCfg,
        faults: SqlReadFaults,
        *,
        drop_metadata: bool = False,
    ) -> list[Iterable[LazyFrameInput]]: ...
