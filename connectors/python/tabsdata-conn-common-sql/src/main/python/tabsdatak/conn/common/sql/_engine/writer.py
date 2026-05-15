from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Literal

from sqlalchemy import Engine

from tabsdatak.api import Conn
from tabsdatak.conn.common.types import CfgSpec
from tabsdatak.spi import TableFileSpec


@dataclass(kw_only=True)
class SqlWriteCfg[C: Conn](ABC):
    conn: C
    target_tables: list[str]
    if_table_exists: Literal["append", "replace"]
    transactional: bool
    cfg: CfgSpec | None

    def create_sql_alchemy_engine(self) -> Engine:
        """
        Implemented by subclasses that use SqlAlchemy.
        """
        raise NotImplementedError()


class SqlWriteEngine(ABC):
    @abstractmethod
    def write(
        self,
        cfg: SqlWriteCfg,
        tables: Iterable[TableFileSpec],
    ) -> None: ...
