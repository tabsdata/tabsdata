from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import Engine

from tabsdatak.api import Conn
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec
from tabsdatak.spi import TableFileSpec


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


class SqlReadEngine(ABC):
    @abstractmethod
    def read(self, cfg: SqlReadCfg, work_dir: Path) -> list[list[TableFileSpec]]: ...
