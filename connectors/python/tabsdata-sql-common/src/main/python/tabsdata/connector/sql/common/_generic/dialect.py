#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import tzinfo
from typing import Any, Literal

import sqlalchemy.types as sa_types
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.types import BLOB, CLOB, LargeBinary
from sqlglot import exp, parse as sqlglot_parse

logger = logging.getLogger(__name__)

LARGE_COLUMN_TYPES = (
    BLOB,
    CLOB,
    LargeBinary,
)

CdcIngesterType = Literal[
    "binlog",
    "sqlreplica",
    "wal2json",
]

edt = exp.DataType.Type

SQLGLOT_TO_SA: dict[exp.DataType.Type, type[sa_types.TypeEngine]] = {
    edt.INT: sa_types.Integer,
    edt.UINT: sa_types.Integer,
    edt.BIGINT: sa_types.BigInteger,
    edt.UBIGINT: sa_types.BigInteger,
    edt.SMALLINT: sa_types.SmallInteger,
    edt.USMALLINT: sa_types.SmallInteger,
    edt.TINYINT: sa_types.SmallInteger,
    edt.UTINYINT: sa_types.SmallInteger,
    edt.MEDIUMINT: sa_types.Integer,
    edt.SERIAL: sa_types.Integer,
    edt.BIGSERIAL: sa_types.BigInteger,
    edt.SMALLSERIAL: sa_types.SmallInteger,
    edt.FLOAT: sa_types.Float,
    edt.DOUBLE: sa_types.Float,
    edt.DECIMAL: sa_types.Numeric,
    edt.VARCHAR: sa_types.String,
    edt.CHAR: sa_types.String,
    edt.NVARCHAR: sa_types.Unicode,
    edt.NCHAR: sa_types.Unicode,
    edt.TEXT: sa_types.Text,
    edt.BOOLEAN: sa_types.Boolean,
    edt.BIT: sa_types.Boolean,
    edt.DATE: sa_types.Date,
    edt.DATETIME: sa_types.DateTime,
    edt.DATETIME2: sa_types.DateTime,
    edt.TIMESTAMP: sa_types.DateTime,
    edt.TIMESTAMPTZ: sa_types.DateTime,
    edt.TIMESTAMPLTZ: sa_types.DateTime,
    edt.TIME: sa_types.Time,
    edt.TIMETZ: sa_types.Time,
    edt.BINARY: sa_types.LargeBinary,
    edt.VARBINARY: sa_types.LargeBinary,
    edt.BLOB: sa_types.LargeBinary,
    edt.IMAGE: sa_types.LargeBinary,
    edt.JSON: sa_types.JSON,
    edt.JSONB: sa_types.JSON,
    edt.XML: sa_types.Text,
    edt.UUID: sa_types.Uuid,
    edt.ENUM: sa_types.String,
    edt.SET: sa_types.String,
    edt.YEAR: sa_types.Integer,
    edt.MONEY: sa_types.Numeric,
    edt.SMALLMONEY: sa_types.Numeric,
}


class Dialect(ABC):
    @property
    @abstractmethod
    def sqlglot_dialect(self) -> str | None: ...

    @property
    @abstractmethod
    def open_quote(self) -> str: ...

    @property
    @abstractmethod
    def close_quote(self) -> str: ...

    @abstractmethod
    def default_case_fold(self, name: str) -> str: ...

    @abstractmethod
    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]: ...

    @abstractmethod
    def is_large_column(self, satype: Any) -> bool: ...

    @abstractmethod
    def server_timezone(self, engine: Any) -> tzinfo | None: ...

    def ddl_to_sa(self, dtype_string: str) -> sa_types.TypeEngine:
        # noinspection PyBroadException
        try:
            sql = f"select cast(x as {dtype_string})"
            trees = sqlglot_parse(sql, dialect=self.sqlglot_dialect)
            if not trees:
                return sa_types.String()
            cast = trees[0].find(exp.Cast)
            if cast is None:
                return sa_types.String()
            return self.sqlglot_to_sa(cast.to)
        except Exception:
            logger.warning(f"Could not parse type string: {dtype_string!r}")
            return sa_types.String()

    def cdc_to_sa(
        self,
        ingester_type: CdcIngesterType,
        cdc_dtype: Any,
    ) -> sa_types.TypeEngine:
        raise NotImplementedError(
            f"{type(self).__name__}.cdc_to_sa is not implemented "
            f"for ingester type {ingester_type!r}"
        )

    def parse_qualified_table(self, spec: str) -> tuple[quoted_name, quoted_name]:
        raw = spec
        spec = spec.strip()
        oq = self.open_quote
        cq = self.close_quote

        def token(s: str, start: int) -> tuple[quoted_name, int]:
            if start >= len(s):
                raise ValueError(
                    f"Invalid qualified table spec: {raw!r}; unexpected end of string"
                )
            if s[start] == oq:
                pos = start + 1
                while True:
                    end = s.find(cq, pos)
                    if end == -1:
                        raise ValueError(
                            f"Invalid qualified table spec: {raw!r}; "
                            f"unclosed {oq} at position {start}"
                        )
                    if end + 1 < len(s) and s[end + 1] == cq:
                        pos = end + 2
                        continue
                    break
                escaped = s[start + 1 : end]
                name = escaped.replace(cq + cq, cq)
                if not name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"empty quoted identifier at position {start}"
                    )
                return quoted_name(name, quote=True), end + 1
            else:
                end = s.find(".", start)
                if end == -1:
                    end = len(s)
                name = s[start:end].strip()
                if not name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"empty identifier at position {start}"
                    )
                if oq in name or cq in name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"unquoted identifier contains quote "
                        f"character at position {start}"
                    )
                return quoted_name(name, quote=False), end

        schema, idx = token(spec, 0)
        if idx >= len(spec) or spec[idx] != ".":
            raise ValueError(
                f"Invalid qualified table spec: {raw!r}, expected '.' after schema"
            )
        table, idx = token(spec, idx + 1)
        if idx != len(spec):
            raise ValueError(
                f"Invalid qualified table spec: {raw!r}, "
                f"unexpected content after table name"
            )
        return schema, table

    @staticmethod
    def sqlglot_to_sa(dt: exp.DataType) -> sa_types.TypeEngine:
        def extract_params(dtype: exp.DataType) -> list:
            dtype_params = []
            for expression in dtype.expressions:
                if isinstance(expression, exp.DataTypeParam):
                    inner = expression.this
                    if isinstance(inner, exp.Literal):
                        if inner.is_string:
                            dtype_params.append(inner.this)
                        else:
                            dtype_params.append(int(inner.this))
                    elif isinstance(inner, exp.Var):
                        dtype_params.append(inner.this)
                    else:
                        dtype_params.append(str(inner))
                elif isinstance(expression, exp.Literal):
                    if expression.is_string:
                        dtype_params.append(expression.this)
                    else:
                        dtype_params.append(int(expression.this))
                else:
                    dtype_params.append(str(expression))
            return dtype_params

        sa_cls = SQLGLOT_TO_SA.get(dt.this)
        if sa_cls is None:
            raise ValueError(f"Unsupported sqlglot type: {dt.this!r}")
        params = extract_params(dt)
        if issubclass(sa_cls, sa_types.Numeric) and not issubclass(
            sa_cls, sa_types.Float
        ):
            int_params = [param for param in params if isinstance(param, int)]
            if len(int_params) >= 2:
                return sa_cls(precision=int_params[0], scale=int_params[1])
            elif len(int_params) == 1:
                return sa_cls(precision=int_params[0])
            return sa_cls()
        if issubclass(sa_cls, sa_types.Float):
            int_params = [param for param in params if isinstance(param, int)]
            if int_params:
                return sa_cls(precision=int_params[0])
            return sa_cls()
        if issubclass(sa_cls, (sa_types.String, sa_types.Unicode)):
            int_params = [param for param in params if isinstance(param, int)]
            if int_params:
                return sa_cls(length=int_params[0])
            return sa_cls()
        if issubclass(sa_cls, sa_types.DateTime):
            tz = dt.this in (edt.TIMESTAMPTZ, edt.TIMESTAMPLTZ, edt.TIMETZ)
            return sa_cls(timezone=tz)
        if issubclass(sa_cls, sa_types.Time):
            tz = dt.this == edt.TIMETZ
            return sa_cls(timezone=tz)
        return sa_cls()
