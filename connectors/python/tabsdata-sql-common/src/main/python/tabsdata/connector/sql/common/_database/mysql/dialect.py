#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import tzinfo
from typing import Any

import pymysqlreplication.constants.FIELD_TYPE as FIELD_TYPE
import sqlalchemy.types as sa_types
from sqlalchemy import text
from sqlalchemy.dialects.mysql import (
    BIT,
    DATETIME,
    DOUBLE,
    ENUM,
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMINT,
    MEDIUMTEXT,
    SET,
    TIME,
    TIMESTAMP,
    TINYBLOB,
    TINYINT,
    YEAR,
)
from sqlalchemy.types import Text

from tabsdata.connector.sql.common._generic.dialect import LARGE_COLUMN_TYPES, Dialect
from tabsdata.connector.sql.common._utils import parse_server_timezone

MYSQL_LARGE_COLUMN_TYPES = LARGE_COLUMN_TYPES + (
    Text,
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    TINYBLOB,
)

FIELD_TYPE_MAP: dict[int, type] = {
    FIELD_TYPE.TINY: TINYINT,
    FIELD_TYPE.SHORT: sa_types.SmallInteger,
    FIELD_TYPE.LONG: sa_types.Integer,
    FIELD_TYPE.FLOAT: sa_types.Float,
    FIELD_TYPE.DOUBLE: DOUBLE,
    FIELD_TYPE.NULL: sa_types.NullType,
    FIELD_TYPE.TIMESTAMP: TIMESTAMP,
    FIELD_TYPE.LONGLONG: sa_types.BigInteger,
    FIELD_TYPE.INT24: MEDIUMINT,
    FIELD_TYPE.DATE: sa_types.Date,
    FIELD_TYPE.TIME: TIME,
    FIELD_TYPE.DATETIME: DATETIME,
    FIELD_TYPE.YEAR: YEAR,
    FIELD_TYPE.VARCHAR: sa_types.String,
    FIELD_TYPE.BIT: BIT,
    FIELD_TYPE.JSON: sa_types.JSON,
    FIELD_TYPE.NEWDECIMAL: sa_types.Numeric,
    FIELD_TYPE.ENUM: ENUM,
    FIELD_TYPE.SET: SET,
    FIELD_TYPE.TINY_BLOB: TINYBLOB,
    FIELD_TYPE.MEDIUM_BLOB: MEDIUMBLOB,
    FIELD_TYPE.LONG_BLOB: LONGBLOB,
    FIELD_TYPE.BLOB: sa_types.LargeBinary,
    FIELD_TYPE.VAR_STRING: sa_types.String,
    FIELD_TYPE.STRING: sa_types.String,
    FIELD_TYPE.GEOMETRY: sa_types.LargeBinary,
}

for attr in ("TIMESTAMP2", "DATETIME2", "TIME2"):
    code = getattr(FIELD_TYPE, attr, None)
    if code is not None:
        base_attr = attr.rstrip("2")
        FIELD_TYPE_MAP[code] = FIELD_TYPE_MAP[getattr(FIELD_TYPE, base_attr)]


class MySQLDialect(Dialect):
    @property
    def sqlglot_dialect(self) -> str | None:
        return "mysql"

    @property
    def open_quote(self) -> str:
        return "`"

    @property
    def close_quote(self) -> str:
        return "`"

    def default_case_fold(self, name: str) -> str:
        return name

    def server_timezone(self, engine: Any) -> tzinfo | None:
        with engine.connect() as connection:
            row = connection.execute(
                text("select @@global.time_zone, @@session.time_zone")
            ).fetchone()
        if row and row[0] is not None:
            tz_value = row[0]
            if tz_value == "SYSTEM":
                with engine.connect() as connection:
                    row = connection.execute(
                        text("select timediff(now(), utc_timestamp())")
                    ).fetchone()
                if row and row[0] is not None:
                    return parse_server_timezone(row[0])
                return None
            return parse_server_timezone(tz_value)
        return None

    def is_large_column(self, satype: Any) -> bool:
        return isinstance(satype, MYSQL_LARGE_COLUMN_TYPES)

    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        rows = connection.execute(
            text(
                "select column_name "
                "from information_schema.columns "
                "where table_schema = :schema "
                "and table_name = :table "
                "order by ordinal_position"
            ),
            {
                "schema": schema,
                "table": table,
            },
        ).fetchall()
        return [row[0] for row in rows]

    def cdc_to_sa(self, ingester_type: str, cdc_dtype: Any) -> sa_types.TypeEngine:
        match ingester_type:
            case "binlog":
                return self.cdc_to_sa_binlog(cdc_dtype)
            case _:
                raise NotImplementedError(
                    f"{type(self).__name__}.cdc_to_sa is not implemented "
                    f"for ingester type {ingester_type!r}"
                )

    @staticmethod
    def cdc_to_sa_binlog(cdc_type: Any) -> sa_types.TypeEngine:
        column_type = getattr(cdc_type, "type", None)
        column_class = FIELD_TYPE_MAP.get(column_type)
        if column_class is None:
            raise ValueError(
                f"Unsupported MySQL binlog cdc type: '{cdc_type!r}' - '{column_type!r}'"
            )
        if column_class is sa_types.Numeric:
            precision = getattr(cdc_type, "precision", None)
            decimals = getattr(cdc_type, "decimals", None)
            kw: dict[str, Any] = {}
            if precision is not None:
                kw["precision"] = precision
            if decimals is not None:
                kw["scale"] = decimals
            return sa_types.Numeric(**kw)
        if column_class is sa_types.String:
            max_length = getattr(cdc_type, "max_length", None)
            if max_length:
                return sa_types.String(length=max_length)
            return sa_types.String()
        if column_class in (DATETIME, TIME, TIMESTAMP):
            fsp = getattr(cdc_type, "fsp", None)
            if fsp is not None:
                return column_class(fsp=fsp)
            return column_class()
        if column_class is ENUM:
            enum_values = getattr(cdc_type, "enum_values", None)
            if enum_values:
                return ENUM(*enum_values)
            return ENUM()
        if column_class is SET:
            set_values = getattr(cdc_type, "set_values", None)
            if set_values:
                return SET(*set_values)
            return SET()
        if column_class is sa_types.Float:
            size = getattr(cdc_type, "size", None)
            if size:
                return sa_types.Float(precision=size)
            return sa_types.Float()
        return column_class()
