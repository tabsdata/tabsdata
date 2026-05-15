from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from sqlalchemy.exc import OperationalError, SQLAlchemyError

from tabsdatak.conn.common.sql import _sqlalchemy
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeConn, SnowflakeDest
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    TableFileSpec,
)


def _conn_args(conn: SnowflakeConn) -> dict[str, Any]:
    return dict(conn.conn_cfg or {})


def _credentials(conn: SnowflakeConn) -> UserPassword:
    creds = conn.credentials
    if not isinstance(creds, UserPassword):
        raise TypeError(
            f"SnowflakeConn.credentials must be UserPassword, got "
            f"{type(creds).__name__}"
        )
    return creds


def _build_uri(conn: SnowflakeConn) -> str:
    account = quote_plus(conn.account.value())
    path = ""
    if conn.database is not None:
        path = "/" + quote_plus(conn.database.value())
        if conn.schema is not None:
            path += "/" + quote_plus(conn.schema.value())

    params: list[str] = []
    if conn.warehouse is not None:
        params.append(f"warehouse={quote_plus(conn.warehouse.value())}")
    if conn.role is not None:
        params.append(f"role={quote_plus(conn.role.value())}")
    query = ("?" + "&".join(params)) if params else ""

    return f"snowflake://{account}{path}{query}"


class _SnowflakeDestPlugin(DestPlugin[SnowflakeDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: SnowflakeDest,
        tables: list[TableFileSpec],
    ) -> None:
        if len(tables) != len(dest.tables):
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_8.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        loader, chunk_size = _sqlalchemy.extract_cfg(
            dest.dest_cfg,
            where="SnowflakeDest.dest_cfg",
        )

        conn: SnowflakeConn = dest.conn  # type: ignore[assignment]

        try:
            _sqlalchemy.write_tables(
                uri=_build_uri(conn),
                credentials=_credentials(conn),
                tables=tables,
                target_tables=list(dest.tables),
                if_table_exists=dest.if_table_exists,
                chunk_size=chunk_size,
                loader=loader,
                connect_args=_conn_args(conn),
            )
        except OperationalError as e:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_6.exception(
                cause=e,
                account=conn.account.value(),
            )
        except SQLAlchemyError as e:
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_7.exception(
                cause=e,
                table=", ".join(dest.tables),
            )


class _SnowflakeExplorer(Explorer[SnowflakeConn]):
    def metadata(self, conn: SnowflakeConn) -> Any:
        # TODO
        pass

    def data(self, conn: SnowflakeConn, query: str) -> Any:
        # TODO
        pass


SNOWFLAKE_DEST = DestDef(
    conn=SnowflakeConn,
    dest_type="snowflake",
    dest_version="v1",
    dest=SnowflakeDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_SnowflakeDestPlugin,
    explorer=_SnowflakeExplorer,
)
