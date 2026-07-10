#
#  Copyright 2026. Tabs Data Inc.
#

"""ClickHouse metadata explorer."""

from tabsdatak.conn.clickhouse import ClickHouseDestConn
from tabsdatak.conn.common._explorer import Column
from tabsdatak.conn.common.sql._explorer import SQLEntity, SqlAlchemyConnectionExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert


class ClickHouseConnectionExplorer(SqlAlchemyConnectionExplorer):
    """ClickHouse metadata via the clickhouse-connect native client.

    Reuses the shared :class:`SqlAlchemyConnectionExplorer` entity model
    (``SQLConnMetaModel`` / ``SQLEntity``) but replaces its SQLAlchemy
    introspection with direct ``clickhouse_connect`` queries against the
    ``system.tables`` / ``system.columns`` catalog.
    """

    def check_connect(self, conn: ClickHouseDestConn) -> None:
        client = _client(conn)
        try:
            # Smallest possible round-trip: a scalar SELECT that fails if
            # the host is unreachable or the credentials are rejected.
            client.command("SELECT 1")
        finally:
            client.close()

    def metadata(self, conn: ClickHouseDestConn) -> list[SQLEntity]:
        database = _convert(conn.database, str) or "default"
        client = _client(conn)
        try:
            # ``system.tables`` includes views (engine = 'View'), so this
            # one query covers both tables and views in the target db.
            tables = client.query(
                "SELECT name FROM system.tables "
                "WHERE database = {db:String} ORDER BY name",
                parameters={"db": database},
            ).result_rows

            entities: list[SQLEntity] = []
            for (table_name,) in tables:
                columns = self._columns(client, database, table_name)
                entities.append(
                    SQLEntity(
                        connection_type=self.meta_model,
                        # ClickHouse's two-level namespace (database.table)
                        # maps onto ``database``; there is no schema level.
                        database=database,
                        schema="",
                        table=table_name,
                        columns=columns,
                    )
                )
            return entities
        finally:
            client.close()

    def data(
        self, conn: ClickHouseDestConn, entity: SQLEntity, limit: int = 100
    ) -> list[dict]:
        database = entity.database or (_convert(conn.database, str) or "default")
        client = _client(conn)
        try:
            fqn = f"`{database}`.`{entity.table}`"
            # ``limit`` is coerced to int so it can't smuggle SQL; the
            # identifiers come from our own metadata scan (system.tables).
            result = client.query(f"SELECT * FROM {fqn} LIMIT {int(limit)}")
            return [dict(row) for row in result.named_results()]
        finally:
            client.close()

    def make_url(self, conn: ClickHouseDestConn) -> str:
        # This explorer bypasses SQLAlchemy entirely (see the module
        # docstring), so no engine URL is ever built. Retained only to
        # satisfy the abstract base; a call here is a wiring mistake.
        raise NotImplementedError(
            "ClickHouseConnectionExplorer talks to clickhouse-connect "
            "directly; make_url is not used."
        )

    @staticmethod
    def _columns(client, database: str, table_name: str) -> list[Column]:
        rows = client.query(
            "SELECT name, type, is_in_primary_key, comment "
            "FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            parameters={"db": database, "tbl": table_name},
        ).result_rows

        columns: list[Column] = []
        for name, ctype, in_pk, comment in rows:
            simple_type, nullable = _split_type(ctype)
            columns.append(
                Column(
                    name=name,
                    type=simple_type,
                    nullable=nullable,
                    primary_key=bool(in_pk),
                    description=comment or "",
                )
            )
        return columns


def _client(conn: ClickHouseDestConn):
    # clickhouse-connect is imported lazily so merely importing this
    # module (e.g. a registry scan) doesn't pay for the driver.
    import clickhouse_connect

    user = password = ""
    if conn.credentials is not None:
        user = conn.credentials.user.value()
        password = conn.credentials.password.value()
    return clickhouse_connect.get_client(
        host=_convert(conn.host, str) or "localhost",
        port=_convert(conn.port, int),
        database=_convert(conn.database, str) or "default",
        username=user,
        password=password,
        secure=_convert(conn.secure, bool),
    )


def _split_type(raw: str) -> tuple[str, bool]:
    """Split a ClickHouse column type into ``(canonical_type, nullable)``.

    ClickHouse encodes nullability in the type (``Nullable(String)``)
    rather than a separate flag, and parameterises many types
    (``DateTime64(3)``, ``Decimal(18, 4)``, ``FixedString(16)``). Unwrap
    the ``Nullable(...)`` marker and drop the parameter list, keeping the
    native ClickHouse spelling (e.g. ``UInt64``, ``DateTime64``).
    """
    t = raw.strip()
    nullable = t.startswith("Nullable(") and t.endswith(")")
    if nullable:
        t = t[len("Nullable(") : -1].strip()
    if "(" in t:
        t = t[: t.index("(")]
    return t.strip(), nullable
