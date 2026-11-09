#
# Copyright 2026 Tabsdata Inc.
#

import contextlib
import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import TYPE_CHECKING, Any, Callable, Iterator, Literal

import polars as pl
from sqlalchemy import BigInteger, Connection, Numeric, cast
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.types import TypeDecorator
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
from tabsdatak.conn.common.sql._engine.writer import SqlWriteCfg, SqlWriteEngine

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._schema.delta import SchemaChange

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
        tables: list[pl.LazyFrame],
    ) -> None:
        write_loader = self.loader()
        frames = list(tables)
        if len(frames) != len(dest.target_tables):
            raise ValueError(
                f"tables count {len(frames)} must equal target_tables "
                f"count {len(dest.target_tables)}"
            )

        def _run() -> None:
            logger.debug("sql-writer: establishing engine")
            engine = dest.create_sql_alchemy_engine()
            try:
                if dest.transactional:
                    with Session(engine) as session, session.begin():
                        conn = session.connection()
                        self._run_writes(write_loader, conn, dest, frames)
                else:
                    with engine.connect() as conn:
                        self._run_writes(write_loader, conn, dest, frames)
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
        frames: list[pl.LazyFrame],
    ):
        chunk_size = _config.get_cfg(
            dest.cfg,
            const.WRITE_CHUNK_SIZE_KEY,
            const.WRITE_CHUNK_SIZE_DEFAULT,
        )
        total = len(dest.target_tables)
        logger.info("sql-writer: writing %d table(s)", total)
        for i, (frame, table) in enumerate(
            zip(frames, dest.target_tables),
            1,
        ):
            logger.info("sql-writer: writing table %d/%d: %r", i, total, table)
            write_loader(
                connection=conn,
                frame=frame,
                table=table,
                if_table_exists=dest.if_table_exists,
                chunk_size=chunk_size,
                schema_evolution=dest.schema_evolution,
            )
            logger.debug("sql-writer: finished table %r", table)
        logger.info("sql-writer: finished writing %d table(s)", total)


@contextlib.contextmanager
def _unmasked_ibm_db_errors() -> Iterator[None]:
    """Stop ibm_db from masking a failed executemany's real error.

    When ``ibm_db_dbi.Cursor.executemany`` fails, its except handler calls
    ``_set_rowcount()`` -> ``ibm_db.num_rows()`` on the already-failed statement
    handle. That itself raises ``CLI0125E`` ("Function sequence error", SQLSTATE
    HY010) -- and, being raised from the except block *before* the original error
    is re-raised, it REPLACES the real error (e.g. a binding error) with a
    meaningless ``CLI0125E``. So every DB2 executemany failure surfaces the
    driver's cleanup crash instead of its cause.

    Neutralize ``_set_rowcount``'s own failure for the duration of the write so
    the driver re-raises the original error. It resets the rowcount to ``-1``
    before the ``num_rows`` read, and DB2 rowcounts are never consumed here (see
    :func:`_insert_without_rowcount`), so swallowing that read is harmless.
    """
    try:
        import ibm_db_dbi
    except ImportError:
        yield
        return

    cursor_cls = ibm_db_dbi.Cursor
    original = cursor_cls._set_rowcount

    def _guarded(self):
        try:
            return original(self)
        except Exception:
            return True

    cursor_cls._set_rowcount = _guarded
    try:
        yield
    finally:
        cursor_cls._set_rowcount = original


def _insert_without_rowcount(pd_table, conn, keys, data_iter) -> None:
    # ibm_db raises CLI0125E "Function sequence error" (SQLSTATE HY010) when
    # cursor.rowcount is read after an executemany INSERT. Mirror pandas' default
    # insert (SQLTable._execute_insert) but skip the rowcount read; pandas
    # tolerates a None return. The unmask guard keeps a *failed* executemany from
    # reporting the same CLI0125E in place of its real cause.
    data = [dict(zip(keys, row)) for row in data_iter]
    with _unmasked_ibm_db_errors():
        conn.execute(pd_table.table.insert(), data)


def _write_database_options(connection: Connection) -> dict[str, Any]:
    # ibm_db_sa needs the rowcount-free insert method above; every other dialect
    # keeps pandas' default insert (real rowcounts), unchanged.
    if connection.dialect.name == "ibm_db_sa":
        return {"engine_options": {"method": _insert_without_rowcount}}
    return {}


# (max_precision, max_scale) per SQLAlchemy dialect for DECIMAL/NUMERIC. Arrow
# decimal128 sources carry precision <= 38; DB2 (31) and MySQL (scale 30) are the
# only targets below that, so a faithful map would emit an invalid DDL there.
_DECIMAL_MAX: dict[str, tuple[int, int]] = {
    "ibm_db_sa": (31, 31),
    "oracle": (38, 38),
    "mssql": (38, 38),
    "mysql": (65, 30),
    "postgresql": (1000, 1000),
}


def _decimal_dtype_overrides(connection: Connection, arrow_schema) -> dict:
    # pandas' create-on-write does not map an Arrow decimal column to a numeric
    # column (on DB2 it lands as CLOB, so the bulk INSERT can't bind the Decimal
    # values). Force every decimal column to SQLAlchemy Numeric so the table is
    # created as DECIMAL(precision, scale). Precision/scale are clamped to the
    # target's maximum (see _DECIMAL_MAX) -- a stopgap that keeps writes working
    # where the source precision exceeds the engine's limit (e.g. decimal(38,10)
    # -> DECIMAL(31,10) on DB2), pending a fuller Arrow -> SQLAlchemy type map.
    limits = _DECIMAL_MAX.get(connection.dialect.name)
    if limits is None:
        return {}
    import pyarrow as pa

    max_precision, max_scale = limits
    overrides: dict[str, Any] = {}
    for field in arrow_schema:
        if not pa.types.is_decimal(field.type):
            continue
        precision = min(field.type.precision, max_precision)
        # Scale can exceed neither the clamped precision nor the engine's max.
        scale = min(field.type.scale, max_scale, precision)
        numeric = _ExactDecimal if connection.dialect.name == "ibm_db_sa" else Numeric
        overrides[field.name] = numeric(precision, scale)
    return overrides


class _ExactDecimal(TypeDecorator):
    """A DECIMAL whose values reach the driver as they are.

    ibm_db_sa declares no native decimal support, so SQLAlchemy turns every
    Decimal into a float on the way in and a DB2 DECIMAL keeps only the float's
    digits. The ibm_db driver binds a Decimal exactly.
    """

    impl = Numeric
    cache_ok = True

    def bind_processor(self, dialect):
        return None


class _CheckedInteger(TypeDecorator):
    """A BIGINT whose values reach DB2 as BIGINT.

    ibm_db binds a value as the type of the column it lands in, and cuts one an
    INTEGER or SMALLINT column cannot hold to its low bits (2**40 is stored as
    0). Bound as a BIGINT, DB2 converts it itself and refuses the overflow
    (SQL0413N), as every other database does.
    """

    impl = BigInteger
    cache_ok = True

    def bind_expression(self, bindvalue):
        return cast(bindvalue, BigInteger)


# The date a time-of-day column carries on a target with no time type. Arbitrary
# but fixed, and documented in TYPE_MAPPING.md: a reader drops it to recover the
# original time.
_SENTINEL_DATE = date(1970, 1, 1)


def _sentinel_datetime(column: str):
    """`column`'s time-of-day as a datetime on `_SENTINEL_DATE`. polars refuses to
    cast Time to Datetime directly, so the date is combined in."""
    return pl.lit(_SENTINEL_DATE).dt.combine(pl.col(column))


def _dialect_type_fixes(
    connection: Connection, arrow_schema
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The SQLAlchemy type to force per column, plus any value transform that type
    needs, for the columns pandas' inference maps wrongly on this dialect.

    pandas picks a type from the (polars -> pandas) column alone, so it cannot know
    what the target accepts:

    * mssql -- a tz-aware timestamp becomes a generic ``TIMESTAMP``, which T-SQL
      reads as ``ROWVERSION``: an auto-generated row counter that rejects every
      insert. ``DATETIMEOFFSET`` is the tz-aware column SQL Server actually has.
    * oracle -- pandas' ``Float`` carries decimal precision, which the Oracle
      dialect refuses to compile ("FLOAT types use binary precision"), so float
      columns never reach the database. ``BINARY_FLOAT`` / ``BINARY_DOUBLE`` are
      Oracle's native IEEE types.
    * oracle -- there is no time-of-day type, and oracledb rejects a Python
      ``time`` whatever the target column is (DPY-3002), so the column is stored
      as a ``TIMESTAMP`` on `_SENTINEL_DATE`.
    * ibm_db_sa -- an integer is bound as the column it lands in, and a value
      that column cannot hold is silently cut (`_CheckedInteger`).

    A transform is keyed off the type being forced rather than off the dialect, so
    a user-supplied override resolving to the same type reuses it.
    """
    import pyarrow as pa
    from sqlalchemy.dialects import mssql, oracle

    dialect = connection.dialect.name
    dtype: dict[str, Any] = {}
    transforms: dict[str, Any] = {}
    for field in arrow_schema:
        name, arrow = field.name, field.type
        if dialect == "mssql" and pa.types.is_timestamp(arrow) and arrow.tz:
            dtype[name] = mssql.DATETIMEOFFSET()
        elif dialect == "oracle" and pa.types.is_float32(arrow):
            dtype[name] = oracle.BINARY_FLOAT()
        elif dialect == "oracle" and pa.types.is_float64(arrow):
            dtype[name] = oracle.BINARY_DOUBLE()
        elif dialect == "oracle" and (
            pa.types.is_time32(arrow) or pa.types.is_time64(arrow)
        ):
            dtype[name] = oracle.TIMESTAMP()
            transforms[name] = _sentinel_datetime(name)
        elif dialect == "ibm_db_sa" and pa.types.is_integer(arrow):
            dtype[name] = _CheckedInteger()
    return dtype, transforms


# Db2 takes TRUNCATE ... IMMEDIATE only as the first statement of a unit of
# work, and SQLite has no TRUNCATE.
_EMPTY_TABLE: dict[str, str] = {
    "ibm_db_sa": "DELETE FROM {table}",
    "sqlite": "DELETE FROM {table}",
}


def _empty_table_if_exists(connection: Connection, table: str) -> None:
    from sqlalchemy import inspect, text

    # Mirror polars.write_database's [schema.]table unpacking so the statement
    # targets the same identifier polars/pandas would CREATE. has_table is
    # identifier-aware rather than MetaData.reflect, so it never raises Oracle's
    # "requested table(s) not available".
    *prefix, name = table.split(".")
    schema = prefix[-1] if prefix else None
    if not inspect(connection).has_table(name, schema=schema):
        return
    preparer = connection.dialect.identifier_preparer
    target = ".".join(preparer.quote(part) for part in [*prefix, name])
    statement = _EMPTY_TABLE.get(connection.dialect.name, "TRUNCATE TABLE {table}")
    connection.execute(text(statement.format(table=target)))


# Dialects whose `default_schema_name` is not the schema an unqualified CREATE
# lands in, and the query that answers it for the live session. ibm_db_sa derives
# its default from the authorization ID (the connect user), while statements run
# under CURRENT SCHEMA -- which the connection URL sets via `?currentSchema=`.
# Existence is resolved through has_table both by the DROP below and by pandas'
# create-on-write, so with an unqualified name they probe the connect user's
# schema, miss a table that is really there, and the CREATE that follows fails
# with SQL0601N.
_SESSION_SCHEMA_QUERY: dict[str, str] = {"ibm_db_sa": "VALUES CURRENT SCHEMA"}


def _qualify_for_write(connection: Connection, table: str) -> str:
    """`table` qualified with the schema this session writes into, for dialects
    whose own default schema disagrees with it. Already-qualified names and every
    other dialect are returned untouched."""
    query = _SESSION_SCHEMA_QUERY.get(connection.dialect.name)
    if query is None or "." in table:
        return table
    from sqlalchemy import text

    # DB2 returns the special register space-padded.
    schema = (connection.execute(text(query)).scalar() or "").strip()
    return f"{schema}.{table}" if schema else table


# Dialects that match a column name whatever its case, quoted or not. Elsewhere
# SQLAlchemy reflects a name in the same form pandas writes it, so the two are
# compared as they are.
_CASE_INSENSITIVE_COLUMNS = frozenset({"mysql", "mssql"})


def _evolve_columns(
    connection: Connection,
    table: str,
    frame: pl.LazyFrame,
    dtype: dict[str, Any],
    evolution: Literal["strict", "update"],
) -> None:
    """Bring an existing `table` up to the batch as far as `evolution` allows:
    add the columns the batch has and the table lacks, and widen a number
    column the batch has outgrown. A column takes the type pandas would give it
    in a table it creates, so both paths agree. Under strict, a number column
    the batch has outgrown is taken as it is when every value of the batch fits
    it, and refused otherwise.

    A column the batch lacks is left alone and written NULL by the insert. Other
    type differences are left to the database: a value the column cannot hold
    fails the insert.
    """
    from pandas.io.sql import SQLDatabase, SQLTable
    from sqlalchemy import Boolean, inspect, text

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.widening import outgrows

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._schema.delta import (
        Action,
        SchemaChange,
        SchemaDelta,
    )

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._schema.policy import governed

    schema = frame.collect_schema()
    *prefix, name = table.split(".")
    db_schema = prefix[-1] if prefix else None
    inspector = inspect(connection)
    if not inspector.has_table(name, schema=db_schema):
        return
    dialect = connection.dialect
    insensitive = dialect.name in _CASE_INSENSITIVE_COLUMNS

    def fold(column: str) -> str:
        return column.lower() if insensitive else column

    stored = {
        fold(column["name"]): column
        for column in inspector.get_columns(name, schema=db_schema)
    }
    empty = pl.DataFrame(schema=schema).to_pandas(use_pyarrow_extension_array=True)
    with SQLDatabase(connection, schema=db_schema) as database:
        columns = SQLTable(
            name,
            database,
            frame=empty,
            index=False,
            schema=db_schema,
            dtype=dtype or None,
        ).table.columns
    changes = []
    for column in schema.names():
        batch_type = columns[column].type
        spelling = batch_type.compile(dialect=dialect)
        existing = stored.get(fold(column))
        if existing is None:
            changes.append(
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column=column,
                    safe=True,
                    detail={"type": spelling},
                )
            )
        elif not isinstance(batch_type, Boolean):
            current = existing["type"].compile(dialect=dialect)
            if outgrows(current, spelling, dialect.name):
                changes.append(
                    SchemaChange(
                        action=Action.ALTER_TYPE,
                        column=existing["name"],
                        safe=True,
                        detail={
                            "old_type": current,
                            "new_type": spelling,
                            "nullable": existing.get("nullable", True),
                        },
                    )
                )
    if not changes:
        return
    if evolution == "strict":
        changes = _fitting(frame, changes, dialect.name)
    delta = governed(
        SchemaDelta(table_name=table, is_create=False, changes=changes), evolution
    )
    preparer = dialect.identifier_preparer
    target = ".".join(preparer.quote(part) for part in [*prefix, name])
    retyped = False
    for change in delta.changes:
        column = preparer.quote(change.column)
        if change.action is Action.ADD_COLUMN:
            clause = _ADD_COLUMN.get(dialect.name, "ADD COLUMN {column} {type}")
            statement = clause.format(column=column, type=change.detail["type"])
        else:
            clause = _ALTER_TYPE.get(dialect.name, "ALTER COLUMN {column} TYPE {type}")
            null = "NULL" if change.detail["nullable"] else "NOT NULL"
            statement = clause.format(
                column=column, type=change.detail["new_type"], null=null
            )
            retyped = True
        statement = f"ALTER TABLE {target} {statement}"
        logger.info("sql-writer: evolving %s: %s", table, statement)
        connection.execute(text(statement))
    if retyped and dialect.name == "ibm_db_sa":
        # DB2 leaves a table whose column type changed in reorg-pending state,
        # refusing inserts until it is reorganized.
        connection.execute(
            text("CALL SYSPROC.ADMIN_CMD(:command)"),
            {"command": f"REORG TABLE {target}"},
        )


def _fitting(
    frame: pl.LazyFrame, changes: list["SchemaChange"], dialect: str
) -> list["SchemaChange"]:
    """``changes`` with each widening marked as fitting when every value of the
    batch fits the column as it is: an integer or decimal within the column's
    range (:func:`holds`, read off the smallest and largest value), a double
    that single precision holds exactly (:func:`single_holds_double`). The
    batch is read for those columns only, in one pass."""
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.widening import holds, single_holds_double

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._schema.delta import Action, SchemaChange

    retyped = [change for change in changes if change.action is Action.ALTER_TYPE]
    checks = {
        change.column: check
        for change in retyped
        if (
            check := holds(
                change.detail["old_type"], change.detail["new_type"], dialect
            )
        )
    }
    singles = {
        change.column
        for change in retyped
        if single_holds_double(
            change.detail["old_type"], change.detail["new_type"], dialect
        )
    }
    if not checks and not singles:
        return changes
    facts = (
        frame
        .select(
            *(pl.col(column).min().alias(f"{column}\0min") for column in checks),
            *(pl.col(column).max().alias(f"{column}\0max") for column in checks),
            *(
                (
                    pl.col(column).is_null()
                    | pl.col(column).is_nan()
                    | (
                        pl.col(column).cast(pl.Float32).cast(pl.Float64)
                        == pl.col(column)
                    )
                )
                .all()
                .alias(f"{column}\0exact")
                for column in singles
            ),
        )
        .collect()
        .row(0, named=True)
    )

    def fits(column: str) -> bool:
        if column in singles:
            return bool(facts[f"{column}\0exact"])
        check = checks.get(column)
        low, high = facts.get(f"{column}\0min"), facts.get(f"{column}\0max")
        return bool(check) and (low is None or (check(low) and check(high)))

    fitting = []
    for change in changes:
        if change.action is Action.ALTER_TYPE and fits(change.column):
            change = SchemaChange(
                action=change.action,
                column=change.column,
                safe=change.safe,
                detail={**change.detail, "fits": True},
            )
        fitting.append(change)
    return fitting


_ADD_COLUMN: dict[str, str] = {
    "mssql": "ADD {column} {type}",
    "oracle": "ADD ({column} {type})",
}

_ALTER_TYPE: dict[str, str] = {
    "mysql": "MODIFY COLUMN {column} {type} {null}",
    "mssql": "ALTER COLUMN {column} {type} {null}",
    "oracle": "MODIFY ({column} {type})",
    "ibm_db_sa": "ALTER COLUMN {column} SET DATA TYPE {type}",
}


def _polars_write(
    *,
    connection: Connection,
    frame: pl.LazyFrame,
    table: str,
    if_table_exists: Literal["append", "replace"],
    chunk_size: int,
    schema_evolution: Literal["strict", "update"] = "strict",
) -> None:
    schema = frame.collect_schema()
    arrow_schema = pl.DataFrame(schema=schema).to_arrow().schema
    table = _qualify_for_write(connection, table)
    options = _write_database_options(connection)
    # Force decimal columns to a (clamped) DECIMAL on create, across dialects, and
    # fix the types this dialect cannot take from pandas' inference.
    _dtype = _decimal_dtype_overrides(connection, arrow_schema)
    _fixes, _transforms = _dialect_type_fixes(connection, arrow_schema)
    _dtype.update(_fixes)
    if _dtype:
        options.setdefault("engine_options", {})["dtype"] = _dtype
    # A replace keeps the table: it evolves it like an append, empties it, and
    # appends. pandas' own 'replace' would drop and recreate the table, and
    # reflects it first, which fails on Oracle -- its identifier
    # normalize/denormalize asymmetry makes the reflect's name-membership check
    # miss the table ("requested table(s) not available").
    _evolve_columns(connection, table, frame, _dtype, schema_evolution)
    if if_table_exists == "replace":
        _empty_table_if_exists(connection, table)
        if_table_exists = "append"
    wrote_any = False
    for df in frame.collect_batches(chunk_size=chunk_size):
        if _transforms:
            df = df.with_columns(**_transforms)
        df.write_database(
            table_name=table,
            connection=connection,
            if_table_exists=if_table_exists,
            **options,
        )
        if_table_exists = "append"
        wrote_any = True

    if not wrote_any:
        # No rows: still create / preserve the table with the caller's
        # if_table_exists semantics by writing an empty frame of the schema.
        df = pl.DataFrame(schema=schema)
        if _transforms:
            df = df.with_columns(**_transforms)
        df.write_database(
            table_name=table,
            connection=connection,
            if_table_exists=if_table_exists,
            **options,
        )


class PolarsWriteEngine(SqlAlchemyWriteEngine):
    def loader(self) -> WriteLoader:
        return _polars_write
