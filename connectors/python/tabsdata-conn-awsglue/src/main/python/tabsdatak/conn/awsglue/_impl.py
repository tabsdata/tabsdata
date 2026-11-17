#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import polars as pl

from tabsdatak.common._storage.transfer import (
    AWS_ACCESS_KEY_ID_OPTION,
    AWS_REGION_OPTION,
    AWS_SECRET_ACCESS_KEY_OPTION,
)
from tabsdatak.conn.awsglue import AwsGlueDestConn

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue._validation import table_parts
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode
from tabsdatak.conn.common.file._store import FileStore, forwarded, text
from tabsdatak.conn.common.sql.error import SqlCommonErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.error import ApiException

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table
    from pyiceberg.types import IcebergType

logger = logging.getLogger(__name__)

CATALOG_NAME = "awsglue"
PYICEBERG_PREFIX = "pyiceberg."
# pyiceberg hands these to botocore as they come, and botocore wants numbers.
NUMERIC_PROPERTIES = frozenset({"glue.max-retries"})
# The snapshot summary property holding the id of the trx a write belongs to.
TRX_PROPERTY = "tabsdata.trx-id"
DATA_FOLDER = "data"
S3_SCHEMES = ("s3", "s3a", "s3n")

_INT32 = (pl.Int8, pl.Int16, pl.UInt8, pl.UInt16)
_INT64 = (pl.UInt32, pl.UInt64)
_KEPT = (
    pl.Int32,
    pl.Int64,
    pl.Float32,
    pl.Float64,
    pl.Boolean,
    pl.String,
    pl.Binary,
    pl.Date,
    pl.Decimal,
)


@dataclass(frozen=True)
class Change:
    """A column the table gains (``before`` is `None`) or a type it widens."""

    column: str
    before: IcebergType | None
    after: IcebergType

    def __str__(self) -> str:
        if self.before is None:
            return f"new column {self.column!r}"
        return f"column {self.column!r} from {self.before} to {self.after}"


@dataclass(frozen=True)
class WriteCfg:
    conn: AwsGlueDestConn
    tables: list[str]
    if_table_exists: str
    schema_evolution: str
    transaction_id: str


# --- Clients ----------------------------------------------------------


def credentials(conn: AwsGlueDestConn) -> tuple[str, str]:
    try:
        return (
            _convert(conn.credentials.access_key_id, str),
            _convert(conn.credentials.secret_access_key, str),
        )
    except ApiException as e:
        raise AwsGlueErrorCode.AWSGLUE_7.exception(cause=e, reason=str(e))


def catalog_properties(conn: AwsGlueDestConn) -> dict[str, str | int]:
    access_key_id, secret_access_key = credentials(conn)
    region = conn._region()
    properties = {
        "glue.region": region,
        "glue.access-key-id": access_key_id,
        "glue.secret-access-key": secret_access_key,
        "s3.region": region,
        "s3.access-key-id": access_key_id,
        "s3.secret-access-key": secret_access_key,
    }
    catalog_id = conn._catalog_id()
    if catalog_id is not None:
        properties["glue.id"] = catalog_id
    for key, value in (conn.conn_cfg or {}).items():
        if key.startswith(PYICEBERG_PREFIX):
            name = key.removeprefix(PYICEBERG_PREFIX)
            properties[name] = int(value) if name in NUMERIC_PROPERTIES else text(value)
    return properties


def glue_catalog(conn: AwsGlueDestConn) -> Catalog:
    # Built directly rather than through load_catalog, which would merge in
    # the .pyiceberg.yaml and PYICEBERG_* settings of the host.
    from pyiceberg.catalog.glue import GlueCatalog

    return GlueCatalog(CATALOG_NAME, **catalog_properties(conn))


def data_target(
    conn: AwsGlueDestConn, table: str, location: str
) -> tuple[FileStore, str]:
    """The store and the key prefix the data files of the table at ``location``
    are written under."""
    parsed = urlparse(location)
    if parsed.scheme not in S3_SCHEMES:
        raise AwsGlueErrorCode.AWSGLUE_14.exception(table=table, location=location)
    root = f"s3://{parsed.netloc}"
    access_key_id, secret_access_key = credentials(conn)
    options = {
        AWS_ACCESS_KEY_ID_OPTION: access_key_id,
        AWS_SECRET_ACCESS_KEY_OPTION: secret_access_key,
        AWS_REGION_OPTION: conn._region(),
    }
    store = FileStore(root, {**options, **forwarded(root, conn.conn_cfg)})
    return store, parsed.path.strip("/")


# --- Names ------------------------------------------------------------


def identifier(conn: AwsGlueDestConn, table: str) -> tuple[str, str]:
    database, name = table_parts(table)
    database = database or conn._database()
    if database is None:
        raise AwsGlueErrorCode.AWSGLUE_8.exception(table=table)
    return database, name


def new_table_location(conn: AwsGlueDestConn, database: str, name: str) -> str:
    parts = [conn._bucket(), conn._base_path().strip("/"), database, name]
    return "s3://" + "/".join(part for part in parts if part)


# --- Types ------------------------------------------------------------


def _iceberg_dtype(dtype: pl.DataType) -> pl.DataType | None:
    """The type ``dtype`` is written as, or `None` when Iceberg has no type to
    hold it."""
    if isinstance(dtype, _INT32):
        return pl.Int32()
    if isinstance(dtype, _INT64):
        return pl.Int64()
    if isinstance(dtype, (pl.Categorical, pl.Enum)):
        return pl.String()
    if isinstance(dtype, pl.Datetime):
        return pl.Datetime("us", None if dtype.time_zone is None else "UTC")
    if isinstance(dtype, (pl.List, pl.Array)):
        inner = _iceberg_dtype(dtype.inner)
        return None if inner is None else pl.List(inner)
    if isinstance(dtype, pl.Struct):
        fields = [(field.name, _iceberg_dtype(field.dtype)) for field in dtype.fields]
        if any(inner is None for _, inner in fields):
            return None
        return pl.Struct([pl.Field(name, inner) for name, inner in fields])
    if isinstance(dtype, _KEPT):
        return dtype
    return None


def iceberg_frame(frame: pl.LazyFrame) -> pl.LazyFrame:
    """``frame`` with every column in a type Iceberg holds: the integers
    Iceberg lacks widened to the next signed one, timestamps in microseconds
    and, when zoned, in UTC, and categories as text."""
    casts = []
    for column, dtype in frame.collect_schema().items():
        target = _iceberg_dtype(dtype)
        if target is None:
            raise AwsGlueErrorCode.AWSGLUE_12.exception(column=column, dtype=dtype)
        if target != dtype:
            casts.append(pl.col(column).cast(target))
    return frame.with_columns(casts) if casts else frame


def arrow_schema(frame: pl.LazyFrame) -> pa.Schema:
    return pl.DataFrame(schema=frame.collect_schema()).to_arrow().schema


# --- Table ------------------------------------------------------------


def target(
    catalog: Catalog, conn: AwsGlueDestConn, name: tuple[str, str], schema: pa.Schema
) -> Table:
    """The table ``name``, created from ``schema`` when it does not exist."""
    from pyiceberg.exceptions import (
        NoSuchIcebergTableError,
        NoSuchNamespaceError,
        NoSuchTableError,
        TableAlreadyExistsError,
    )

    try:
        return catalog.load_table(name)
    except NoSuchIcebergTableError as e:
        raise AwsGlueErrorCode.AWSGLUE_10.exception(cause=e, table=".".join(name))
    except NoSuchTableError:
        pass
    location = new_table_location(conn, *name)
    logger.info("awsglue: creating table %s at %s", ".".join(name), location)
    try:
        return catalog.create_table(name, schema=schema, location=location)
    except NoSuchNamespaceError as e:
        raise AwsGlueErrorCode.AWSGLUE_9.exception(cause=e, database=name[0])
    except TableAlreadyExistsError:
        return catalog.load_table(name)


def stamped(table: Table, transaction_id: str) -> bool:
    return any(
        snapshot.summary is not None
        and snapshot.summary.additional_properties.get(TRX_PROPERTY) == transaction_id
        for snapshot in table.metadata.snapshots
    )


def referenced(table: Table, uri: str) -> bool:
    """Whether ``uri`` is one of the table's data files: the write it belongs
    to is in the table, even once the snapshot that stamped it has expired."""
    import pyarrow.compute as pc

    if table.current_snapshot() is None:
        return False
    files = table.inspect.data_files().filter(pc.field("file_path") == uri)
    return files.num_rows > 0


def evolution(table: Table, name: str, schema: pa.Schema, policy: str) -> list[Change]:
    """The changes the table needs to hold a batch of ``schema``: the columns
    it gains and the types it widens. Empty when the batch already fits."""
    from pyiceberg.exceptions import ValidationError

    current = table.schema()
    missing = sorted(
        field.name
        for field in current.fields
        if field.required and field.name not in schema.names
    )
    if missing:
        raise AwsGlueErrorCode.AWSGLUE_13.exception(
            table=name, columns=", ".join(missing)
        )
    # Applied to a transaction that is never committed: the schema it ends up
    # with is what the table would take.
    trial = table.transaction()
    try:
        with trial.update_schema() as update:
            update.union_by_name(schema)
    except (ValidationError, ValueError) as e:
        raise refusal(name, policy, str(e), cause=e)
    before = {field.name: field.field_type for field in current.fields}
    return [
        Change(field.name, before.get(field.name), field.field_type)
        for field in trial.table_metadata.schema().fields
        if field.field_type != before.get(field.name)
    ]


def refusal(
    table: str, policy: str, changes: str, cause: BaseException | None = None
) -> Exception:
    return SqlCommonErrorCode.SQL_COMMON_10.exception(
        cause=cause, table=table, evolution=policy, changes=changes
    )


def _held_dtype(held: IcebergType) -> pl.DataType | None:
    """The type a batch column is cast to so that it fits a narrower column of
    type ``held``, or `None` when there is no such cast."""
    from pyiceberg.types import (
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
    )

    if isinstance(held, IntegerType):
        return pl.Int32()
    if isinstance(held, LongType):
        return pl.Int64()
    if isinstance(held, FloatType):
        return pl.Float32()
    if isinstance(held, DoubleType):
        return pl.Float64()
    if isinstance(held, DecimalType):
        return pl.Decimal(held.precision, held.scale)
    return None


def conformed(frame: pl.LazyFrame, name: str, changes: list[Change]) -> pl.LazyFrame:
    """``frame`` as a strict write takes it: no column the table lacks, and a
    wider column cast to the type the table holds, which fails the write when
    a value does not fit."""
    casts = []
    for change in changes:
        held = None if change.before is None else _held_dtype(change.before)
        if held is None:
            raise refusal(name, "strict", "; ".join(str(c) for c in changes))
        casts.append(pl.col(change.column).cast(held))
    return frame.with_columns(casts)


def commit(table: Table, uri: str, cfg: WriteCfg, schema: pa.Schema | None) -> None:
    """One commit: the schema change when ``schema`` is given, the emptying of
    the table on a replace, and the data file."""
    from pyiceberg.expressions import AlwaysTrue

    stamp = {TRX_PROPERTY: cfg.transaction_id}
    with table.transaction() as transaction:
        if schema is not None:
            with transaction.update_schema() as update:
                update.union_by_name(schema)
        if cfg.if_table_exists == "replace" and table.current_snapshot() is not None:
            transaction.delete(AlwaysTrue(), snapshot_properties=stamp)
        # write_table has already made sure the table does not hold the file.
        transaction.add_files(
            [uri], snapshot_properties=stamp, check_duplicate_files=False
        )


def write_table(
    catalog: Catalog, cfg: WriteCfg, name: tuple[str, str], frame: pl.LazyFrame
) -> None:
    from pyiceberg.exceptions import CommitStateUnknownException

    label = ".".join(name)
    frame = iceberg_frame(frame)
    schema = arrow_schema(frame)
    table = target(catalog, cfg.conn, name, schema)
    location = table.location().rstrip("/")
    # Named after the trx, so a rerun of a write that failed between the
    # upload and the commit overwrites the file instead of leaving a second.
    file_name = f"{DATA_FOLDER}/tabsdata-{cfg.transaction_id}.parquet"
    uri = f"{location}/{file_name}"
    if stamped(table, cfg.transaction_id) or referenced(table, uri):
        logger.info(
            "awsglue: skipping %s, already written by trx %s",
            label,
            cfg.transaction_id,
        )
        return
    if not table.spec().is_unpartitioned():
        raise AwsGlueErrorCode.AWSGLUE_11.exception(table=label)
    changes = evolution(table, label, schema, cfg.schema_evolution)
    if changes and cfg.schema_evolution == "strict":
        frame = conformed(frame, label, changes)
        changes = []
    store, prefix = data_target(cfg.conn, label, location)
    key = f"{prefix}/{file_name}" if prefix else file_name
    try:
        writer = store.open_writer(key)
        frame.sink_parquet(writer)
        writer.close()
        commit(table, uri, cfg, schema if changes else None)
    except CommitStateUnknownException:
        # The commit may have gone through, and then the table holds the file.
        raise
    except BaseException:
        discard(store, key)
        raise


def discard(store: FileStore, key: str) -> None:
    try:
        store.delete([key])
    except Exception:  # noqa: BLE001 -- the write's own failure is the one reported
        logger.warning("awsglue: could not remove %s", store.uri(key), exc_info=True)


def write_all_tables(cfg: WriteCfg, frames: list[pl.LazyFrame]) -> None:
    names = [identifier(cfg.conn, table) for table in cfg.tables]
    catalog = glue_catalog(cfg.conn)
    total = len(names)
    for position, (name, frame) in enumerate(zip(names, frames, strict=True), 1):
        logger.info("awsglue: writing table %d/%d: %s", position, total, ".".join(name))
        write_table(catalog, cfg, name, frame)
