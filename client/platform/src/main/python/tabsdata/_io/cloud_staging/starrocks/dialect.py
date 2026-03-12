#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import re
import time

import pyarrow as pa
from pyarrow import schema

from tabsdata._io.cloud_staging.common.dialect import Dialect
from tabsdata._io.cloud_staging.common.errors import SchemaError
from tabsdata._io.cloud_staging.common.modes import LoadMode
from tabsdata._io.cloud_staging.common.results import BulkLoadResult
from tabsdata._io.cloud_staging.common.statuses import BulkLoadStatus
from tabsdata._io.cloud_staging.starrocks.cloud_storage import StarRocksCloudStorage
from tabsdata._io.cloud_staging.starrocks.config import StarRocksConfig

logger = logging.getLogger(__name__)


# ── Type conversion: PyArrow → StarRocks ─────────────────────────────────────────────

_ARROW_TO_STARROCKS: dict = {
    pa.bool_(): "BOOLEAN",
    pa.int8(): "TINYINT",
    pa.int16(): "SMALLINT",
    pa.int32(): "INT",
    pa.int64(): "BIGINT",
    pa.uint8(): "SMALLINT",
    pa.uint16(): "INT",
    pa.uint32(): "BIGINT",
    pa.uint64(): "LARGEINT",
    pa.float16(): "FLOAT",
    pa.float32(): "FLOAT",
    pa.float64(): "DOUBLE",
    pa.string(): "STRING",
    pa.utf8(): "STRING",
    pa.large_string(): "STRING",
    pa.large_utf8(): "STRING",
    pa.binary(): "VARBINARY",
    pa.date32(): "DATE",
}


def _arrow_type_to_starrocks(arrow_type: pa.DataType) -> str:
    """Convert a PyArrow DataType to a StarRocks type string.

    Does not include ``NULL``/``NOT NULL`` — that is applied by
    :func:`schema_to_starrocks_columns` based on each field's nullability flag.
    """
    if arrow_type in _ARROW_TO_STARROCKS:
        return _ARROW_TO_STARROCKS[arrow_type]

    if pa.types.is_decimal(arrow_type):
        return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"

    # All timestamps → DATETIME (second precision, sub-second truncated)
    if pa.types.is_timestamp(arrow_type):
        return "DATETIME"

    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        inner = _arrow_type_to_starrocks(arrow_type.value_type)
        return f"ARRAY<{inner}>"

    if pa.types.is_map(arrow_type):
        key = _arrow_type_to_starrocks(arrow_type.key_type)
        val = _arrow_type_to_starrocks(arrow_type.item_type)
        return f"MAP<{key}, {val}>"

    if pa.types.is_struct(arrow_type):
        fields = ", ".join(
            f"{f.name} {_arrow_type_to_starrocks(f.type)}" for f in arrow_type
        )
        return f"STRUCT<{fields}>"

    raise TypeError(f"Unsupported PyArrow type for StarRocks: {arrow_type}")


def schema_to_starrocks_columns(
    arrow_schema: pa.Schema,
) -> list[tuple[str, str, bool]]:
    """Convert a PyArrow schema to ``(column_name, starrocks_type, nullable)`` triples.

    StarRocks expresses nullability via ``NULL``/``NOT NULL`` in DDL — there is
    no ``Nullable()`` wrapper like ClickHouse.
    """
    columns = []
    for field in arrow_schema:
        sr_type = _arrow_type_to_starrocks(field.type)
        columns.append((field.name, sr_type, field.nullable))
    return columns


# ── Type conversion: StarRocks → PyArrow ─────────────────────────────────────────────

_STARROCKS_SIMPLE_TYPES: dict[str, pa.DataType] = {
    "BOOLEAN": pa.bool_(),
    "TINYINT": pa.int8(),
    "SMALLINT": pa.int16(),
    "INT": pa.int32(),
    "BIGINT": pa.int64(),
    "LARGEINT": pa.int64(),  # best PyArrow fit for 128-bit signed
    "FLOAT": pa.float32(),
    "DOUBLE": pa.float64(),
    "STRING": pa.string(),
    "VARCHAR": pa.string(),
    "CHAR": pa.string(),
    "DATE": pa.date32(),
    "DATETIME": pa.timestamp("s"),
    "VARBINARY": pa.binary(),
}


def _starrocks_type_to_arrow(sr_type: str) -> pa.DataType:
    """Convert a StarRocks column type string to PyArrow.

    Handles simple types, parameterised types (``VARCHAR(n)``, ``CHAR(n)``,
    ``DECIMAL(p,s)``), and container types (``ARRAY<T>``, ``MAP<K,V>``,
    ``STRUCT<…>``).
    """
    sr_type = sr_type.strip()
    upper = sr_type.upper()

    # Simple types (exact match)
    if upper in _STARROCKS_SIMPLE_TYPES:
        return _STARROCKS_SIMPLE_TYPES[upper]

    # VARCHAR(n), CHAR(n) → string
    if upper.startswith("VARCHAR(") or upper.startswith("CHAR("):
        return pa.string()

    # DECIMAL(p, s)
    m = re.match(r"^DECIMAL\((\d+),\s*(\d+)\)$", upper)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))

    # ARRAY<T>
    if upper.startswith("ARRAY<") and upper.endswith(">"):
        inner = sr_type[6:-1]
        return pa.list_(_starrocks_type_to_arrow(inner))

    # MAP<K, V> — split at the top-level comma
    if upper.startswith("MAP<") and upper.endswith(">"):
        inner = sr_type[4:-1]
        depth = 0
        for i, c in enumerate(inner):
            if c in ("<", "("):
                depth += 1
            elif c in (">", ")"):
                depth -= 1
            elif c == "," and depth == 0:
                key_type = _starrocks_type_to_arrow(inner[:i].strip())
                val_type = _starrocks_type_to_arrow(inner[i + 1 :].strip())
                return pa.map_(key_type, val_type)
        raise TypeError(f"Could not parse MAP type: {sr_type!r}")

    # STRUCT<field1 TYPE1, field2 TYPE2, ...> — split at top-level commas
    if upper.startswith("STRUCT<") and upper.endswith(">"):
        inner = sr_type[7:-1]
        fields = _split_top_level(inner)
        pa_fields = []
        for field_str in fields:
            field_str = field_str.strip()
            # First token is the field name, rest is the type
            name, type_str = field_str.split(None, 1)
            pa_fields.append(pa.field(name, _starrocks_type_to_arrow(type_str)))
        return pa.struct(pa_fields)

    raise TypeError(f"Unsupported StarRocks type: {sr_type!r}")


def _split_top_level(s: str) -> list[str]:
    """Split *s* by commas that are not nested inside ``<>`` or ``()``."""
    parts = []
    depth = 0
    start = 0
    for i, c in enumerate(s):
        if c in ("<", "("):
            depth += 1
        elif c in (">", ")"):
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(s[start:i])
            start = i + 1
    parts.append(s[start:])
    return parts


# ── Normalise information_schema types to canonical DDL types ─────────────────────────


def _normalize_column_type(col_type: str) -> str:
    """Normalise a type from ``information_schema.COLUMNS`` to the canonical
    DDL type produced by :func:`_arrow_type_to_starrocks`.

    StarRocks stores types in MySQL format internally, e.g. ``int(11)`` instead
    of ``INT``, ``varchar(65533)`` instead of ``STRING``, ``tinyint(1)`` instead
    of ``BOOLEAN``.
    """
    col_type = col_type.strip()
    lower = col_type.lower()

    # BOOLEAN is reported as tinyint(1)
    if lower == "tinyint(1)":
        return "BOOLEAN"

    # Integer types with optional display width: int(11) → INT, etc.
    m = re.match(r"^(tinyint|smallint|int|bigint|largeint)(?:\(\d+\))?$", lower)
    if m:
        return m.group(1).upper()

    if lower in ("float", "double", "date", "datetime", "boolean", "varbinary"):
        return lower.upper()

    # varchar(65533) is StarRocks' internal representation of STRING
    m = re.match(r"^varchar\((\d+)\)$", lower)
    if m:
        return "STRING" if m.group(1) == "65533" else f"VARCHAR({m.group(1)})"

    m = re.match(r"^char\((\d+)\)$", lower)
    if m:
        return f"CHAR({m.group(1)})"

    m = re.match(r"^decimal\((\d+),\s*(\d+)\)$", lower)
    if m:
        return f"DECIMAL({m.group(1)}, {m.group(2)})"

    # Container types — recurse into children
    if lower.startswith("array<") and lower.endswith(">"):
        inner = col_type[6:-1]
        return f"ARRAY<{_normalize_column_type(inner)}>"

    if lower.startswith("map<") and lower.endswith(">"):
        inner = col_type[4:-1]
        depth = 0
        for i, c in enumerate(inner):
            if c in ("<", "("):
                depth += 1
            elif c in (">", ")"):
                depth -= 1
            elif c == "," and depth == 0:
                key_type = _normalize_column_type(inner[:i].strip())
                val_type = _normalize_column_type(inner[i + 1 :].strip())
                return f"MAP<{key_type}, {val_type}>"
        raise TypeError(f"Could not parse MAP type: {col_type!r}")

    if lower.startswith("struct<") and lower.endswith(">"):
        inner = col_type[7:-1]
        fields = _split_top_level(inner)
        parts = []
        for field_str in fields:
            field_str = field_str.strip()
            name, type_str = field_str.split(None, 1)
            parts.append(f"{name} {_normalize_column_type(type_str)}")
        return f"STRUCT<{', '.join(parts)}>"

    return col_type.upper()


# ── StarRocks Dialect ────────────────────────────────────────────────────────────────


class StarRocksDialect(Dialect):
    """Dialect implementation for StarRocks.

    Uses ``pymysql`` (MySQL protocol) for DDL/DML and delegates bulk reads to
    StarRocks' ``FILES()`` table function via
    :class:`StarRocksCloudStorage.starrocks_files_function`.

    Schema evolution is **not** supported (both capability flags return
    ``False``). Tables must have a matching schema before bulk loading.

    StarRocks does not support multi-statement transactions with bulk loads,
    so each table is loaded independently. A failure after some tables have
    been loaded leaves those tables with new data; the recommended recovery
    path is to re-run in ``REPLACE`` mode.

    Args:
        config: StarRocks server connection parameters. The optional
            :class:`~tabsdata._credentials.UserPasswordCredentials` inside
            ``config.credentials`` supply the username and password; when
            ``None`` the StarRocks ``root`` user with no password is used.
        cloud_storage: Cloud storage backend that both stages the Parquet
            files and knows how to generate the matching StarRocks
            ``FILES()`` expression for the staged file references.
    """

    ALTER_POLL_INTERVAL = 2.0  # seconds between ALTER TABLE status polls
    ALTER_POLL_TIMEOUT = 300.0  # max seconds to wait for ALTER TABLE completion

    def __init__(
        self,
        config: StarRocksConfig,
        cloud_storage: StarRocksCloudStorage,
    ) -> None:
        try:
            import pymysql  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "pymysql is required for the StarRocks dialect. "
                "Install it with: pip install pymysql"
            ) from e

        self._config = config
        self._cloud_storage = cloud_storage
        self._db = config.database
        self._conn = None

    # ── Connection management ────────────────────────────────────────────────────────

    def _get_connection(self):
        import pymysql

        if self._conn is None or not self._conn.open:
            username = (
                self._config.credentials.user.secret_value
                if self._config.credentials is not None
                else "root"
            )
            password = (
                self._config.credentials.password.secret_value
                if self._config.credentials is not None
                else ""
            )
            self._conn = pymysql.connect(
                host=self._config.host,
                port=self._config.port,
                database=self._config.database,
                user=username,
                password=password,
                autocommit=True,
            )
        return self._conn

    def _execute(self, sql: str, params=None):
        """Execute a query and return all result rows."""
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _command(self, sql: str, params=None):
        """Execute a statement that does not return rows."""
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)

    # ── Capability flags ──────────────────────────────────────────────────────────────

    def supports_tabsdata_schema_evolution(self) -> bool:
        return False

    def supports_native_schema_evolution(self) -> bool:
        return False

    def supports_transactions(self) -> bool:
        return False

    def supports_auto_cleanup(self) -> bool:
        return False

    def is_bulk_load_async(self) -> bool:
        return False

    # ── Schema operations ─────────────────────────────────────────────────────────────

    def table_exists(self, load_mode: LoadMode, table_name: str) -> bool:
        rows = self._execute(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (self._db, table_name),
        )
        return rows[0][0] > 0

    def get_table_schema(self, load_mode: LoadMode, table_name: str) -> schema:
        """Return the table schema as a PyArrow schema.

        Converts StarRocks column types from ``information_schema.COLUMNS``
        back to PyArrow types.
        """
        rows = self._execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (self._db, table_name),
        )
        fields = [
            pa.field(
                row[0],
                _starrocks_type_to_arrow(_normalize_column_type(row[1])),
                nullable=(row[2] == "YES"),
            )
            for row in rows
        ]
        return pa.schema(fields)

    def create_table(
        self, load_mode: LoadMode, table_name: str, arrow_schema: schema
    ) -> None:
        sr_columns = schema_to_starrocks_columns(arrow_schema)
        col_defs = ",\n    ".join(
            f"`{name}` {typ}{' NULL' if nullable else ' NOT NULL'}"
            for name, typ, nullable in sr_columns
        )
        first_col = sr_columns[0][0]
        ddl = (
            f"CREATE TABLE `{self._db}`.`{table_name}` (\n"
            f"    {col_defs}\n"
            f") DUPLICATE KEY(`{first_col}`)\n"
            f"DISTRIBUTED BY HASH(`{first_col}`)\n"
            f'PROPERTIES ("fast_schema_evolution" = "true")'
        )
        logger.info("Creating table '%s': %s", table_name, ddl)
        self._command(ddl)

    def verify_schema(
        self, load_mode: LoadMode, table_name: str, arrow_schema: schema
    ) -> bool:
        """Verify that the existing table schema matches the Arrow schema.

        Compares column count, names, and types (in position order) against the
        StarRocks-mapped representation of ``arrow_schema``. Any mismatch
        raises :class:`~tabsdata._io.cloud_staging.common.errors.SchemaError`.
        """
        sr_columns = schema_to_starrocks_columns(arrow_schema)
        expected = [(name, typ) for name, typ, _ in sr_columns]
        existing_full = self._get_raw_columns(table_name)
        existing = [(row[0], row[1]) for row in existing_full]
        if existing != expected:
            raise SchemaError(
                f"Schema mismatch for table '{self._db}.{table_name}'.\n"
                f"  Expected: {expected}\n"
                f"  Found:    {existing}"
            )
        return True

    # ── Schema evolution ──────────────────────────────────────────────────────────────

    def get_type_widening_map(self) -> dict[str, set[str]]:
        """Return the TED4 type-widening map for StarRocks integer and float types."""
        return {
            "SMALLINT": {"TINYINT"},
            "INT": {"TINYINT", "SMALLINT"},
            "BIGINT": {"TINYINT", "SMALLINT", "INT"},
            "LARGEINT": {"TINYINT", "SMALLINT", "INT", "BIGINT"},
            "DOUBLE": {"FLOAT"},
        }

    def _wait_for_alter(self, table_name: str) -> None:
        """Poll ``SHOW ALTER TABLE COLUMN`` until the latest operation completes.

        Used by :meth:`apply_schema_delta` (TED4/Iceberg-style evolution) if
        schema evolution is enabled in the future.
        """
        start = time.time()
        while time.time() - start < self.ALTER_POLL_TIMEOUT:
            rows = self._execute(
                f"SHOW ALTER TABLE COLUMN FROM `{self._db}` "
                f"WHERE TableName = '{table_name}' "
                f"ORDER BY CreateTime DESC LIMIT 1"
            )
            if not rows:
                return  # no pending alter
            # State column is at index 9 in SHOW ALTER TABLE COLUMN output
            state = rows[0][9] if len(rows[0]) > 9 else rows[0][-2]
            if state in ("FINISHED", "CANCELLED"):
                if state == "CANCELLED":
                    raise SchemaError(
                        f"ALTER TABLE on '{table_name}' was cancelled: {rows[0]}"
                    )
                return
            time.sleep(self.ALTER_POLL_INTERVAL)
        raise SchemaError(
            f"ALTER TABLE on '{table_name}' timed out after {self.ALTER_POLL_TIMEOUT}s"
        )

    # ── Data operations ───────────────────────────────────────────────────────────────

    def truncate_table(self, load_mode: LoadMode, table_name: str) -> None:
        self._command(f"TRUNCATE TABLE `{self._db}`.`{table_name}`")
        logger.debug("Truncated table: %s.%s", self._db, table_name)

    def bulk_load(
        self, load_mode: LoadMode, table_name: str, staged_file_refs: list[str]
    ) -> BulkLoadResult:
        """Load staged Parquet files into the target table.

        Calls :meth:`StarRocksCloudStorage.starrocks_files_function` to
        produce a single ``FILES(...)`` expression covering all staged files,
        then executes::

            INSERT INTO `<db>`.`<table>` SELECT * FROM FILES(...)
        """
        files_fn = self._cloud_storage.starrocks_files_function(staged_file_refs)
        sql = f"INSERT INTO `{self._db}`.`{table_name}` SELECT * FROM {files_fn}"
        logger.info(
            "Executing bulk load for %s.%s (%d files)",
            self._db,
            table_name,
            len(staged_file_refs),
        )
        self._command(sql)
        logger.info("Bulk load complete for %s.%s", self._db, table_name)
        return BulkLoadResult(status=BulkLoadStatus.COMPLETED)

    # ── Internal helpers ──────────────────────────────────────────────────────────────

    def _get_raw_columns(self, table_name: str) -> list[tuple[str, str, bool]]:
        """Return ``[(name, type_string, nullable), …]`` from ``information_schema``."""
        rows = self._execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (self._db, table_name),
        )
        return [
            (row[0], _normalize_column_type(row[1]), row[2] == "YES") for row in rows
        ]
