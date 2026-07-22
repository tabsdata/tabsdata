#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import re

import pyarrow as pa
from pyarrow import schema

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._clickhouse_config import ClickHouseConfig

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._cloud_storage import (
    ClickHouseCloudStorage,
)

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._dialect_base import Dialect

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._errors import SchemaError

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._modes import LoadMode

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._results import BulkLoadResult

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._statuses import BulkLoadStatus
from tabsdatak.conn.clickhouse._validation import CLICKHOUSE_FQN_DEF

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import complete_levels, fqn, unfqn

logger = logging.getLogger(__name__)


# --- Type conversion: PyArrow -> ClickHouse ------------------------

_ARROW_TO_CLICKHOUSE: dict = {
    pa.bool_(): "Bool",
    pa.int8(): "Int8",
    pa.int16(): "Int16",
    pa.int32(): "Int32",
    pa.int64(): "Int64",
    pa.uint8(): "UInt8",
    pa.uint16(): "UInt16",
    pa.uint32(): "UInt32",
    pa.uint64(): "UInt64",
    pa.float16(): "Float32",
    pa.float32(): "Float32",
    pa.float64(): "Float64",
    pa.string(): "String",
    pa.utf8(): "String",
    pa.large_string(): "String",
    pa.large_utf8(): "String",
    pa.binary(): "String",
    pa.date32(): "Date32",
}

_TIMESTAMP_PRECISION: dict[str, int] = {"s": 0, "ms": 3, "us": 6, "ns": 9}

# Container types are not wrapped in Nullable() in ClickHouse.
_CONTAINER_TYPE_CHECKS = (
    pa.types.is_list,
    pa.types.is_large_list,
    pa.types.is_map,
    pa.types.is_struct,
)


def _arrow_type_to_clickhouse(arrow_type: pa.DataType) -> str:
    """Convert a PyArrow ``DataType`` to a raw ClickHouse type string.

    Does not add a ``Nullable()`` wrapper -- that is applied by
    :func:`schema_to_clickhouse_columns` based on each field's
    nullability flag.
    """
    if arrow_type in _ARROW_TO_CLICKHOUSE:
        return _ARROW_TO_CLICKHOUSE[arrow_type]

    if pa.types.is_decimal(arrow_type):
        return f"Decimal({arrow_type.precision}, {arrow_type.scale})"

    if pa.types.is_timestamp(arrow_type):
        precision = _TIMESTAMP_PRECISION.get(arrow_type.unit, 6)
        if arrow_type.tz:
            return f"DateTime64({precision}, '{arrow_type.tz}')"
        return f"DateTime64({precision})"

    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        inner = _arrow_type_to_clickhouse(arrow_type.value_type)
        return f"Array({inner})"

    if pa.types.is_map(arrow_type):
        key = _arrow_type_to_clickhouse(arrow_type.key_type)
        val = _arrow_type_to_clickhouse(arrow_type.item_type)
        return f"Map({key}, {val})"

    if pa.types.is_struct(arrow_type):
        fields = ", ".join(
            f"{f.name} {_arrow_type_to_clickhouse(f.type)}" for f in arrow_type
        )
        return f"Tuple({fields})"

    raise TypeError(f"Unsupported PyArrow type for ClickHouse: {arrow_type}")


def schema_to_clickhouse_columns(
    arrow_schema: pa.Schema,
    column_nulls: dict[str, bool | None] | None = None,
) -> list[tuple[str, str]]:
    """Convert a PyArrow schema to ``[(name, clickhouse_type), ...]``.

    Non-container nullable fields are wrapped with ``Nullable()``.
    Container types (``Array``/``Map``/``Tuple``) are never wrapped
    because ClickHouse does not support top-level ``Nullable`` on those
    types.

    ``column_nulls`` (when provided) overrides Arrow's schema-level
    nullable flag: ``True`` or ``None`` (stats missing) -> ``Nullable``,
    ``False`` -> bare type.
    """
    columns = []
    for field in arrow_schema:
        ch_type = _arrow_type_to_clickhouse(field.type)
        is_container = any(check(field.type) for check in _CONTAINER_TYPE_CHECKS)
        if is_container:
            nullable = False
        elif column_nulls is not None and field.name in column_nulls:
            nullable = column_nulls[field.name] is not False
        else:
            nullable = field.nullable
        if nullable:
            ch_type = f"Nullable({ch_type})"
        columns.append((field.name, ch_type))
    return columns


# --- Type conversion: ClickHouse -> PyArrow ------------------------

_CLICKHOUSE_SIMPLE_TYPES: dict[str, pa.DataType] = {
    "Int8": pa.int8(),
    "Int16": pa.int16(),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "UInt8": pa.uint8(),
    "UInt16": pa.uint16(),
    "UInt32": pa.uint32(),
    "UInt64": pa.uint64(),
    "Float32": pa.float32(),
    "Float64": pa.float64(),
    "Bool": pa.bool_(),
    "String": pa.string(),
    "Date": pa.date32(),
    "Date32": pa.date32(),
    "DateTime": pa.timestamp("s"),
}

_PRECISION_TO_UNIT: dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}


def _clickhouse_type_to_arrow(ch_type: str) -> pa.DataType:
    """Convert a ClickHouse column type string (from ``system.columns``)
    to PyArrow. Transparently unwraps ``Nullable()`` and
    ``LowCardinality()`` wrappers.
    """
    ch_type = ch_type.strip()

    for wrapper in ("Nullable(", "LowCardinality("):
        if ch_type.startswith(wrapper) and ch_type.endswith(")"):
            return _clickhouse_type_to_arrow(ch_type[len(wrapper) : -1])

    if ch_type in _CLICKHOUSE_SIMPLE_TYPES:
        return _CLICKHOUSE_SIMPLE_TYPES[ch_type]

    if ch_type.startswith("FixedString("):
        return pa.string()

    # DateTime64(precision) or DateTime64(precision, 'tz')
    m = re.match(r"^DateTime64\((\d+)(?:,\s*'([^']*)')?\)$", ch_type)
    if m:
        precision = int(m.group(1))
        tz = m.group(2) or None
        unit = _PRECISION_TO_UNIT.get(precision, "us")
        return pa.timestamp(unit, tz=tz)

    # Decimal(P, S)
    m = re.match(r"^Decimal\((\d+),\s*(\d+)\)$", ch_type)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))

    # Decimal{32,64,128}(S) shorthand aliases
    for alias, precision in (
        ("Decimal32", 9),
        ("Decimal64", 18),
        ("Decimal128", 38),
    ):
        m = re.match(rf"^{alias}\((\d+)\)$", ch_type)
        if m:
            return pa.decimal128(precision, int(m.group(1)))

    # Array(T)
    if ch_type.startswith("Array(") and ch_type.endswith(")"):
        return pa.list_(_clickhouse_type_to_arrow(ch_type[6:-1]))

    # Map(K, V) -- split at the top-level comma.
    if ch_type.startswith("Map(") and ch_type.endswith(")"):
        inner = ch_type[4:-1]
        depth = 0
        for i, c in enumerate(inner):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "," and depth == 0:
                key_type = _clickhouse_type_to_arrow(inner[:i].strip())
                val_type = _clickhouse_type_to_arrow(inner[i + 1 :].strip())
                return pa.map_(key_type, val_type)
        raise TypeError(f"Could not parse Map type: {ch_type!r}")

    raise TypeError(f"Unsupported ClickHouse type: {ch_type!r}")


# --- FQN helpers (module-level so they're testable without a live
# ClickHouse client) ------------------------------------------------


def clickhouse_fqn_parts(table_name: str, default_db: str) -> tuple[str, str]:
    """Return ``(database, table)`` for a user-supplied table name.

    Input may be 1-part (``"users"``) or 2-part
    (``"analytics.users"``) and may use backtick-quoted parts with
    doubled-backtick escaping. A missing leading database part is
    filled in from ``default_db``. Returned values are raw identifier
    strings (no quoting / escaping), suitable for parameterized
    queries against ``system.columns`` / ``system.tables``.
    """
    parts = unfqn(table_name, CLICKHOUSE_FQN_DEF, total_parts=2)
    db, tbl = complete_levels(parts, [default_db, None])
    return db, tbl


def clickhouse_fqn(table_name: str, default_db: str) -> str:
    """Return the fully-qualified, backtick-escaped name for use in
    SQL: ``` `<db>`.`<table>` ```. Embedded backticks are doubled.
    """
    db, tbl = clickhouse_fqn_parts(table_name, default_db)
    return fqn([db, tbl], CLICKHOUSE_FQN_DEF)


# --- ClickHouse Dialect ------------------------------------------


class ClickHouseDialect(Dialect):
    """Dialect implementation for ClickHouse.

    Uses ClickHouse's native cloud storage table functions (``s3``,
    ``azureBlobStorage``) for ``INSERT INTO ... SELECT`` ingestion.

    ClickHouse does not support multi-statement transactions, so each
    table is loaded independently. A failure after some tables have
    been loaded leaves those tables with new data; recovery is to
    re-run in REPLACE mode.
    """

    def __init__(
        self,
        config: ClickHouseConfig,
        cloud_storage: ClickHouseCloudStorage,
    ) -> None:
        try:
            import clickhouse_connect
        except ImportError as e:
            raise ImportError(
                "clickhouse-connect is required for the ClickHouse"
                " dialect. Install it with:"
                " pip install clickhouse-connect"
            ) from e

        self._config = config
        self._cloud_storage = cloud_storage
        self._db = config.database
        self._client = clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            database=config.database,
            username=config.username,
            password=config.password,
            secure=config.secure,
        )

    # --- FQN helpers -------------------------------------------------

    def _fqn_parts(self, table_name: str) -> tuple[str, str]:
        return clickhouse_fqn_parts(table_name, self._db)

    def _fqn(self, table_name: str) -> str:
        return clickhouse_fqn(table_name, self._db)

    # --- Capability flags --------------------------------------------

    def supports_tabsdata_schema_evolution(self) -> bool:
        return True

    def supports_native_schema_evolution(self) -> bool:
        return False

    def supports_transactions(self) -> bool:
        return False

    def supports_auto_cleanup(self) -> bool:
        return False

    def is_bulk_load_async(self) -> bool:
        return False

    # --- Schema operations -------------------------------------------

    def table_exists(self, load_mode: LoadMode, table_name: str) -> bool:
        db, tbl = self._fqn_parts(table_name)
        result = self._client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": db, "tbl": tbl},
        )
        return result.result_rows[0][0] > 0

    def get_table_schema(self, load_mode: LoadMode, table_name: str) -> schema:
        """Return the table schema as a PyArrow schema."""
        db, tbl = self._fqn_parts(table_name)
        result = self._client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            parameters={"db": db, "tbl": tbl},
        )
        fields = [
            pa.field(row[0], _clickhouse_type_to_arrow(row[1]))
            for row in result.result_rows
        ]
        return pa.schema(fields)

    def create_table(
        self,
        load_mode: LoadMode,
        table_name: str,
        arrow_schema: schema,
        column_nulls: dict[str, bool | None] | None = None,
    ) -> None:
        col_defs = schema_to_clickhouse_columns(arrow_schema, column_nulls)
        columns = ",\n    ".join(f"`{name}` {typ}" for name, typ in col_defs)
        fq = self._fqn(table_name)
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {fq} (\n"
            f"    {columns}\n"
            f") ENGINE = MergeTree()\n"
            f"ORDER BY tuple()"
        )
        logger.info("Creating table '%s': %s", fq, ddl)
        self._client.command(ddl)

    def verify_schema(
        self,
        load_mode: LoadMode,
        table_name: str,
        arrow_schema: schema,
    ) -> bool:
        """Verify that the existing table schema exactly matches the
        Arrow schema."""
        expected = schema_to_clickhouse_columns(arrow_schema)
        existing = self._get_raw_columns(table_name)
        if existing != expected:
            raise SchemaError(
                f"Schema mismatch for table {self._fqn(table_name)}.\n"
                f"  Expected: {expected}\n"
                f"  Found:    {existing}"
            )
        return True

    # --- Schema evolution --------------------------------------------

    def get_type_widening_map(self) -> dict[str, set[str]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.clickhouse._iceberg import (
            CLICKHOUSE_TYPE_WIDENING_MAP,
        )

        return CLICKHOUSE_TYPE_WIDENING_MAP

    def get_evolution_table_schema(self, table_name: str):
        # noinspection PyProtectedMember
        from tabsdatak.conn.clickhouse._iceberg import (
            table_schema_from_system_columns,
        )

        db, tbl = self._fqn_parts(table_name)
        result = self._client.query(
            "SELECT name, type, default_kind, default_expression "
            "FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            parameters={"db": db, "tbl": tbl},
        )
        return table_schema_from_system_columns(
            self._fqn(table_name),
            result.result_rows,
        )

    def compute_evolution_delta(
        self,
        load_mode: LoadMode,
        table_name: str,
        new_arrow_schema,
        column_nulls: dict[str, bool | None] | None = None,
    ):
        """Compute a SchemaDelta between the current ClickHouse table
        and a new Arrow schema."""
        # noinspection PyProtectedMember
        from tabsdatak.conn.clickhouse._iceberg import (
            CLICKHOUSE_TYPE_WIDENING_MAP,
            ColumnDef,
            compute_delta,
            table_schema_from_arrow,
            validate_delta,
        )

        current = self.get_evolution_table_schema(table_name)
        desired = table_schema_from_arrow(
            current.table_name, new_arrow_schema, column_nulls
        )

        # Reconcile nullability between current table and incoming data.
        # The parquet stats tell us whether the *incoming batch* contains
        # nulls, but two adjustments are needed:
        #
        # 1. Existing columns that are already nullable must stay
        #    nullable even if this batch has no nulls -- we never tighten
        #    constraints (DROP_NULLABLE is unsafe in Iceberg semantics).
        #
        # 2. New columns (not in the current table) must be nullable
        #    regardless of the incoming data -- existing rows have no
        #    value for them and need NULL.
        current_cols = {c.name: c for c in current.columns}
        desired_columns = []
        for col in desired.columns:
            if col.name in current_cols:
                existing = current_cols[col.name]
                if existing.nullable and not col.nullable:
                    col = ColumnDef(
                        name=col.name,
                        type=col.type,
                        nullable=True,
                        default=col.default,
                    )
            else:
                if not col.nullable:
                    col = ColumnDef(
                        name=col.name,
                        type=col.type,
                        nullable=True,
                        default=col.default,
                    )
            desired_columns.append(col)
        desired = type(desired)(table_name=desired.table_name, columns=desired_columns)

        delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
        validate_delta(delta, strict=True)
        return delta

    def apply_schema_delta(self, load_mode: LoadMode, table_name: str, delta) -> None:
        """Apply a SchemaDelta by rendering and executing ClickHouse
        ALTER statements."""
        # noinspection PyProtectedMember
        from tabsdatak.conn.clickhouse._iceberg import (
            render_clickhouse_sql,
        )

        stmts = render_clickhouse_sql(delta)
        for stmt in stmts:
            logger.info("Schema evolution on '%s': %s", table_name, stmt)
            self._client.command(stmt)

    # --- Data operations ---------------------------------------------

    def drop_table(self, load_mode: LoadMode, table_name: str) -> None:
        fq = self._fqn(table_name)
        self._client.command(f"DROP TABLE IF EXISTS {fq}")
        logger.debug("Dropped table: %s", fq)

    def bulk_load(
        self,
        load_mode: LoadMode,
        table_name: str,
        staged_file_refs: list[str],
    ) -> BulkLoadResult:
        """Load staged Parquet files into the target table.

        For each staged file reference, asks the cloud storage object
        to produce the ClickHouse table function call, then executes::

            INSERT INTO `<db>`.`<table>` SELECT * FROM <table_fn>
        """
        fq = self._fqn(table_name)
        for ref in staged_file_refs:
            table_fn = self._cloud_storage.clickhouse_table_function(ref)
            sql = f"INSERT INTO {fq} SELECT * FROM {table_fn}"
            logger.debug("Executing INSERT INTO")
            self._client.command(sql)
            logger.debug("Inserted data into %s from %s", fq, ref)
        return BulkLoadResult(status=BulkLoadStatus.COMPLETED)

    # --- Internal helpers --------------------------------------------

    def _get_raw_columns(self, table_name: str) -> list[tuple[str, str]]:
        """Return ``[(name, type_string), ...]`` from ``system.columns``."""
        db, tbl = self._fqn_parts(table_name)
        result = self._client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            parameters={"db": db, "tbl": tbl},
        )
        return [(row[0], row[1]) for row in result.result_rows]
