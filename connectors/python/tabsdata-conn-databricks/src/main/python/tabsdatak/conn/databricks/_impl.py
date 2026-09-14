#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks adapter: every driver-touching piece used by
``_plugin.py``.

The legacy :class:`tabsdata.connector.databricks._connector.DatabricksSource`
and :class:`DatabricksDestination` are ported here, adapted to the
new-model ``_AnyDatabricksConn`` / ``DatabricksSrc`` / ``DatabricksDest``
classes. The library choice matches the legacy: ``databricks.sdk``
(WorkspaceClient + statement_execution API + Files API) plus
``databricks.sql`` (Connection for ``COPY INTO`` / DDL). No SQLAlchemy.

All driver imports happen inside method bodies so importing
``tabsdatak.conn.databricks`` stays cheap when nothing actually runs
the plugin (registry scans pay only for the SrcDef / DestDef
metadata).
"""

from __future__ import annotations

import io
import logging
import threading
import time
import uuid
from collections.abc import Collection
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

from tabsdatak.conn.common._validation import complete_levels, fqn, unfqn

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import merge_batch

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._watermark import watermarked_batch
from tabsdatak.conn.common.sql.cdc.constants import (
    OP_DELETE,
    OP_INSERT,
    OP_UPDATE,
)
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.databricks import DatabricksDestConn, DatabricksSrcConn, MergeCfg
from tabsdatak.conn.databricks._merge_dialect import DatabricksMergeDialect
from tabsdatak.conn.databricks._validation import _DATABRICKS_FQN_DEF
from tabsdatak.spi import DestContext

# Conn helpers operate on both Src and Dest variants.
_AnyDatabricksConn = DatabricksSrcConn | DatabricksDestConn

if TYPE_CHECKING:
    import pyarrow as pa

    import databricks.sdk as dbsdk
    import databricks.sql as dbsql

logger = logging.getLogger(__name__)


# --- Constants (matched to legacy) ----------------------------------

_ASYNC_EXECUTION_WAIT_TIMEOUT = "0s"
_DEFAULT_POLL_INTERVAL_SECONDS = 2
_DEFAULT_DOWNLOAD_THREADS = 4
_DOWNLOAD_CONNECT_TIMEOUT_SECONDS = 10
_DOWNLOAD_READ_TIMEOUT_SECONDS = 300
_DOWNLOAD_RETRY_ATTEMPTS = 12
_DOWNLOAD_RETRY_WAIT_SECONDS = 5
_PARQUET_COMPRESSION = "snappy"
_COPY_INTO_RETRY_ATTEMPTS = 6
_COPY_INTO_RETRY_WAIT_SECONDS = 5

# The two aliases the generated MERGE binds its sides to. The source is a
# staging table whose name the connector picks, so nothing outside the
# statement can name either side.
MERGE_TARGET_ALIAS = "target"
MERGE_SOURCE_ALIAS = "source"

# The markers ``MergeCfg.op_column`` carries, and what each one makes the merge
# do with the row -- ``CdcOperation``'s, so a CDC source in this family feeds a
# merge without a translation step. Lower case, and the column is lower-cased
# before it is compared, so a feed emitting either case works.
#
# ``.value``, not the member: these are interpolated into the marker predicates,
# and formatting a str-mixin enum yields ``"CdcOperation.INSERT"`` -- which would
# be valid SQL that no row can ever match.
_OP_INSERT = OP_INSERT.value
_OP_UPDATE = OP_UPDATE.value
_OP_DELETE = OP_DELETE.value


# --- Read cfg / write cfg dataclasses -------------------------------


@dataclass(kw_only=True)
class DatabricksReadCfg:
    conn: _AnyDatabricksConn
    queries: list[str]
    initial_values: dict | None = None
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS
    download_threads: int = _DEFAULT_DOWNLOAD_THREADS


@dataclass(kw_only=True)
class DatabricksWriteCfg:
    conn: _AnyDatabricksConn
    target_tables: list[str]  # raw user input -- not yet fqn'd
    if_table_exists: str  # "append" | "replace" | "merge"
    schema_evolution: str  # "update" | "strict"
    ctx: DestContext  # the trx the write belongs to; a write stamps its id
    # The target column each row is stamped with the trx id in, already in the
    # form the target names it by; ``None`` when the write names no watermark.
    # A merge reads its own off ``merge_cfgs``.
    watermark_column: str | None = None
    merge_cfgs: list[MergeCfg] | None = None
    # Support knobs (legacy parity). Forwarded directly to
    # ``cursor.execute(**kwargs)``; only set when debugging.
    support_append_create_table: dict = field(default_factory=dict)
    support_append_copy_options: str = ""
    support_append_copy_into: dict = field(default_factory=dict)
    support_replace_create_table: dict = field(default_factory=dict)
    support_replace_copy_options: str = ""
    support_replace_copy_into: dict = field(default_factory=dict)


# --- Conn helpers ---------------------------------------------------


def _host_value(conn: _AnyDatabricksConn) -> str:
    """Resolve ``conn.host_url`` to a plain string."""
    return _convert(conn.host_url, str)


def _token_value(conn: _AnyDatabricksConn) -> str:
    return _convert(conn.token, str)


def _build_workspace_client(conn: _AnyDatabricksConn):
    """Return a :class:`databricks.sdk.WorkspaceClient` bound to the
    connection's PAT."""
    try:
        import databricks.sdk as dbsdk
    except ImportError as e:
        raise ImportError(
            "databricks-sdk is required for the Databricks connector."
            " Install it with: pip install databricks-sdk"
        ) from e
    return dbsdk.WorkspaceClient(
        host=_host_value(conn),
        token=_token_value(conn),
    )


def _resolve_warehouse_id(
    ws_client: dbsdk.WorkspaceClient, conn: _AnyDatabricksConn
) -> str:
    """Look up the warehouse id from ``conn``.

    ``conn.warehouse_id`` wins if set; otherwise ``conn.warehouse``
    (name) is resolved via the workspace's warehouse list. The
    cross-field check in ``_AnyDatabricksConn.validate`` ensures at most
    one is set.
    """
    if conn.warehouse_id is not None:
        return conn._warehouse_id()
    if conn.warehouse is None:
        raise ValueError(
            "_AnyDatabricksConn requires either ``warehouse`` (name) or"
            " ``warehouse_id`` for runtime operations"
        )
    target_name = conn._warehouse()
    for w in ws_client.warehouses.list():
        if w.name == target_name:
            return w.id
    raise ValueError(f"Databricks warehouse {target_name!r} not found in workspace")


def _build_sql_connection(
    ws_client: dbsdk.WorkspaceClient, conn: _AnyDatabricksConn
) -> dbsql.Connection:
    """Return a :class:`databricks.sql.Connection` bound to the
    workspace + warehouse identified by ``conn``."""
    try:
        import databricks.sql as dbsql
        from databricks.sdk.core import Config, pat_auth
    except ImportError as e:
        raise ImportError(
            "databricks-sql-connector is required for the Databricks"
            " connector. Install it with:"
            " pip install databricks-sql-connector"
        ) from e

    warehouse_id = _resolve_warehouse_id(ws_client, conn)
    host = _host_value(conn)
    token = _token_value(conn)

    def credentials_provider():
        return pat_auth(Config(host=host, token=token))

    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=credentials_provider,
    )


# --- Source: query execution + chunked download ---------------------


def _param_sql_type(value: Any) -> str | None:
    """Map a Python bind value to the Databricks SQL type name for a
    ``StatementParameterListItem``. Returns ``None`` for a ``None``
    value (typed NULL is left to Databricks). ``bool`` is checked
    before ``int`` because ``bool`` is a subclass of ``int``.

    Getting this right matters: an unset type defaults to STRING, which
    breaks numeric comparisons like ``unix_timestamp(data_ts) > :wm``
    (bigint vs string)."""
    from datetime import date, datetime
    from decimal import Decimal

    if value is None:
        return None
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, Decimal):
        return "DECIMAL"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, date):
        return "DATE"
    return "STRING"


def _param_value_str(value: Any) -> str | None:
    """Render a Python bind value as the string form Databricks expects
    for a ``StatementParameterListItem.value`` (``None`` -> typed NULL)."""
    from datetime import date, datetime

    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _statement_parameters(initial_values: dict[str, Any] | None):
    """Build the native ``parameters`` list for
    ``statement_execution.execute_statement`` from the resolved bind
    dict, so ``:name`` markers in the SQL are bound as real, typed
    parameters (no text interpolation). Returns ``None`` when there are
    no binds."""
    if not initial_values:
        return None
    from databricks.sdk.service.sql import StatementParameterListItem

    return [
        StatementParameterListItem(
            name=name,
            value=_param_value_str(value),
            type=_param_sql_type(value),
        )
        for name, value in initial_values.items()
    ]


def _chunk_links_iterator(ws_client, response):
    """Walk the external-link payload chunk-by-chunk, fetching the
    next page from ``statement_execution.get_statement_result_chunk_n``
    on demand.

    Returns a callable that yields ``(chunk_index, external_link)`` on
    each call (thread-safe). When the result is exhausted it returns
    ``None``.
    """

    @dataclass
    class _State:
        statement_id: str
        payload: Any | None
        links: list = field(default_factory=list)
        position: int = 0
        chunk: int = 0
        exhausted: bool = False

    lock = threading.Lock()
    state = _State(
        statement_id=response.statement_id,
        payload=response.result,
    )

    def next_link():
        with lock:
            while True:
                if state.position < len(state.links):
                    link = state.links[state.position]
                    chunk = state.chunk
                    state.position += 1
                    state.chunk += 1
                    return chunk, link
                if state.exhausted:
                    return None
                payload = state.payload
                if payload is not None and payload.external_links:
                    state.links = list(payload.external_links)
                    state.position = 0
                    last_link = state.links[-1]
                    next_chunk_index = last_link.next_chunk_index
                    if next_chunk_index is not None:
                        state.payload = (
                            ws_client.statement_execution.get_statement_result_chunk_n(
                                state.statement_id,
                                next_chunk_index,
                            )
                        )
                    else:
                        state.payload = None
                else:
                    state.exhausted = True
                    return None

    return next_link


def _download_chunk(link):
    """Fetch a single external-link payload and parse it as an
    Arrow IPC stream. Retried with fixed-wait backoff (matches the
    legacy ``tenacity`` configuration)."""
    import pyarrow.ipc as ipc
    import requests

    last_exc: Exception | None = None
    for attempt in range(1, _DOWNLOAD_RETRY_ATTEMPTS + 1):
        try:
            with requests.get(
                link.external_link,
                stream=True,
                timeout=(
                    _DOWNLOAD_CONNECT_TIMEOUT_SECONDS,
                    _DOWNLOAD_READ_TIMEOUT_SECONDS,
                ),
            ) as response:
                response.raise_for_status()
                return ipc.open_stream(io.BytesIO(response.content)).read_all()
        except Exception as e:  # noqa: BLE001 -- retry loop
            last_exc = e
            if attempt < _DOWNLOAD_RETRY_ATTEMPTS:
                logger.warning(
                    "databricks: chunk download attempt %d/%d failed (%s);"
                    " retrying in %ds",
                    attempt,
                    _DOWNLOAD_RETRY_ATTEMPTS,
                    e,
                    _DOWNLOAD_RETRY_WAIT_SECONDS,
                )
                time.sleep(_DOWNLOAD_RETRY_WAIT_SECONDS)
    assert last_exc is not None
    raise last_exc


def _databricks_type_to_arrow(col) -> "pa.DataType":
    """Map a single Databricks result-manifest ``ColumnInfo`` to a
    pyarrow type. Used only to type an **empty** (0-row) result -- for
    non-empty results the Arrow IPC stream carries its own schema.

    Covers the scalar / temporal / decimal types; anything exotic
    (``ARRAY`` / ``MAP`` / ``STRUCT`` / ``INTERVAL`` / ...) can't be
    reconstructed from the flat manifest, so it falls back to
    ``string`` with a warning rather than failing the whole read."""
    import pyarrow as pa

    name = (col.type_name.value if col.type_name is not None else "STRING").upper()
    scalar = {
        "BOOLEAN": pa.bool_(),
        "BYTE": pa.int8(),
        "SHORT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "LONG": pa.int64(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "STRING": pa.string(),
        "CHAR": pa.string(),
        "VARCHAR": pa.string(),
        "BINARY": pa.binary(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "TIMESTAMP_NTZ": pa.timestamp("us"),
    }
    if name in scalar:
        return scalar[name]
    if name == "DECIMAL":
        precision = getattr(col, "type_precision", None)
        scale = getattr(col, "type_scale", None)
        return pa.decimal128(
            precision if precision is not None else 38,
            scale if scale is not None else 0,
        )
    logger.warning(
        "databricks: unmapped column type %r for empty-result schema;"
        " defaulting to string",
        name,
    )
    return pa.string()


def _arrow_schema_from_manifest(response) -> "pa.Schema | None":
    """Build a pyarrow schema from a statement response's result
    manifest (``response.manifest.schema.columns``), which is present
    even for a 0-row result. Returns ``None`` when the manifest carries
    no column schema (the caller decides whether that's fatal)."""
    import pyarrow as pa

    manifest = getattr(response, "manifest", None)
    schema = getattr(manifest, "schema", None) if manifest is not None else None
    columns = getattr(schema, "columns", None) if schema is not None else None
    if not columns:
        return None
    fields = [
        pa.field(col.name or f"col_{i}", _databricks_type_to_arrow(col))
        for i, col in enumerate(columns)
    ]
    return pa.schema(fields)


class _ChunkWriter:
    """Drain pa.Tables to a single parquet file, preserving order
    across worker threads.

    ``empty_schema`` is the result-set schema derived from the query
    manifest; when no chunks arrive (a 0-row result), :meth:`close`
    uses it to still emit a valid empty (0-row) parquet so downstream
    steps that read the file's schema don't hit a missing path."""

    def __init__(self, path: Path, empty_schema: "pa.Schema | None" = None) -> None:
        import pyarrow.parquet as pq

        self._path = path
        self._pq = pq
        self._empty_schema = empty_schema
        self._lock = threading.Lock()
        self._pending: dict[int, "pa.Table"] = {}
        self._next = 0
        self._writer: pq.ParquetWriter | None = None
        self.written = 0

    def submit(self, chunk: int, table: "pa.Table") -> None:
        with self._lock:
            self._pending[chunk] = table
            self._drain()

    def _drain(self) -> None:
        while self._next in self._pending:
            table = self._pending.pop(self._next)
            if self._writer is None:
                self._writer = self._pq.ParquetWriter(
                    str(self._path),
                    table.schema,
                    compression=_PARQUET_COMPRESSION,
                )
            self._writer.write_table(table)
            self._next += 1
            self.written += 1

    def close(self) -> None:
        with self._lock:
            self._drain()
            if self._writer is not None:
                self._writer.close()
                self._writer = None
            elif self._empty_schema is not None:
                # Zero chunks arrived: materialize a valid empty parquet with
                # the real column schema so the slot's file always exists.
                self._pq.ParquetWriter(
                    str(self._path),
                    self._empty_schema,
                    compression=_PARQUET_COMPRESSION,
                ).close()


def _process_chunks(next_link, chunk_writer: _ChunkWriter) -> None:
    while True:
        item = next_link()
        if item is None:
            return
        chunk, link = item
        chunk_writer.submit(chunk, _download_chunk(link))


def execute_query(
    ws_client,
    cfg: DatabricksReadCfg,
    statement: str,
    work_dir: Path,
    index: int,
) -> Path:
    """Execute one query via ``statement_execution.execute_statement``
    (``Disposition.EXTERNAL_LINKS`` + ``Format.ARROW_STREAM``), poll
    until completion, then download every result chunk and write them
    in order to a single parquet file under ``work_dir``."""
    from databricks.sdk.service.sql import (
        Disposition,
        Format,
        StatementState,
    )

    # ``:name`` markers stay in the SQL; the resolved values bind natively as
    # typed parameters (no text interpolation).
    parameters = _statement_parameters(cfg.initial_values)

    uuid_string = uuid.uuid4().hex[:16]
    out_path = work_dir / f"intermediate_{index}_{uuid_string}.parquet"

    warehouse_id = _resolve_warehouse_id(ws_client, cfg.conn)
    # Run the query in the connection's catalog/schema so unqualified table
    # names (e.g. ``SELECT * FROM wld_country``) resolve against the right
    # schema instead of the warehouse default.
    conn = cfg.conn
    catalog = conn._catalog() if conn.catalog is not None else None
    schema = conn._schema() if conn.schema is not None else None
    response = ws_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        catalog=catalog,
        schema=schema,
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
        wait_timeout=_ASYNC_EXECUTION_WAIT_TIMEOUT,
        parameters=parameters,
    )

    while response.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        time.sleep(cfg.poll_interval_seconds)
        response = ws_client.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"Databricks query failed with state"
            f" {response.status.state!r}: {response.status.error}"
        )

    # The result manifest carries the column schema even for a 0-row result;
    # capture it so an empty result still materializes a valid typed parquet.
    empty_schema = _arrow_schema_from_manifest(response)

    next_link = _chunk_links_iterator(ws_client, response)
    chunk_writer = _ChunkWriter(out_path, empty_schema)
    try:
        if cfg.download_threads <= 1:
            _process_chunks(next_link, chunk_writer)
        else:
            with ThreadPoolExecutor(max_workers=cfg.download_threads) as executor:
                futures = [
                    executor.submit(_process_chunks, next_link, chunk_writer)
                    for _ in range(cfg.download_threads)
                ]
                for future in futures:
                    future.result()
    finally:
        chunk_writer.close()

    if chunk_writer.written == 0 and empty_schema is None:
        from tabsdatak.conn.databricks.error import DatabricksErrorCode

        raise DatabricksErrorCode.DATABRICKS_11.exception(query_index=index)

    return out_path


# --- Destination: table FQN + volume upload + COPY INTO -------------


def _fq_for_sql(fq_table: str) -> str:
    """Return the SQL-safe (per-part backtick-wrapped) form of a
    ``catalog.schema.table`` name.

    Accepts a bare dotted name (output of :func:`_fully_qualify_table`)
    OR a name whose parts are already backtick-quoted -- a single
    ``unfqn`` -> ``fqn`` round-trip strips any pre-existing quotes so
    the result never double-quotes.
    """
    parts = unfqn(fq_table, _DATABRICKS_FQN_DEF)
    return fqn(parts, _DATABRICKS_FQN_DEF)


def _fully_qualify_table(conn: _AnyDatabricksConn, table: str) -> str:
    """Resolve ``table`` to a 3-part bare ``catalog.schema.table`` name,
    drawing from ``conn.catalog`` / ``conn.schema`` to fill in the
    missing leading parts. Accepts parts that are bare or backtick-
    quoted (a doubled backtick escapes an embedded backtick).

    The result is BARE -- callers re-apply quoting via
    :func:`_fq_for_sql` when feeding the value back into SQL, and use
    the bare parts directly for ``/Volumes/...`` paths.

    Raises a :class:`DatabricksValidateException` (DATABRICKS_10) if
    the result is not 3-part.
    """
    catalog = conn._catalog() if conn.catalog is not None else None
    schema = conn._schema() if conn.schema is not None else None
    parts = unfqn(table, _DATABRICKS_FQN_DEF, total_parts=3)
    parts = complete_levels(parts, [catalog or None, schema or None, None])
    if any(p is None for p in parts):
        from tabsdatak.conn.databricks.error import DatabricksErrorCode

        raise DatabricksErrorCode.DATABRICKS_10.exception(value=table)
    return ".".join(parts)


def _volume_path(conn: _AnyDatabricksConn, fq_table: str) -> str:
    """Compute the ``/Volumes/<catalog>/<schema>/<volume>`` prefix
    that ``COPY INTO`` will read from. ``conn.volume`` may itself be
    a 1-, 2-, or 3-part name; the rules match the legacy:

    - 3-part volume (``c.s.v``): used verbatim.
    - 2-part volume (``s.v``): catalog comes from the table's first
      segment (which is always present at this point because
      ``_fully_qualify_table`` already ran).
    - 1-part volume (``v``): catalog and schema both come from the
      table.
    """
    parts = fq_table.split(".", maxsplit=2)
    t_catalog, t_schema, _t_name = parts
    volume_raw = conn._volume()
    vol_parts = volume_raw.split(".")
    if len(vol_parts) == 3:
        v_catalog, v_schema, v_name = vol_parts
    elif len(vol_parts) == 2:
        v_catalog, v_schema, v_name = t_catalog, vol_parts[0], vol_parts[1]
    else:
        v_catalog, v_schema, v_name = t_catalog, t_schema, vol_parts[0]
    return f"/Volumes/{v_catalog}/{v_schema}/{v_name}"


def upload_file_to_volume(
    ws_client,
    conn: _AnyDatabricksConn,
    fq_table: str,
    local_path: Path,
) -> str:
    """Upload ``local_path`` to the conn's volume and return the
    in-volume absolute path (suitable for ``read_files`` /
    ``COPY INTO``)."""
    staged_name = f"{uuid.uuid4().hex}_{local_path.name}"
    target = f"{_volume_path(conn, fq_table)}/{staged_name}"
    logger.debug("Uploading %s to volume %s", local_path, target)
    with open(local_path, "rb") as f:
        ws_client.files.upload(target, f)
    return target


def _is_volume_path_not_found(exc: BaseException) -> bool:
    from databricks.sql.exc import ServerOperationError

    return isinstance(exc, ServerOperationError) and "PATH_NOT_FOUND" in str(exc)


@retry(
    retry=retry_if_exception(_is_volume_path_not_found),
    stop=stop_after_attempt(_COPY_INTO_RETRY_ATTEMPTS),
    wait=wait_fixed(_COPY_INTO_RETRY_WAIT_SECONDS),
    reraise=True,
)
def _execute_volume_statement(cursor, statement: str, parameters: dict) -> None:
    logger.debug("Executing: %s", statement)
    cursor.execute(statement, **parameters)


def _temp_table_name(fq_table: str) -> str:
    """A name for a temp table beside ``fq_table``, unique per call.

    Keeps the target's catalog / schema parts so the table is created where the
    write is happening, and suffixes only the last part. The suffix is what makes
    it safe: it cannot collide with the target it is about to be merged into, nor
    with a concurrent write to the same target, nor with a copy an earlier run
    failed to drop.

    Returned per-part backtick-quoted and ready to embed -- *not* bare like
    :func:`_fully_qualify_table`. Quoting here, off the split parts, rather than
    re-deriving it from a bare string is what keeps a target part holding a dot
    or a backtick from re-splitting into a different name.
    """
    parts = unfqn(fq_table, _DATABRICKS_FQN_DEF)
    parts[-1] = f"{parts[-1]}_{uuid.uuid4().hex[:8]}"
    return fqn(parts, _DATABRICKS_FQN_DEF)


def _create_temp_table_from_volume_schema(
    cursor,
    file_in_volume: str,
    fq_table: str,
) -> str:
    """``CREATE TABLE`` beside ``fq_table`` carrying the volume file's own schema
    and no rows, and return its quoted ``catalog.schema.table`` name.

    ``read_files`` reads the parquet footer, so the temp table holds every column
    the file has -- including any the target does not, which a merge predicate
    may still need to read. ``LIMIT 0`` takes the schema without the rows; the
    caller loads them with :func:`copy_into_table`.

    A real catalog table, not a session relation: it outlives the connection, so
    **the caller must pass it to :func:`drop_temp_table` when the merge is
    done** -- including when the merge fails, or the copy is left behind.

    Goes through :func:`_execute_volume_statement` because it reads the volume:
    an upload is not always visible to the warehouse immediately, and the retry
    there is what absorbs the ``PATH_NOT_FOUND`` window.
    """
    temp_table = _temp_table_name(fq_table)
    logger.debug("Creating temp table %s for %s", temp_table, fq_table)
    _execute_volume_statement(
        cursor,
        f"CREATE TABLE {temp_table} USING DELTA AS SELECT * FROM "
        f"read_files('{file_in_volume}', format => 'parquet') LIMIT 0",
        {},
    )
    return temp_table


def drop_temp_table(cursor, temp_table: str) -> None:
    """Drop a table :func:`_create_temp_table_from_volume_schema` made.

    ``IF EXISTS`` so this is safe in the ``finally`` it belongs in: it has to run
    whether the merge succeeded, failed, or never got as far as loading the copy.

    Not routed through :func:`_execute_volume_statement` -- the statement touches
    the catalog, not the volume, so the ``PATH_NOT_FOUND`` retry has nothing to
    absorb here.
    """
    logger.debug("Dropping temp table %s", temp_table)
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")


def _volume_schema_projection(exclude: Collection[str] = ()) -> str:
    """The select list that copies a volume file's schema, less ``exclude``.

    ``* EXCEPT`` drops by name against the file's own columns, and Databricks
    matches identifiers case-insensitively, so a marker the publisher wrote as
    ``OP`` is dropped by ``op`` without the caller having to know which case the
    file used.

    An excluded name the file does not have makes the statement fail rather than
    being ignored, which is the wanted end of that trade: a merge whose marker
    column is missing cannot read any row's fate, so it must not quietly build a
    target as though it could.
    """
    if not exclude:
        return "*"
    return f"* EXCEPT ({_quote_columns(exclude)})"


def _quote_column(name: str) -> str:
    """A column reference, backtick-quoted.

    Safe unconditionally: quoting is the only way to spell a name that is not a
    legal bare identifier, and Databricks resolves a quoted name the same way it
    resolves a bare one -- the backticks do not make it case-sensitive.
    """
    return fqn([name], _DATABRICKS_FQN_DEF)


def _quote_columns(names: Collection[str]) -> str:
    return ", ".join(_quote_column(name) for name in names)


# --- Watermark ------------------------------------------------------
#
# Shared by the two paths that stamp a target with the id of the trx that wrote
# it: the merge, through
# :class:`~tabsdatak.conn.databricks._merge_dialect.DatabricksMergeDialect`,
# and the bulk load below. They live here so the two cannot drift on the
# column's type, on how a target is probed for it, or on whether the rows
# already in a target get backfilled.

# A trx id is an opaque string, and STRING is what DESCRIBE TABLE reports back
# for the column, so a target that already has it does not read as a retype.
WATERMARK_TYPE = "STRING"


def string_literal(value: str) -> str:
    """``value`` as a Databricks SQL string literal.

    The trx id is a system-generated token, but it is the one value in these
    statements that does not come from the connector itself, and a statement
    built by hand has nowhere to bind it.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def has_watermark_column(cursor, fq_table: str, column: str) -> bool:
    """Whether ``fq_table`` carries ``column``.

    Read off ``information_schema`` rather than ``DESCRIBE TABLE`` because the
    target may not be there at all: every watermark pass can run before anything
    has been written to it, and a missing table is not an error here -- it simply
    has no such column. Unity Catalog stores an identifier lower-cased, which is
    also how both paths fold one, so both sides of each comparison are lowered
    and the two agree.
    """
    catalog, schema, table = unfqn(fq_table, _DATABRICKS_FQN_DEF, total_parts=3)
    cursor.execute(
        f"SELECT COUNT(*) FROM {fqn([catalog], _DATABRICKS_FQN_DEF)}"
        f".information_schema.columns"
        f" WHERE lower(table_schema) = {string_literal(schema.lower())}"
        f" AND lower(table_name) = {string_literal(table.lower())}"
        f" AND lower(column_name) = {string_literal(column.lower())}"
    )
    row = cursor.fetchone()
    return bool(row and row[0])


def check_watermark_column(
    cursor, fq_table: str, sql_fq: str, column: str, transaction_id: str
) -> bool:
    """Whether ``fq_table`` already holds a row stamped with ``transaction_id``.

    Asked of ``information_schema.columns`` first, and only then of the table: a
    target that does not exist yet, or one that predates the watermark column,
    has no such row and must not be probed for one -- selecting a column the
    table has not got is an error rather than an answer.
    """
    if not has_watermark_column(cursor, fq_table, column):
        return False
    cursor.execute(
        f"SELECT COUNT(*) > 0 FROM {sql_fq}"
        f" WHERE {_quote_column(column)} = {string_literal(transaction_id)}"
    )
    row = cursor.fetchone()
    return bool(row and row[0])


def ensure_watermark_column(cursor, fq_table: str, sql_fq: str, column: str) -> bool:
    """``ALTER`` the watermark column onto ``fq_table`` unless it is there
    already, reporting whether anything was added.

    ``ADD COLUMNS`` carries no ``IF NOT EXISTS``, so the probe is what makes
    this idempotent.

    Added bare, with no value: the rows already in the target were written by
    earlier transactions and keep the NULL a new column starts with. Stamping
    them with this trx's id would claim them for it.
    """
    if has_watermark_column(cursor, fq_table, column):
        return False
    logger.debug("Adding watermark column %s to %s", column, sql_fq)
    cursor.execute(
        f"ALTER TABLE {sql_fq} ADD COLUMNS ({_quote_column(column)} {WATERMARK_TYPE})"
    )
    return True


def copy_into_table(
    sql_conn,
    cfg: DatabricksWriteCfg,
    file_in_volume: str,
    fq_table: str,
) -> None:
    """Execute the CREATE-OR-REPLACE / CREATE-IF-NOT-EXISTS + COPY INTO
    sequence against ``fq_table`` from ``file_in_volume``.

    Append and replace only -- a merge goes through the shared driver and
    :class:`~tabsdatak.conn.databricks._merge_dialect.DatabricksMergeDialect`.
    """
    cursor = sql_conn.cursor()
    sql_fq = _fq_for_sql(fq_table)
    if cfg.if_table_exists == "replace":
        create_stmt = (
            f"CREATE OR REPLACE TABLE {sql_fq} "
            f"USING DELTA AS SELECT * FROM read_files("
            f"'{file_in_volume}', format => 'parquet') LIMIT 0"
        )
        copy_stmt = (
            f"COPY INTO {sql_fq} FROM '{file_in_volume}' "
            f"FILEFORMAT = PARQUET"
            f"{cfg.support_replace_copy_options}"
        )
        _execute_volume_statement(cursor, create_stmt, cfg.support_replace_create_table)
        _execute_volume_statement(cursor, copy_stmt, cfg.support_replace_copy_into)
        return
    if cfg.if_table_exists == "append":
        merge = (
            "'mergeSchema' = 'false'"
            if cfg.schema_evolution == "strict"
            else "'mergeSchema' = 'true'"
        )
        create_stmt = (
            f"CREATE TABLE IF NOT EXISTS {sql_fq} "
            f"USING DELTA AS SELECT * FROM read_files("
            f"'{file_in_volume}', format => 'parquet') LIMIT 0"
        )
        copy_stmt = (
            f"COPY INTO {sql_fq} FROM '{file_in_volume}' "
            f"FILEFORMAT = PARQUET "
            f"COPY_OPTIONS ({merge}{cfg.support_append_copy_options})"
        )
        _execute_volume_statement(cursor, create_stmt, cfg.support_append_create_table)
        if cfg.watermark_column is not None:
            # After the CREATE, so a target this write is creating already has
            # the column from the staged file and the probe finds nothing to do.
            # Before the COPY, because under strict ``mergeSchema`` is off and a
            # batch carrying a column the target lacks fails the load outright
            # -- and because a column added afterwards would be NULL in the rows
            # just written, leaving the guard to read the next rerun as the
            # first.
            ensure_watermark_column(cursor, fq_table, sql_fq, cfg.watermark_column)
        _execute_volume_statement(cursor, copy_stmt, cfg.support_append_copy_into)
        return
    raise ValueError(
        f"Invalid if_table_exists {cfg.if_table_exists!r}: "
        f"expected 'append' or 'replace'"
    )


def delete_from_volume(ws_client, file_in_volume: str) -> None:
    """Delete a previously-uploaded volume file. Failures are logged
    and swallowed -- COPY INTO has already landed and an orphan
    volume file is not worth failing the whole write_out for."""
    try:
        ws_client.files.delete(file_in_volume)
    except Exception:  # noqa: BLE001 -- best-effort cleanup
        logger.warning(
            "Failed to delete file %r from volume (continuing)",
            file_in_volume,
        )


# --- Top-level read / write orchestration ---------------------------


def read_all_queries(cfg: DatabricksReadCfg, work_dir: Path) -> list[Path]:
    """Execute every statement in ``cfg.queries`` against the
    workspace and return the list of resulting parquet paths (one per
    query, in order). The plugin wraps these into
    ``list[list[TableFileInput]]`` for the SPI."""
    logger.debug("databricks: establishing workspace client")
    ws_client = _build_workspace_client(cfg.conn)
    out: list[Path] = []
    total = len(cfg.queries)
    for index, statement in enumerate(cfg.queries):
        logger.info("databricks: reading query %d/%d", index + 1, total)
        out.append(execute_query(ws_client, cfg, statement, work_dir, index))
        logger.debug("databricks: finished query %d/%d", index + 1, total)
    return out


def reject_unsupported_columns(
    target_tables: list[str],
    parquet_files: list[Path],
) -> None:
    """Fail before uploading anything if a column has a type Databricks cannot
    store.

    COPY INTO rejects the parquet TIME type outright
    ("[PARQUET_TYPE_ILLEGAL] Illegal Parquet type: INT64 (TIME(...))"), and it
    does so per file after the upload and the CREATE have already run. Reading
    the parquet footer up front costs nothing and turns that into one error that
    names the table and the offending columns.
    """
    import pyarrow.parquet as pq

    from tabsdatak.conn.databricks.error import DatabricksErrorCode

    for table, path in zip(target_tables, parquet_files):
        schema = pq.read_schema(str(path))
        offenders = [f.name for f in schema if pa_is_time(f.type)]
        if offenders:
            raise DatabricksErrorCode.DATABRICKS_13.exception(
                table=table,
                columns=", ".join(repr(name) for name in offenders),
            )


def pa_is_time(arrow_type: Any) -> bool:
    import pyarrow as pa

    return pa.types.is_time(arrow_type)


def write_all_tables(
    cfg: DatabricksWriteCfg,
    parquet_files: list[Path],
) -> None:
    """Upload each parquet file to the volume and run the matching
    CREATE / COPY INTO sequence. Failures are surfaced to the caller
    which wraps them into the appropriate DATABRICKS_* code."""
    if len(cfg.target_tables) != len(parquet_files):
        # Should already have been caught by the plugin; defensive.
        from tabsdatak.conn.databricks.error import DatabricksErrorCode

        raise DatabricksErrorCode.DATABRICKS_7.exception(
            tables=len(cfg.target_tables),
            slots=len(parquet_files),
        )

    reject_unsupported_columns(list(cfg.target_tables), parquet_files)

    logger.debug("databricks: establishing workspace client")
    ws_client = _build_workspace_client(cfg.conn)
    logger.debug("databricks: establishing SQL connection")
    sql_conn = _build_sql_connection(ws_client, cfg.conn)
    total = len(cfg.target_tables)
    # ``strict`` also holds the merge configs to one per table: the plugin
    # normalizes them to that shape, and a mismatch here would silently merge one
    # table by another's keys.
    merge_cfgs = cfg.merge_cfgs or [None] * len(cfg.target_tables)
    try:
        for i, (raw_table, parquet_path, merge_cfg) in enumerate(
            zip(
                cfg.target_tables,
                parquet_files,
                merge_cfgs,
                strict=True,
            ),
            1,
        ):
            fq_table = _fully_qualify_table(cfg.conn, raw_table)
            logger.info("databricks: writing table %d/%d: %s", i, total, fq_table)
            if cfg.if_table_exists == "merge":
                _merge_one(
                    sql_conn, ws_client, cfg, Path(parquet_path), fq_table, merge_cfg
                )
                logger.debug("databricks: finished table %s", fq_table)
                continue
            # Append is the only disposition that stamps: a merge has
            # continued above, and a replace is refused the column by
            # DatabricksDest.validate. Named again here rather than inferred
            # from those two, so widening the API later cannot quietly start
            # stamping a disposition that has no business being stamped.
            watermark = (
                cfg.watermark_column if cfg.if_table_exists == "append" else None
            )
            if watermark is not None and check_watermark_column(
                sql_conn.cursor(),
                fq_table,
                _fq_for_sql(fq_table),
                watermark,
                cfg.ctx.trx_id,
            ):
                # Asked before the batch is rewritten and uploaded: a rerun that
                # has nothing to do should not pay to build and ship a file it
                # will not load.
                logger.info(
                    "databricks: skipping append onto %s: watermark column %s"
                    " already has value %s",
                    fq_table,
                    watermark,
                    cfg.ctx.trx_id,
                )
                continue
            logger.info("databricks: staging parquet for %s", fq_table)
            with watermarked_batch(
                Path(parquet_path), watermark, cfg.ctx.trx_id
            ) as batch:
                file_in_volume = upload_file_to_volume(
                    ws_client, cfg.conn, fq_table, batch
                )
            try:
                copy_into_table(sql_conn, cfg, file_in_volume, fq_table)
            finally:
                logger.debug("databricks: deleting staged file %s", file_in_volume)
                delete_from_volume(ws_client, file_in_volume)
            logger.debug("databricks: finished table %s", fq_table)
    finally:
        logger.debug("databricks: closing SQL connection")
        sql_conn.close()


def _merge_one(
    sql_conn,
    ws_client,
    cfg: DatabricksWriteCfg,
    parquet_path: Path,
    fq_table: str,
    merge_cfg: MergeCfg,
) -> None:
    """Drive one merge through the shared driver.

    Everything the merge does -- coalescing the batch, staging it, creating the
    target, evolving it, running the statement -- belongs to the driver and to
    :class:`~tabsdatak.conn.databricks._merge_dialect.DatabricksMergeDialect`.
    The volume upload and the staged file's deletion move in with the staging,
    which is why nothing is left here but the cursor.
    """
    cursor = sql_conn.cursor()
    try:
        merge_batch(
            DatabricksMergeDialect(
                cursor,
                ws_client,
                cfg.conn,
                fq_table,
                schema_evolution=cfg.schema_evolution,
                copy_options=cfg.support_replace_copy_options,
                copy_kwargs=cfg.support_replace_copy_into,
            ),
            target=_fq_for_sql(fq_table),
            batch=parquet_path,
            cfg=merge_cfg._sql_merge_cfg(),
            schema_evolution=cfg.schema_evolution,
            ctx=cfg.ctx,
            watermark_column=cfg.watermark_column,
        )
    finally:
        cursor.close()
