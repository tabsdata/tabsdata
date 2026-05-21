"""Databricks adapter: every driver-touching piece used by
``_plugin.py``.

The legacy :class:`tabsdata.connector.databricks._connector.DatabricksSource`
and :class:`DatabricksDestination` are ported here, adapted to the
new-model ``DatabricksConn`` / ``DatabricksSrc`` / ``DatabricksDest``
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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tabsdatak.conn.common._validation import fqn, unfqn
from tabsdatak.conn.databricks import DatabricksConn
from tabsdatak.conn.databricks._validation import _DATABRICKS_FQN_DEF

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


# --- Read cfg / write cfg dataclasses -------------------------------


@dataclass(kw_only=True)
class DatabricksReadCfg:
    conn: DatabricksConn
    queries: list[str]
    initial_values: dict | None = None
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS
    download_threads: int = _DEFAULT_DOWNLOAD_THREADS


@dataclass(kw_only=True)
class DatabricksWriteCfg:
    conn: DatabricksConn
    target_tables: list[str]  # raw user input -- not yet fqn'd
    if_table_exists: str  # "append" | "replace"
    schema_evolution: str  # "update" | "strict"
    # Support knobs (legacy parity). Forwarded directly to
    # ``cursor.execute(**kwargs)``; only set when debugging.
    support_append_create_table: dict = field(default_factory=dict)
    support_append_copy_options: str = ""
    support_append_copy_into: dict = field(default_factory=dict)
    support_replace_create_table: dict = field(default_factory=dict)
    support_replace_copy_options: str = ""
    support_replace_copy_into: dict = field(default_factory=dict)


# --- Conn helpers ---------------------------------------------------


def _host_value(conn: DatabricksConn) -> str:
    """Resolve ``conn.host_url`` to a plain string."""
    return conn.host_url.value()


def _token_value(conn: DatabricksConn) -> str:
    return conn.token.value()


def _build_workspace_client(conn: DatabricksConn):
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
    ws_client: dbsdk.WorkspaceClient, conn: DatabricksConn
) -> str:
    """Look up the warehouse id from ``conn``.

    ``conn.warehouse_id`` wins if set; otherwise ``conn.warehouse``
    (name) is resolved via the workspace's warehouse list. The
    cross-field check in ``DatabricksConn.validate`` ensures at most
    one is set.
    """
    if conn.warehouse_id is not None:
        return conn.warehouse_id.value()
    if conn.warehouse is None:
        raise ValueError(
            "DatabricksConn requires either ``warehouse`` (name) or"
            " ``warehouse_id`` for runtime operations"
        )
    target_name = conn.warehouse.value()
    for w in ws_client.warehouses.list():
        if w.name == target_name:
            return w.id
    raise ValueError(f"Databricks warehouse {target_name!r} not found in workspace")


def _build_sql_connection(
    ws_client: dbsdk.WorkspaceClient, conn: DatabricksConn
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


def _replace_initial_values(statement: str, initial_values: dict[str, Any]) -> str:
    """Naive ``{key}`` -> repr-value substitution. Matches the legacy
    behaviour: dict keys are interpolated as ``{key}`` placeholders in
    the statement string. Strings are quoted; non-strings use
    ``repr()``."""
    out = statement
    for k, v in initial_values.items():
        token = "{" + k + "}"
        if isinstance(v, str):
            out = out.replace(token, repr(v))
        else:
            out = out.replace(token, repr(v))
    return out


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


class _ChunkWriter:
    """Drain pa.Tables to a single parquet file, preserving order
    across worker threads."""

    def __init__(self, path: Path) -> None:
        import pyarrow.parquet as pq

        self._path = path
        self._pq = pq
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

    if cfg.initial_values:
        statement = _replace_initial_values(statement, cfg.initial_values)

    uuid_string = uuid.uuid4().hex[:16]
    out_path = work_dir / f"intermediate_{index}_{uuid_string}.parquet"

    warehouse_id = _resolve_warehouse_id(ws_client, cfg.conn)
    response = ws_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
        wait_timeout=_ASYNC_EXECUTION_WAIT_TIMEOUT,
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

    next_link = _chunk_links_iterator(ws_client, response)
    chunk_writer = _ChunkWriter(out_path)
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


def _fully_qualify_table(conn: DatabricksConn, table: str) -> str:
    """Resolve ``table`` to a 3-part bare ``catalog.schema.table`` name,
    drawing from ``conn.catalog`` / ``conn.schema`` to fill in the
    missing leading parts. Accepts parts that are bare or backtick-
    quoted (with ```` `` ```` to escape an embedded backtick).

    The result is BARE -- callers re-apply quoting via
    :func:`_fq_for_sql` when feeding the value back into SQL, and use
    the bare parts directly for ``/Volumes/...`` paths.

    Raises a :class:`DatabricksValidateException` (DATABRICKS_10) if
    the result is not 3-part.
    """
    catalog = conn.catalog.value() if conn.catalog is not None else None
    schema = conn.schema.value() if conn.schema is not None else None
    parts = unfqn(table, _DATABRICKS_FQN_DEF, total_parts=3)
    if parts[0] is None and catalog:
        parts[0] = catalog
    if parts[1] is None and schema:
        parts[1] = schema
    if any(p is None for p in parts):
        from tabsdatak.conn.databricks.error import DatabricksErrorCode

        raise DatabricksErrorCode.DATABRICKS_10.exception(value=table)
    return ".".join(parts)


def _volume_path(conn: DatabricksConn, fq_table: str) -> str:
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
    volume_raw = conn.volume.value()
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
    conn: DatabricksConn,
    fq_table: str,
    local_path: Path,
) -> str:
    """Upload ``local_path`` to the conn's volume and return the
    in-volume absolute path (suitable for ``read_files`` /
    ``COPY INTO``)."""
    target = f"{_volume_path(conn, fq_table)}/{local_path.name}"
    logger.debug("Uploading %s to volume %s", local_path, target)
    with open(local_path, "rb") as f:
        ws_client.files.upload(target, f)
    return target


def copy_into_table(
    sql_conn,
    cfg: DatabricksWriteCfg,
    file_in_volume: str,
    fq_table: str,
) -> None:
    """Execute the legacy CREATE-OR-REPLACE / CREATE-IF-NOT-EXISTS +
    COPY INTO sequence against ``fq_table`` from ``file_in_volume``.
    The exact statements mirror the legacy
    ``_perform_upload_to_table`` codepath."""
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
        logger.debug("Executing: %s", create_stmt)
        cursor.execute(create_stmt, **cfg.support_replace_create_table)
        logger.debug("Executing: %s", copy_stmt)
        cursor.execute(copy_stmt, **cfg.support_replace_copy_into)
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
        logger.debug("Executing: %s", create_stmt)
        cursor.execute(create_stmt, **cfg.support_append_create_table)
        logger.debug("Executing: %s", copy_stmt)
        cursor.execute(copy_stmt, **cfg.support_append_copy_into)
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
    query, in order). The plugin maps these into
    ``list[list[TableFileSpec]]`` for the SPI."""
    logger.debug("databricks: establishing workspace client")
    ws_client = _build_workspace_client(cfg.conn)
    out: list[Path] = []
    total = len(cfg.queries)
    for index, statement in enumerate(cfg.queries):
        logger.info("databricks: reading query %d/%d", index + 1, total)
        out.append(execute_query(ws_client, cfg, statement, work_dir, index))
        logger.debug("databricks: finished query %d/%d", index + 1, total)
    return out


def write_all_tables(
    cfg: DatabricksWriteCfg,
    parquet_files: list[Path],
) -> None:
    """Upload each parquet file to the volume and run the matching
    CREATE / COPY INTO sequence. Failures are surfaced to the caller
    which wraps them into the appropriate CLICKHOUSE_* code."""
    if len(cfg.target_tables) != len(parquet_files):
        # Should already have been caught by the plugin; defensive.
        from tabsdatak.conn.databricks.error import DatabricksErrorCode

        raise DatabricksErrorCode.DATABRICKS_7.exception(
            tables=len(cfg.target_tables),
            slots=len(parquet_files),
        )

    logger.debug("databricks: establishing workspace client")
    ws_client = _build_workspace_client(cfg.conn)
    logger.debug("databricks: establishing SQL connection")
    sql_conn = _build_sql_connection(ws_client, cfg.conn)
    total = len(cfg.target_tables)
    try:
        for i, (raw_table, parquet_path) in enumerate(
            zip(
                cfg.target_tables,
                parquet_files,
            ),
            1,
        ):
            fq_table = _fully_qualify_table(cfg.conn, raw_table)
            logger.info("databricks: writing table %d/%d: %s", i, total, fq_table)
            logger.info("databricks: staging parquet for %s", fq_table)
            file_in_volume = upload_file_to_volume(
                ws_client, cfg.conn, fq_table, parquet_path
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
