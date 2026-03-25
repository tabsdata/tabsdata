#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time

import pyarrow as pa
from pyarrow import schema

from tabsdata._io.cloud_staging.common.dialect import Dialect
from tabsdata._io.cloud_staging.common.modes import LoadMode, WriteMode
from tabsdata._io.cloud_staging.common.results import BulkLoadResult
from tabsdata._io.cloud_staging.common.statuses import BulkLoadStatus
from tabsdata._io.cloud_staging.druid.cloud_storage import DruidCloudStorage
from tabsdata._io.cloud_staging.druid.config import DruidConfig, _sanitize_url

logger = logging.getLogger(__name__)

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF = 2.0

#: HTTP status codes that indicate a transient server issue and should be
#: retried with exponential backoff (429 Too Many Requests, 502 Bad Gateway,
#: 503 Service Unavailable, 504 Gateway Timeout).
_RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})

#: Synthetic column name used when no ``time_column`` is configured.
DUMMY_TIME_COLUMN = "__td_dummy_time"


# ── Type conversion: PyArrow → Druid ─────────────────────────────────────────────


def _pyarrow_type_to_druid(arrow_type: pa.DataType) -> str:
    """Convert a PyArrow type to a Druid native type string.

    Druid has a simplified type system: ``LONG``, ``FLOAT``, ``DOUBLE``,
    ``STRING``, ``COMPLEX<json>``, ``ARRAY<STRING>``, ``ARRAY<LONG>``,
    ``ARRAY<DOUBLE>``.

    .. warning::
        ``uint64`` values exceeding 2^63-1 will silently overflow when
        stored as Druid ``LONG``.  ``decimal128`` values lose precision
        when stored as ``DOUBLE`` (~15 significant digits).
    """
    # Boolean — stored as 0/1
    if pa.types.is_boolean(arrow_type):
        return "LONG"

    # Integer types — all become LONG (64-bit signed)
    if pa.types.is_integer(arrow_type):
        if arrow_type == pa.uint64():
            logger.warning(
                "Mapping uint64 to Druid LONG: values exceeding 2^63-1 "
                "will silently overflow."
            )
        return "LONG"

    # Float types
    if arrow_type == pa.float16() or arrow_type == pa.float32():
        return "FLOAT"
    if arrow_type == pa.float64():
        return "DOUBLE"

    # Decimal — no native type, use DOUBLE
    if pa.types.is_decimal(arrow_type):
        logger.warning(
            "Mapping %s to Druid DOUBLE: precision may be lost "
            "(~15 significant digits).",
            arrow_type,
        )
        return "DOUBLE"

    # String types
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "STRING"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "STRING"

    # Temporal types — stored as LONG (millis since epoch)
    if pa.types.is_timestamp(arrow_type):
        if arrow_type.tz is not None:
            logger.warning(
                "Mapping %s to Druid LONG: timezone info (%s) will be "
                "discarded.  Druid stores timestamps as UTC millis.",
                arrow_type,
                arrow_type.tz,
            )
        return "LONG"
    if pa.types.is_date(arrow_type):
        return "LONG"

    # Array types
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        inner = arrow_type.value_type
        if pa.types.is_string(inner) or pa.types.is_large_string(inner):
            return "ARRAY<STRING>"
        if pa.types.is_integer(inner) or pa.types.is_boolean(inner):
            return "ARRAY<LONG>"
        if pa.types.is_floating(inner) or pa.types.is_decimal(inner):
            return "ARRAY<DOUBLE>"
        return "COMPLEX<json>"

    # Map and Struct — serialized as nested JSON
    if pa.types.is_map(arrow_type) or pa.types.is_struct(arrow_type):
        return "COMPLEX<json>"

    raise ValueError(f"Unsupported PyArrow type for Druid: {arrow_type}")


# ── Druid Dialect ────────────────────────────────────────────────────────────────


class DruidDialect(Dialect):
    """TED5 Dialect implementation for Apache Druid.

    Uses the Overlord REST API to submit native batch ingestion tasks
    (``index_parallel``) and poll for completion.  Unlike synchronous
    dialects (ClickHouse, StarRocks), :meth:`bulk_load` returns
    ``PENDING`` and requires polling via :meth:`poll_bulk_load`.

    Args:
        config: Druid Overlord connection and tuning parameters.
        cloud_storage: Cloud storage backend that both stages the Parquet
            files and knows how to generate the matching Druid
            ``inputSource`` dict for each set of staged file references.
    """

    def __init__(
        self,
        config: DruidConfig,
        cloud_storage: DruidCloudStorage,
    ) -> None:
        try:
            import requests
        except ImportError as e:
            raise ImportError(
                "requests is required for the Druid dialect. "
                "Install it with: pip install requests"
            ) from e

        self._config = config
        self._cloud_storage = cloud_storage
        self._requests = requests

        # Validate auth completeness before allocating the session to avoid
        # leaking a requests.Session (and its urllib3 PoolManager) on error.
        if bool(config.auth_user) != bool(config.auth_password):
            raise ValueError(
                "Druid auth is partially configured (auth_user=%s, "
                "auth_password=%s). Both must be set for HTTP Basic auth."
                % (
                    "set" if config.auth_user else "missing",
                    "set" if config.auth_password else "missing",
                )
            )

        self._session = requests.Session()
        if config.auth_user and config.auth_password:
            self._session.auth = (config.auth_user, config.auth_password)
        self._session.headers["Content-Type"] = "application/json"

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> DruidDialect:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _overlord_url(self, path: str) -> str:
        return f"{self._config.overlord_url.rstrip('/')}{path}"

    def _safe_overlord_url(self, path: str) -> str:
        """Like :meth:`_overlord_url` but with credentials scrubbed for logs."""
        return f"{_sanitize_url(self._config.overlord_url.rstrip('/'))}{path}"

    _ALLOWED_METHODS = frozenset({"get", "post"})

    def _request_with_retry(self, method: str, url: str, **kwargs):
        """Execute an HTTP request with retry on transient errors.

        Retries on ``ConnectionError``, ``Timeout``, and transient HTTP
        status codes (429, 502, 503, 504).  Non-retryable HTTP errors
        (e.g. 400, 401, 404) are raised immediately.
        """
        if method not in self._ALLOWED_METHODS:
            raise ValueError(
                f"Unsupported HTTP method {method!r}; "
                f"expected one of {sorted(self._ALLOWED_METHODS)}"
            )
        kwargs.setdefault("timeout", self._config.http_timeout)
        last_attempt = DEFAULT_RETRY_ATTEMPTS - 1
        for attempt in range(DEFAULT_RETRY_ATTEMPTS):
            try:
                resp = getattr(self._session, method)(url, **kwargs)
                resp.raise_for_status()
                return resp
            except (
                self._requests.ConnectionError,
                self._requests.Timeout,
            ) as e:
                if attempt == last_attempt:
                    raise
                last_exc = e
            except self._requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status not in _RETRYABLE_STATUS_CODES or attempt == last_attempt:
                    raise
                last_exc = e

            wait = DEFAULT_RETRY_BACKOFF * (2**attempt)
            logger.warning(
                "Druid request failed (attempt %d/%d), retrying in %.1fs: %s",
                attempt + 1,
                DEFAULT_RETRY_ATTEMPTS,
                wait,
                last_exc,
            )
            time.sleep(wait)

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
        return True

    # ── Schema operations ─────────────────────────────────────────────────────────────

    def table_exists(self, load_mode: LoadMode, table_name: str) -> bool:
        """Always returns ``False`` — Druid datasources are created implicitly
        by the first ingestion task."""
        return False

    def get_table_schema(self, load_mode: LoadMode, table_name: str) -> schema:
        """Return an empty schema — Druid uses schema-on-read."""
        return pa.schema([])

    def create_table(
        self,
        load_mode: LoadMode,
        table_name: str,
        arrow_schema: schema,
        column_nulls: dict[str, bool | None] | None = None,
    ) -> None:
        """No-op — Druid datasources are created implicitly by the first
        ingestion task with ``useSchemaDiscovery: true``."""
        logger.info(
            "Druid datasource '%s' will be created by ingestion task.", table_name
        )

    def verify_schema(
        self, load_mode: LoadMode, table_name: str, arrow_schema: schema
    ) -> bool:
        """Always returns ``True`` — Druid uses schema-on-read and reconciles
        different segment schemas at query time."""
        return True

    # ── Data operations ───────────────────────────────────────────────────────────────

    def truncate_table(self, load_mode: LoadMode, table_name: str) -> None:
        """No-op — replace mode is handled via ``dropExisting`` in the
        ingestion spec."""
        logger.info(
            "Druid truncate is a no-op — replace handled via dropExisting in "
            "ingestion spec for datasource: %s",
            table_name,
        )

    def bulk_load(
        self, load_mode: LoadMode, table_name: str, staged_file_refs: list[str]
    ) -> BulkLoadResult:
        """Submit an ``index_parallel`` ingestion task to the Druid Overlord.

        Returns ``PENDING`` with the task ID as ``job_handle``.
        """
        is_replace = load_mode.write_mode == WriteMode.REPLACE
        ingestion_spec = self._build_ingestion_spec(
            table_name, staged_file_refs, is_replace
        )
        logger.debug("Ingestion spec for '%s': %s", table_name, ingestion_spec)

        logger.info(
            "Submitting Druid ingestion task for datasource '%s' (%d files, mode=%s)",
            table_name,
            len(staged_file_refs),
            load_mode.write_mode.value,
        )

        try:
            resp = self._request_with_retry(
                "post",
                self._overlord_url("/druid/indexer/v1/task"),
                json=ingestion_spec,
            )
            try:
                payload = resp.json()
            except ValueError as e:
                raise RuntimeError(
                    "Druid Overlord returned non-JSON response. "
                    f"URL: {self._safe_overlord_url('/druid/indexer/v1/task')}, "
                    f"Status: {resp.status_code}, "
                    f"Response body: {resp.text[:500]}"
                ) from e
            task_id = payload["task"]
        except KeyError as e:
            raise RuntimeError(
                "Druid Overlord response missing 'task' field. "
                f"URL: {self._safe_overlord_url('/druid/indexer/v1/task')}, "
                f"Response body: {resp.text}"
            ) from e
        except self._requests.HTTPError as e:
            response = getattr(e, "response", None)
            body = response.text if response is not None else "<no body>"
            raise RuntimeError(
                f"Failed to submit Druid ingestion task for datasource "
                f"'{table_name}' at "
                f"{self._safe_overlord_url('/druid/indexer/v1/task')}: {e}\n"
                f"Response body: {body}"
            ) from e
        except self._requests.RequestException as e:
            raise RuntimeError(
                f"Failed to submit Druid ingestion task for datasource "
                f"'{table_name}' at "
                f"{self._safe_overlord_url('/druid/indexer/v1/task')}: {e}"
            ) from e

        logger.info("Druid ingestion task submitted: %s", task_id)
        return BulkLoadResult(status=BulkLoadStatus.PENDING, job_handle=task_id)

    def poll_bulk_load(self, load_mode: LoadMode, job_handle: str) -> BulkLoadResult:
        """Poll the Druid Overlord for task status."""
        status_path = f"/druid/indexer/v1/task/{job_handle}/status"
        try:
            resp = self._request_with_retry(
                "get",
                self._overlord_url(status_path),
            )
            try:
                payload = resp.json()
            except ValueError as e:
                raise RuntimeError(
                    f"Druid Overlord returned non-JSON status response for "
                    f"task '{job_handle}'. "
                    f"URL: {self._safe_overlord_url(status_path)}, "
                    f"Status: {resp.status_code}, "
                    f"Response body: {resp.text[:500]}"
                ) from e
            status_code = payload["status"]["statusCode"]
        except KeyError as e:
            raise RuntimeError(
                "Druid Overlord status response missing 'status.statusCode'. "
                f"URL: {self._safe_overlord_url(status_path)}, "
                f"Response body: {resp.text}"
            ) from e
        except self._requests.RequestException as e:
            raise RuntimeError(
                f"Failed to poll Druid task status for '{job_handle}' "
                f"at {self._safe_overlord_url(status_path)}: {e}"
            ) from e

        if status_code in ("RUNNING", "WAITING"):
            return BulkLoadResult(status=BulkLoadStatus.PENDING, job_handle=job_handle)
        elif status_code == "SUCCESS":
            logger.info("Druid ingestion task completed: %s", job_handle)
            return BulkLoadResult(status=BulkLoadStatus.COMPLETED)
        else:
            logger.warning(
                "Druid task %s has terminal status: %s", job_handle, status_code
            )
            error_msg = self._get_task_error(job_handle)
            logger.error("Druid ingestion task failed: %s — %s", job_handle, error_msg)
            return BulkLoadResult(
                status=BulkLoadStatus.FAILED,
                error=f"Druid task {job_handle} failed "
                f"(status={status_code}): {error_msg}",
            )

    # ── Internal helpers ──────────────────────────────────────────────────────────────

    def _get_task_error(self, task_id: str) -> str:
        """Fetch task report to extract error details.

        Uses ``_request_with_retry`` so transient failures during error
        fetching don't lose the root cause information.  Extracts the
        primary ``errorMsg`` and, when available, the per-partition
        ``errorMsgs`` list for richer diagnostics.
        """
        try:
            resp = self._request_with_retry(
                "get",
                self._overlord_url(f"/druid/indexer/v1/task/{task_id}/reports"),
            )
            reports = resp.json()
            stats = reports.get("ingestionStatsAndErrors", {}).get("payload", {})

            # Primary error message
            error_msg = stats.get("errorMsg", "Unknown error")

            # Collect supplementary detail lines when available.
            details: list[str] = []
            error_msgs = stats.get("errorMsgs")
            if error_msgs and isinstance(error_msgs, list):
                for entry in error_msgs[:5]:  # cap to avoid huge messages
                    if isinstance(entry, str) and entry:
                        details.append(entry)
                    elif isinstance(entry, dict):
                        details.append(str(entry))

            rows_processed = (
                stats.get("rowStats", {}).get("totals", {}).get("processed")
            )
            if rows_processed is not None:
                details.append(f"rows processed before failure: {rows_processed}")

            if details:
                return f"{error_msg} | details: {'; '.join(details)}"
            return error_msg
        except Exception as e:
            logger.exception("Unable to fetch error details for task %s", task_id)
            return f"Unable to fetch error details: {e}"

    def _build_ingestion_spec(
        self, datasource: str, staged_file_refs: list[str], is_replace: bool
    ) -> dict:
        """Build a native batch ``index_parallel`` ingestion spec.

        Key design decisions documented inline:

        * **useSchemaDiscovery** — lets Druid auto-detect columns from the
          Parquet file metadata rather than requiring an explicit dimensions
          list.  This is the recommended approach for Parquet-based ingestion
          and avoids maintaining a duplicate schema.
        * **dimensions: []** — left empty because ``useSchemaDiscovery``
          populates dimensions from the input data.  An empty list is
          required by the spec when discovery is enabled.
        * **queryGranularity: "none"** — preserves the original row-level
          timestamps without pre-aggregating at query time.  This is the
          safest default; rollup can be enabled by the user via a future
          configuration parameter if needed.
        * **intervals (replace mode)** — ``1970-01-01/2100-01-01`` is an
          all-encompassing interval that tells Druid to drop *all* existing
          segments when ``dropExisting: true``.  Without bounded intervals
          Druid cannot determine which segments to replace.
        """
        input_source = self._cloud_storage.druid_input_source(staged_file_refs)

        if self._config.time_column:
            timestamp_spec = {
                "column": self._config.time_column,
                "format": "auto",
            }
        else:
            # When no time column is provided, use a synthetic column with a
            # constant value so that all rows land in a single time chunk.
            # The column name is a tabsdata convention and must not collide
            # with user-defined column names — validated in DruidDest.stream().
            timestamp_spec = {
                "column": DUMMY_TIME_COLUMN,
                "format": "auto",
                "missingValue": "2000-01-01T00:00:00.000Z",
            }

        granularity_spec: dict = {
            "segmentGranularity": self._config.segment_granularity,
            # "none" preserves row-level timestamps without pre-aggregation.
            "queryGranularity": "none",
            # Rollup disabled — we ingest raw rows, not pre-aggregated data.
            "rollup": False,
        }

        # Replace mode (dropExisting: true) requires bounded intervals so
        # Druid knows which segments to drop.  Use an all-encompassing
        # interval that covers any possible timestamp value.
        if is_replace:
            granularity_spec["intervals"] = ["1970-01-01/2100-01-01"]

        return {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": datasource,
                    "timestampSpec": timestamp_spec,
                    "dimensionsSpec": {
                        # Empty because useSchemaDiscovery will populate
                        # dimensions from Parquet column metadata.
                        "dimensions": [],
                        "useSchemaDiscovery": True,
                    },
                    "granularitySpec": granularity_spec,
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": input_source,
                    "inputFormat": {"type": "parquet"},
                    "appendToExisting": not is_replace,
                    "dropExisting": is_replace,
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxNumConcurrentSubTasks": self._config.max_concurrent_subtasks,
                    "maxRowsInMemory": self._config.max_rows_in_memory,
                    "partitionsSpec": {
                        "type": "dynamic",
                        "maxRowsPerSegment": self._config.max_rows_per_segment,
                    },
                },
            },
        }
