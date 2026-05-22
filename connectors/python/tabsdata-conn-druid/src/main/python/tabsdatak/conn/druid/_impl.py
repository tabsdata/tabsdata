from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.common.file._output import copy_one as _transporter_copy_one

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.druid import (
    AzBlobLocation,
    CloudLocation,
    DruidConn,
    DruidDest,
    GCSLocation,
    S3Location,
)
from tabsdatak.conn.druid.error import DruidErrorCode

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP behavior
# ---------------------------------------------------------------------------

#: HTTP status codes that indicate a transient server issue and should be
#: retried with exponential backoff.
RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})

#: Default attempts / backoff seconds for the Overlord HTTP client.
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF = 2.0

#: How often the plugin polls Druid for ingestion-task completion (sec).
DEFAULT_POLL_INTERVAL_S = 2.0

#: Synthetic column name used when no ``time_column`` is configured on the
#: destination. Druid requires a primary timestamp column; this stub keeps
#: rows in a single time chunk so all data lands together.
DUMMY_TIME_COLUMN = "__td_dummy_time"


def _sanitize_url(url: str) -> str:
    """Strip embedded credentials from a URL for safe logging."""
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        host_part = parsed.hostname or ""
        if parsed.port:
            host_part = f"{host_part}:{parsed.port}"
        return parsed._replace(netloc=f"***:***@{host_part}").geturl()
    return url


# ---------------------------------------------------------------------------
# Staging path builders
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StagedFile:
    """A single parquet file staged at a cloud location, identified by
    the cloud-native URI Druid will use for ingestion."""

    uri: str


def _staging_subdir(ctx_trx_id: str) -> str:
    """Return the per-execution subdir under ``staging.base_path``.

    Uses ``trx_id`` so each transaction lands in its own directory and
    parallel runs never collide. A random suffix is appended so retries
    of the same trx don't clash with leftovers from a partial prior run.
    """
    return f"td-druid/{ctx_trx_id}/{uuid.uuid4().hex[:8]}"


def _join_path(base: str, *parts: str) -> str:
    """Join cloud-path segments with single ``/`` separators."""
    pieces = [base.strip("/")] if base else []
    for p in parts:
        pieces.append(p.strip("/"))
    return "/".join(s for s in pieces if s)


def staged_uri(location: CloudLocation, key: str) -> str:
    """Build the cloud-native URI for a key under ``location``."""
    if isinstance(location, S3Location):
        bucket = _convert(location.bucket, str)
        return f"s3://{bucket}/{key}"
    if isinstance(location, GCSLocation):
        bucket = _convert(location.bucket, str)
        return f"gs://{bucket}/{key}"
    if isinstance(location, AzBlobLocation):
        container = _convert(location.container, str)
        account = _convert(location.credentials.account_name, str)
        return f"azureStorage://{account}/{container}/{key}"
    # noinspection PyProtectedMember
    raise DruidErrorCode.DRUID_11.exception(kind=type(location).__name__)


# ---------------------------------------------------------------------------
# Transporter (LocalFile -> CloudLocation) for staging uploads
# ---------------------------------------------------------------------------


def _transporter_target(
    location: CloudLocation, key: str
) -> _transporter.TransporterLocation:
    """Build the ``TransporterLocation`` target for a staged file.

    The cloud-side credentials are passed via the transporter env (so
    they don't leak into request YAML on disk); the URL identifies the
    target object under ``location``.
    """
    if isinstance(location, S3Location):
        bucket = _convert(location.bucket, str)
        return _transporter.TransporterS3(
            url=f"s3://{bucket}/{key}",
            access_key=_transporter.TransporterEnv(
                _transporter.SERVER_SIDE_AWS_ACCESS_KEY_ID
            ),
            secret_key=_transporter.TransporterEnv(
                _transporter.SERVER_SIDE_AWS_SECRET_ACCESS_KEY
            ),
            region=_transporter.TransporterEnv(_transporter.SERVER_SIDE_AWS_REGION),
        )
    if isinstance(location, GCSLocation):
        bucket = _convert(location.bucket, str)
        return _transporter.TransporterGCS(
            url=f"gs://{bucket}/{key}",
            service_account_key=_transporter.TransporterEnv(
                _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON
            ),
        )
    if isinstance(location, AzBlobLocation):
        container = _convert(location.container, str)
        return _transporter.TransporterAzure(
            url=f"az://{container}/{key}",
            account_name=_transporter.TransporterEnv(
                _transporter.SERVER_SIDE_AZURE_ACCOUNT_NAME
            ),
            account_key=_transporter.TransporterEnv(
                _transporter.SERVER_SIDE_AZURE_ACCOUNT_KEY
            ),
        )
    # noinspection PyProtectedMember
    raise DruidErrorCode.DRUID_11.exception(kind=type(location).__name__)


def _transporter_env(location: CloudLocation) -> dict[str, str]:
    """Build the env dict the transporter binary needs for ``location``.

    Mirrors the per-flavor ``run_env`` helpers in the cloud-side
    connectors (s3, gcs, azureblob).
    """
    if isinstance(location, S3Location):
        creds = location.credentials
        return {
            _transporter.SERVER_SIDE_AWS_ACCESS_KEY_ID: _convert(creds.access_key_id, str),
            _transporter.SERVER_SIDE_AWS_SECRET_ACCESS_KEY: _convert(
                creds.secret_access_key, str,
            ),
            _transporter.SERVER_SIDE_AWS_REGION: _convert(location.region, str),
        }
    if isinstance(location, GCSLocation):
        return {
            _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON: _convert(
                location.credentials.json, str,
            ),
        }
    if isinstance(location, AzBlobLocation):
        creds = location.credentials
        return {
            _transporter.SERVER_SIDE_AZURE_ACCOUNT_NAME: _convert(creds.account_name, str),
            _transporter.SERVER_SIDE_AZURE_ACCOUNT_KEY: _convert(creds.account_key, str),
        }
    # noinspection PyProtectedMember
    raise DruidErrorCode.DRUID_11.exception(kind=type(location).__name__)


def upload_staged_files(
    *,
    staging: CloudLocation,
    parquet_files: list[Path],
    subdir: str,
    work_dir: Path,
) -> list[str]:
    """Copy ``parquet_files`` to ``staging/<subdir>`` via the
    transporter binary; return the list of staged-file URIs.

    Each parquet file is uploaded under a deterministic name --
    ``part-NNNNN-<basename>`` -- so ordering matches the input and so
    parallel writes never overlap. The function returns the list of
    cloud-native URIs Druid will see in the ingestion ``inputSource``.
    """
    pairs: list[
        tuple[_transporter.TransporterLocation, _transporter.TransporterLocation]
    ] = []
    uris: list[str] = []
    base_path = getattr(staging, "base_path", "") or ""
    for idx, src_path in enumerate(parquet_files):
        name = f"part-{idx:05d}-{Path(src_path).name}"
        key = _join_path(base_path, subdir, name)
        target = _transporter_target(staging, key)
        source = _transporter.TransporterLocalFile(
            _transporter.path_to_file_uri(Path(src_path))
        )
        pairs.append((source, target))
        uris.append(staged_uri(staging, key))

    env = _transporter_env(staging)
    _transporter_copy_one(
        source_target_pairs=pairs,
        work_dir=work_dir,
        format=_transporter.TransporterParquetFormat(),
        env=env,
    )
    return uris


# ---------------------------------------------------------------------------
# Druid inputSource (per CloudLocation flavor)
# ---------------------------------------------------------------------------


def build_input_source(location: CloudLocation, uris: list[str]) -> dict:
    """Translate ``location`` + a list of staged file URIs into the
    ``spec.ioConfig.inputSource`` block of a Druid ``index_parallel`` spec.

    The function passes per-task credentials in the spec so the Druid
    cluster does not need to be pre-configured with the destination's
    cloud account. This mirrors the legacy ``DruidCloudStorage``
    implementations in ``tabsdata._io.cloud_staging.druid``.
    """
    if isinstance(location, S3Location):
        creds = location.credentials
        return {
            "type": "s3",
            "uris": uris,
            "endpointConfig": {
                "signingRegion": _convert(location.region, str),
            },
            "properties": {
                "accessKeyId": _convert(creds.access_key_id, str),
                "secretAccessKey": _convert(creds.secret_access_key, str),
            },
        }
    if isinstance(location, GCSLocation):
        # The native ``google`` input source does not support per-task
        # credentials; the Druid cluster must have access via
        # ``GOOGLE_APPLICATION_CREDENTIALS`` baked in.
        return {
            "type": "google",
            "uris": uris,
        }
    if isinstance(location, AzBlobLocation):
        creds = location.credentials
        return {
            "type": "azureStorage",
            "uris": uris,
            "properties": {
                "key": _convert(creds.account_key, str),
            },
        }
    # noinspection PyProtectedMember
    raise DruidErrorCode.DRUID_11.exception(kind=type(location).__name__)


# ---------------------------------------------------------------------------
# Ingestion-spec builder
# ---------------------------------------------------------------------------


def build_ingestion_spec(
    *,
    datasource: str,
    input_source: dict,
    is_replace: bool,
    time_column: str | None,
    segment_granularity: str,
    max_concurrent_subtasks: int,
    max_rows_per_segment: int,
    max_rows_in_memory: int,
) -> dict:
    """Build a native batch ``index_parallel`` ingestion spec.

    The spec uses ``useSchemaDiscovery`` so Druid auto-detects columns
    from the Parquet metadata, ``rollup: False`` to ingest raw rows,
    and ``queryGranularity: none`` to preserve row-level timestamps.
    """
    if time_column:
        timestamp_spec = {"column": time_column, "format": "auto"}
    else:
        # Druid still needs a primary timestamp column. Use a dummy
        # one with a constant value so all rows land in a single
        # time chunk. The synthetic name must not collide with any
        # user-defined column.
        timestamp_spec = {
            "column": DUMMY_TIME_COLUMN,
            "format": "auto",
            "missingValue": "2000-01-01T00:00:00.000Z",
        }

    granularity_spec: dict = {
        "segmentGranularity": segment_granularity,
        "queryGranularity": "none",
        "rollup": False,
    }
    if is_replace:
        # ``dropExisting: True`` requires bounded intervals so Druid
        # knows which segments to drop. The all-encompassing interval
        # covers any possible timestamp value.
        granularity_spec["intervals"] = ["1970-01-01/2100-01-01"]

    return {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": timestamp_spec,
                "dimensionsSpec": {
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
                "maxNumConcurrentSubTasks": max_concurrent_subtasks,
                "maxRowsInMemory": max_rows_in_memory,
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": max_rows_per_segment,
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# Overlord HTTP client
# ---------------------------------------------------------------------------


_ALLOWED_METHODS = frozenset({"get", "post"})


@dataclass
class OverlordClient:
    """Thin HTTP client around the Druid Overlord REST API.

    Wraps a ``requests.Session`` with HTTP basic auth and a small
    retry loop for transient failures. Construction is cheap; the
    plugin builds a fresh client per ``write_out`` call and closes
    it (via ``close``) when ingestion finishes.
    """

    base_url: str
    auth: tuple[str, str] | None = None
    timeout_sec: int = 300
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_backoff: float = DEFAULT_RETRY_BACKOFF

    def __post_init__(self) -> None:
        import requests

        self._requests = requests
        self._session = requests.Session()
        if self.auth is not None:
            self._session.auth = self.auth
        self._session.headers["Content-Type"] = "application/json"

    # ----- lifecycle -----

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> OverlordClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ----- URLs -----

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{path}"

    def _safe_url(self, path: str) -> str:
        return f"{_sanitize_url(self.base_url.rstrip('/'))}{path}"

    # ----- request helpers -----

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Any:
        if method not in _ALLOWED_METHODS:
            raise ValueError(
                f"unsupported HTTP method {method!r}; "
                f"expected one of {sorted(_ALLOWED_METHODS)}"
            )
        kwargs.setdefault("timeout", self.timeout_sec)
        last_attempt = self.retry_attempts - 1
        last_exc: Exception | None = None
        for attempt in range(self.retry_attempts):
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
                if status not in RETRYABLE_STATUS_CODES or attempt == last_attempt:
                    raise
                last_exc = e

            wait = self.retry_backoff * (2**attempt)
            logger.warning(
                "Druid request failed (attempt %d/%d), retrying in %.1fs: %s",
                attempt + 1,
                self.retry_attempts,
                wait,
                last_exc,
            )
            time.sleep(wait)
        # Unreachable
        raise RuntimeError("retry loop exited without return / raise")

    # ----- Druid operations -----

    def submit_task(self, spec: dict) -> str:
        """POST a task spec; return the task id."""
        path = "/druid/indexer/v1/task"
        resp = self._request_with_retry("post", self._url(path), json=spec)
        try:
            payload = resp.json()
        except ValueError as e:
            # noinspection PyProtectedMember
            raise DruidErrorCode.DRUID_10.exception(
                cause=e,
                url=self._safe_url(path),
                reason=f"non-JSON response (status={resp.status_code})",
            )
        task_id = payload.get("task")
        if not isinstance(task_id, str) or not task_id:
            # noinspection PyProtectedMember
            raise DruidErrorCode.DRUID_10.exception(
                url=self._safe_url(path),
                reason="response missing 'task' field",
            )
        return task_id

    def task_status(self, task_id: str) -> tuple[str, dict]:
        """GET the Overlord status payload for ``task_id``.

        Returns ``(status_code, payload)`` where ``status_code`` is
        Druid's status (``RUNNING`` / ``WAITING`` / ``SUCCESS`` / ...).
        """
        path = f"/druid/indexer/v1/task/{task_id}/status"
        resp = self._request_with_retry("get", self._url(path))
        try:
            payload = resp.json()
        except ValueError as e:
            # noinspection PyProtectedMember
            raise DruidErrorCode.DRUID_10.exception(
                cause=e,
                url=self._safe_url(path),
                reason=f"non-JSON status response (status={resp.status_code})",
            )
        try:
            status_code = payload["status"]["statusCode"]
        except (KeyError, TypeError) as e:
            # noinspection PyProtectedMember
            raise DruidErrorCode.DRUID_10.exception(
                cause=e,
                url=self._safe_url(path),
                reason="status response missing 'status.statusCode'",
            )
        return status_code, payload

    def task_reports(self, task_id: str) -> dict:
        """Fetch the per-task ingestion reports (best-effort).

        Returns an empty dict if the report can't be fetched -- the
        caller should treat a missing report as a non-fatal log line.
        """
        path = f"/druid/indexer/v1/task/{task_id}/reports"
        try:
            resp = self._request_with_retry("get", self._url(path))
            return resp.json()
        except Exception:
            logger.exception(
                "unable to fetch task reports for %s",
                task_id,
            )
            return {}

    def wait_for_completion(self, task_id: str) -> None:
        """Poll until ``task_id`` terminates; raise on failure.

        Returns silently on ``SUCCESS``; on any other terminal status
        (``FAILED``, ``UNKNOWN``, ...) raises ``DRUID_9`` with a
        best-effort error message extracted from the task report.
        """
        while True:
            status_code, _ = self.task_status(task_id)
            if status_code in ("RUNNING", "WAITING"):
                time.sleep(self.poll_interval_s)
                continue
            if status_code == "SUCCESS":
                logger.info("Druid ingestion task %s completed", task_id)
                return
            reason = self._extract_error(task_id, status_code)
            # noinspection PyProtectedMember
            raise DruidErrorCode.DRUID_9.exception(
                datasource="?",
                task_id=task_id,
                reason=reason,
            )

    def _extract_error(self, task_id: str, status_code: str) -> str:
        report = self.task_reports(task_id)
        stats = report.get("ingestionStatsAndErrors", {}).get("payload", {})
        primary = stats.get("errorMsg") or f"status={status_code}"
        details: list[str] = []
        error_msgs = stats.get("errorMsgs")
        if isinstance(error_msgs, list):
            for entry in error_msgs[:5]:
                if isinstance(entry, str) and entry:
                    details.append(entry)
                elif isinstance(entry, dict):
                    details.append(str(entry))
        rows = stats.get("rowStats", {}).get("totals", {}).get("processed")
        if rows is not None:
            details.append(f"rows processed before failure: {rows}")
        if details:
            return f"{primary} | details: {'; '.join(details)}"
        return primary


# ---------------------------------------------------------------------------
# Factory helpers used by the plugin
# ---------------------------------------------------------------------------


def make_overlord_client(conn: DruidConn) -> OverlordClient:
    creds = conn.credentials
    auth = (_convert(creds.user, str), _convert(creds.password, str))
    return OverlordClient(
        base_url=_convert(conn.overlord_url, str),
        auth=auth,
    )


def make_overlord_client_for_dest(conn: DruidConn, dest: DruidDest) -> OverlordClient:
    """Build an Overlord client whose ``timeout_sec`` reflects the
    destination's ``http_timeout_sec`` knob."""
    creds = conn.credentials
    auth = (_convert(creds.user, str), _convert(creds.password, str))
    return OverlordClient(
        base_url=_convert(conn.overlord_url, str),
        auth=auth,
        timeout_sec=dest.http_timeout_sec,
    )
