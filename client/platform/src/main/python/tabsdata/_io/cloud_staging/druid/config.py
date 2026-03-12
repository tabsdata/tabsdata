#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse

#: Accepted values for ``segment_granularity``.
SegmentGranularitySpec = Literal["ALL", "DAY", "HOUR", "MONTH", "YEAR"]


def _sanitize_url(url: str) -> str:
    """Remove embedded credentials from a URL for safe logging/error messages.

    ``http://user:pass@host:8090/path`` → ``http://***:***@host:8090/path``
    """
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        # Rebuild netloc without credentials
        host_part = parsed.hostname or ""
        if parsed.port:
            host_part = f"{host_part}:{parsed.port}"
        sanitized_netloc = f"***:***@{host_part}"
        return parsed._replace(netloc=sanitized_netloc).geturl()
    return url


@dataclass(frozen=True, repr=False)
class DruidConfig:
    """Connection and tuning parameters for an Apache Druid Overlord.

    Args:
        overlord_url: Base URL of the Druid Overlord REST API
            (e.g. ``"http://druid-overlord:8090"``).  Must use ``http`` or
            ``https`` scheme and include a hostname.  Embedded credentials
            in the URL (e.g. ``http://user:pass@host``) are not supported
            — use ``auth_user`` / ``auth_password`` instead.
        time_column: Name of the PyArrow column to use as Druid's required
            ``__time`` primary timestamp.  When ``None`` a constant value
            (``2000-01-01T00:00:00.000Z``) is used and ``segmentGranularity``
            defaults to ``"ALL"``.
        segment_granularity: Time-based partitioning granularity for new
            segments.  One of ``"ALL"``, ``"DAY"``, ``"HOUR"``, ``"MONTH"``,
            or ``"YEAR"``.  Defaults to ``"ALL"``.
        max_concurrent_subtasks: Maximum number of parallel sub-tasks inside
            the ``index_parallel`` ingestion task.  When set to ``1`` the
            supervisor runs inline without spawning subtasks.  Must be ≥ 1.
            Defaults to ``1``.
        max_rows_per_segment: Dynamic partitioning threshold — maximum number
            of rows per segment.  Must be ≥ 1.  Defaults to ``5_000_000``.
        max_rows_in_memory: In-memory buffer size during ingestion.
            Must be ≥ 1.  Defaults to ``1_000_000``.
        http_timeout: Timeout in seconds for HTTP requests to the Overlord
            API.  Must be ≥ 1.  Defaults to ``300``.
        auth_user: Username for HTTP Basic authentication against the
            Overlord API, or ``None`` (the default) to skip authentication.
        auth_password: Password for HTTP Basic authentication, or ``None``
            (the default).
    """

    overlord_url: str
    time_column: str | None = None
    segment_granularity: SegmentGranularitySpec = "ALL"
    max_concurrent_subtasks: int = 1
    max_rows_per_segment: int = 5_000_000
    max_rows_in_memory: int = 1_000_000
    http_timeout: int = 300
    auth_user: str | None = None
    auth_password: str | None = None

    def __post_init__(self) -> None:
        # ── URL validation ────────────────────────────────────────────────
        parsed = urlparse(self.overlord_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(
                f"overlord_url must use http or https scheme, "
                f"got {parsed.scheme!r}: {self.overlord_url!r}"
            )
        if not parsed.hostname:
            raise ValueError(
                f"overlord_url must include a hostname: {self.overlord_url!r}"
            )
        if parsed.username or parsed.password:
            raise ValueError(
                "overlord_url must not contain embedded credentials. "
                "Use auth_user / auth_password instead."
            )
        if parsed.query:
            raise ValueError(
                f"overlord_url must not contain a query string: {self.overlord_url!r}"
            )
        if parsed.fragment:
            raise ValueError(
                f"overlord_url must not contain a URL fragment: {self.overlord_url!r}"
            )

        # ── Numeric range validation ──────────────────────────────────────
        if self.max_concurrent_subtasks < 1:
            raise ValueError(
                f"max_concurrent_subtasks must be >= 1, "
                f"got {self.max_concurrent_subtasks}"
            )
        if self.max_rows_per_segment < 1:
            raise ValueError(
                f"max_rows_per_segment must be >= 1, got {self.max_rows_per_segment}"
            )
        if self.max_rows_in_memory < 1:
            raise ValueError(
                f"max_rows_in_memory must be >= 1, got {self.max_rows_in_memory}"
            )
        if self.http_timeout < 1:
            raise ValueError(f"http_timeout must be >= 1, got {self.http_timeout}")

    def __repr__(self) -> str:
        masked_password = "'***'" if self.auth_password else repr(None)
        return (
            f"{self.__class__.__name__}("
            f"overlord_url={self.overlord_url!r}, "
            f"time_column={self.time_column!r}, "
            f"segment_granularity={self.segment_granularity!r}, "
            f"max_concurrent_subtasks={self.max_concurrent_subtasks!r}, "
            f"max_rows_per_segment={self.max_rows_per_segment!r}, "
            f"max_rows_in_memory={self.max_rows_in_memory!r}, "
            f"http_timeout={self.http_timeout!r}, "
            f"auth_user={self.auth_user!r}, "
            f"auth_password={masked_password})"
        )
