#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import Union

from typing_extensions import deprecated

from tabsdata._credentials import UserPasswordCredentials, build_credentials
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
    build_staging_from_dict,
)
from tabsdata._io.cloud_staging.druid.config import SegmentGranularitySpec
from tabsdata._io.connections.connections import Conn
from tabsdata._utils.annotations import unstable, unstable_category
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

#: Type alias for the accepted cloud staging providers.
StagingConfig = Union[S3Config, AzureConfig, GCSConfig]


@unstable()
@deprecated(
    """Class 'DruidConn' is considered unstable and is subject to change
     in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'DruidConn' may undergo API and feature changes as it evolves"
            " toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class DruidConn(Conn):
    """Apache Druid connection config.

    Required: host, port, database, credentials, staging.
    Represents a connection configuration for Apache Druid with cloud-staged
    bulk loading.

    Data is first uploaded to the configured cloud staging area (S3, Azure
    Blob, or GCS) and then ingested into Druid via the Overlord REST API
    using a native batch ``index_parallel`` ingestion task.
    """

    @_td_validate_call(ErrorCode.DECE111)
    def __init__(
        self,
        overlord_url: str,
        staging: StagingConfig,
        time_column: str | None = None,
        segment_granularity: SegmentGranularitySpec = "ALL",
        max_concurrent_subtasks: int = 1,
        max_rows_per_segment: int = 5_000_000,
        max_rows_in_memory: int = 1_000_000,
        http_timeout_sec: int = 300,
        credentials: UserPasswordCredentials | None = None,
    ):
        """
        Represents a connection configuration for Apache Druid with cloud-staged
        bulk loading.

        Data is first uploaded to the configured cloud staging area (S3, Azure
        Blob, or GCS) and then ingested into Druid via the Overlord REST API
        using a native batch ``index_parallel`` ingestion task.

        Args:
            overlord_url: Base URL of the Druid Overlord REST API
                (e.g. ``"http://druid-overlord:8090"``).
            staging: Cloud storage staging configuration.  Provide one of:

                * :class:`~tabsdata._io.cloud_staging.common.storage_config.S3Config`
                  — stage files in Amazon S3.
                * :class:`~tabsdata._io.cloud_staging.common.storage_config.AzureConfig`
                  — stage files in Azure Blob Storage.
                * :class:`~tabsdata._io.cloud_staging.common.storage_config.GCSConfig`
                  — stage files in Google Cloud Storage.

            time_column: Name of the PyArrow column to use as Druid's required
                ``__time`` primary timestamp.  When ``None`` a constant value is
                used (``2000-01-01T00:00:00.000Z``).  Defaults to ``None``.
            segment_granularity: Time-based partitioning granularity.  One of
                ``"ALL"``, ``"DAY"``, ``"HOUR"``, ``"MONTH"``, or ``"YEAR"``.
                Defaults to ``"ALL"``.
            max_concurrent_subtasks: Maximum number of parallel sub-tasks inside
                the ``index_parallel`` ingestion task.  When set to ``1`` the
                supervisor runs inline without spawning subtasks.  Defaults to
                ``1``.
            max_rows_per_segment: Dynamic partitioning threshold — maximum number
                of rows per segment.  Defaults to ``5_000_000``.
            max_rows_in_memory: In-memory buffer size during ingestion.
                Defaults to ``1_000_000``.
            http_timeout_sec: Timeout in seconds for HTTP requests to the Overlord
                API.  Defaults to ``300``.
            credentials: :class:`~tabsdata.UserPasswordCredentials` for
                HTTP Basic authentication against the Overlord API, or ``None``
                (the default) to skip authentication.
        """
        self._overlord_url = overlord_url
        self._staging = staging
        self._time_column = time_column
        self._segment_granularity = segment_granularity
        self._max_concurrent_subtasks = max_concurrent_subtasks
        self._max_rows_per_segment = max_rows_per_segment
        self._max_rows_in_memory = max_rows_in_memory
        self._http_timeout_sec = http_timeout_sec
        self._credentials = self._build_credentials(credentials)

    # ── Druid Overlord parameters ─────────────────────────────────────────────────────

    @property
    def overlord_url(self) -> str:
        """
        The base URL of the Druid Overlord REST API.
        """
        return self._overlord_url

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        The credentials used for HTTP Basic authentication.  ``None`` if no
        credentials were provided.
        """
        return self._credentials

    # ── Ingestion tuning parameters ───────────────────────────────────────────────────

    @property
    def time_column(self) -> str | None:
        """
        The PyArrow column to use as Druid's ``__time`` primary timestamp.
        ``None`` if a constant value should be used.
        """
        return self._time_column

    @property
    def segment_granularity(self) -> SegmentGranularitySpec:
        """
        Time-based partitioning granularity for new segments.
        """
        return self._segment_granularity

    @property
    def max_concurrent_subtasks(self) -> int:
        """
        Maximum number of parallel sub-tasks inside the ingestion task.
        """
        return self._max_concurrent_subtasks

    @property
    def max_rows_per_segment(self) -> int:
        """
        Maximum number of rows per segment (dynamic partitioning threshold).
        """
        return self._max_rows_per_segment

    @property
    def max_rows_in_memory(self) -> int:
        """
        In-memory buffer size during ingestion.
        """
        return self._max_rows_in_memory

    @property
    def http_timeout_sec(self) -> int:
        """
        Timeout in seconds for HTTP requests to the Overlord API.
        """
        return self._http_timeout_sec

    # ── Cloud staging parameters ──────────────────────────────────────────────────────

    @property
    def staging(self) -> StagingConfig:
        """
        The cloud storage staging configuration (S3Config, AzureConfig, or
        GCSConfig).
        """
        return self._staging

    # ── Internal helpers ──────────────────────────────────────────────────────────────

    def _build_credentials(
        self, credentials: UserPasswordCredentials | None
    ) -> UserPasswordCredentials | None:
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    # ── Dunder methods ────────────────────────────────────────────────────────────────

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "DruidConn":
        configs = dict(configs)
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs["credentials"] = UserPasswordCredentials(**credentials)
        staging = configs.get("staging")
        if isinstance(staging, dict):
            configs["staging"] = build_staging_from_dict(staging)
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DruidConn):
            return False
        return (
            self.overlord_url == other.overlord_url
            and self.staging == other.staging
            and self.time_column == other.time_column
            and self.segment_granularity == other.segment_granularity
            and self.max_concurrent_subtasks == other.max_concurrent_subtasks
            and self.max_rows_per_segment == other.max_rows_per_segment
            and self.max_rows_in_memory == other.max_rows_in_memory
            and self.http_timeout_sec == other.http_timeout_sec
            and self.credentials == other.credentials
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"overlord_url={self.overlord_url!r}, "
            f"time_column={self.time_column!r}, "
            f"segment_granularity={self.segment_granularity!r}, "
            f"max_concurrent_subtasks={self.max_concurrent_subtasks!r}, "
            f"max_rows_per_segment={self.max_rows_per_segment!r}, "
            f"max_rows_in_memory={self.max_rows_in_memory!r}, "
            f"http_timeout_sec={self.http_timeout_sec!r}, "
            f"staging={self.staging!r})"
        )
