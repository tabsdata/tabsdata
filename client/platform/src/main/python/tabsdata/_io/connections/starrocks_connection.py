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
from tabsdata._io.connections.connections import Conn
from tabsdata._utils.annotations import unstable, unstable_category
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

#: Type alias for the accepted cloud staging providers.
StagingConfig = Union[S3Config, AzureConfig, GCSConfig]


@unstable()
@deprecated(
    """Class 'StarRocksConn' is considered unstable and is subject to change
     in future releases.""",  # noqa: E231,E241,E501
    category=unstable_category(
        reason=(
            "Class 'StarRocksConn' may undergo API and feature changes as it evolves"
            " toward full capability and stability."
        ),
        since="1.8.0",
        replacement="There is no current stable replacement for this class.",
    ),
)
class StarRocksConn(Conn):
    """StarRocks connection config.

    Required: host, port, database, credentials, staging.
    Represents a connection configuration for StarRocks with cloud-staged bulk
    loading.

    StarRocks ingests data via cloud storage (S3, Azure Blob, or GCS): files are
    first uploaded to the configured staging area and then loaded into StarRocks
    using the ``FILES()`` table function in an ``INSERT INTO … SELECT * FROM
    FILES(...)`` statement.
    """

    @_td_validate_call(ErrorCode.DECE110)
    def __init__(
        self,
        host: str,
        staging: StagingConfig,
        database: str = "default",
        credentials: UserPasswordCredentials | None = None,
        enforce_connection_params: bool = True,
        cx_src_configs_starrocks: dict | None = None,
        cx_dst_configs_starrocks: dict | None = None,
    ):
        """
        Represents a connection configuration for StarRocks with cloud-staged bulk
        loading.

        StarRocks ingests data via cloud storage (S3, Azure Blob, or GCS): files are
        first uploaded to the configured staging area and then loaded into StarRocks
        using the ``FILES()`` table function in an ``INSERT INTO … SELECT * FROM
        FILES(...)`` statement.

        Args:
            host: StarRocks FE hostname or IP address.
            staging: Cloud storage staging configuration.  Provide one of:

                * :class:`~tabsdata._io.cloud_staging.common.storage_config.S3Config`
                  — stage files in Amazon S3 and read with StarRocks' ``FILES()``
                  table function using ``s3://`` paths.
                * :class:`~tabsdata._io.cloud_staging.common.storage_config.AzureConfig`
                  — stage files in Azure Blob Storage and read with ``FILES()``
                  using ``wasbs://`` paths.
                * :class:`~tabsdata._io.cloud_staging.common.storage_config.GCSConfig`
                  — stage files in Google Cloud Storage and read with ``FILES()``
                  using ``s3a://`` paths via the GCS S3-compatible HMAC API.

            port: MySQL protocol port of the StarRocks FE server.  Defaults to
                ``9030``.
            database: Target database name.  Defaults to ``"default"``.
            credentials: :class:`~tabsdata.UserPasswordCredentials` for
                StarRocks authentication.  When ``None`` the StarRocks ``root``
                user with no password is used.
            enforce_connection_params: Whether to enforce connection parameters for
                the StarRocks connection.  Applicable only for connections to a
                StarRocks destination.  Defaults to ``True``.
            cx_src_configs_starrocks: Source-specific configuration parameters for
                the StarRocks connection.  Defaults to ``None``.
            cx_dst_configs_starrocks: Destination-specific configuration parameters
                for the StarRocks connection.  Defaults to ``None``.
        """
        self._host = host
        self._port = port
        self._database = database
        self._credentials = self._build_credentials(credentials)
        self._staging = staging
        self._enforce_connection_params = enforce_connection_params
        self._cx_src_configs_starrocks = cx_src_configs_starrocks or {}
        self._cx_dst_configs_starrocks = cx_dst_configs_starrocks or {}

    # ── StarRocks server parameters ───────────────────────────────────────────────────

    @property
    def host(self) -> str:
        """
        The StarRocks FE hostname or IP address.
        """
        return self._host

    @property
    def port(self) -> int:
        """
        The MySQL protocol port of the StarRocks FE server.
        """
        return self._port

    @property
    def database(self) -> str:
        """
        The target StarRocks database name.
        """
        return self._database

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        The credentials used to authenticate with StarRocks.  ``None`` if no
        credentials were provided (StarRocks ``root`` user, no password).
        """
        return self._credentials

    # ── Cloud staging parameters ──────────────────────────────────────────────────────

    @property
    def staging(self) -> StagingConfig:
        """
        The cloud storage staging configuration (S3Config, AzureConfig, or
        GCSConfig).
        """
        return self._staging

    # ── Connection behaviour parameters ───────────────────────────────────────────────

    @property
    def enforce_connection_params(self) -> bool:
        """
        Whether to enforce connection parameters for the StarRocks connection.
        """
        return self._enforce_connection_params

    @property
    def cx_src_configs_starrocks(self) -> dict:
        """
        Source-specific configuration parameters for the StarRocks connection.
        """
        return self._cx_src_configs_starrocks

    @property
    def cx_dst_configs_starrocks(self) -> dict:
        """
        Destination-specific configuration parameters for the StarRocks connection.
        """
        return self._cx_dst_configs_starrocks

    # ── Internal helpers ──────────────────────────────────────────────────────────────

    def _build_credentials(
        self, credentials: UserPasswordCredentials | None
    ) -> UserPasswordCredentials | None:
        if credentials:
            credentials = build_credentials(credentials)
        return credentials

    # ── Dunder methods ────────────────────────────────────────────────────────────────

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "StarRocksConn":
        configs = dict(configs)
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs["credentials"] = UserPasswordCredentials(**credentials)
        staging = configs.get("staging")
        if isinstance(staging, dict):
            configs["staging"] = build_staging_from_dict(staging)
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StarRocksConn):
            return False
        return (
            self.host == other.host
            and self.port == other.port
            and self.database == other.database
            and self.credentials == other.credentials
            and self.staging == other.staging
            and self.enforce_connection_params == other.enforce_connection_params
            and self.cx_src_configs_starrocks == other.cx_src_configs_starrocks
            and self.cx_dst_configs_starrocks == other.cx_dst_configs_starrocks
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"host={self.host!r}, "
            f"port={self.port!r}, "
            f"database={self.database!r}, "
            f"enforce_connection_params={self.enforce_connection_params!r}, "
            f"cx_src_configs_starrocks={self.cx_src_configs_starrocks!r}, "
            f"cx_dst_configs_starrocks={self.cx_dst_configs_starrocks!r}, "
            f"staging={self.staging!r})"
        )
