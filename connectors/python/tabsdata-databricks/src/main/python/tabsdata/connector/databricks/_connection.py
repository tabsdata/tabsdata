#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdata._io.connections.connections import Conn
from tabsdata._secret import Secret, build_secret
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DatabricksConn(Conn):
    """Databricks connection config.

    Required: server_hostname, http_path, access_token.
    Connection configuration for Databricks.
    """

    @_td_validate_call(ErrorCode.DECE101)
    def __init__(
        self,
        host_url: str,
        token: str | Secret,
    ):
        """
        Initialize a DatabricksConn instance.

        Args:
            host_url: The Databricks workspace URL.
            token: The Databricks Personal Access Token (PAT). Can be a plain
                string (wrapped as a DirectSecret) or a Secret object
                (EnvironmentSecret, HashiCorpSecret, etc.).
        """
        self.host_url = host_url
        self.token = token

    @property
    def host_url(self) -> str:
        """
        The Databricks workspace URL.
        """
        return self._host_url

    @host_url.setter
    @_td_validate_call(ErrorCode.DECE101)
    def host_url(self, host_url: str):
        self._host_url = host_url

    @property
    def token(self) -> Secret:
        """
        The Databricks PAT, wrapped as a Secret.
        """
        return self._token

    @token.setter
    @_td_validate_call(ErrorCode.DECE101)
    def token(self, token: str | Secret):
        self._token = build_secret(token)

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "DatabricksConn":
        """
        Construct a DatabricksConn from a plain dict of configs, e.g. one
        loaded from a NamedConn JSON catalog entry.

        The ``token`` value may be a plain string (wrapped as a DirectSecret)
        or a nested dict specifying the secret type.
        """
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DatabricksConn):
            return False
        return self.host_url == other.host_url and self.token == other.token

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(host_url={self.host_url!r}, token={self.token})"
        )
