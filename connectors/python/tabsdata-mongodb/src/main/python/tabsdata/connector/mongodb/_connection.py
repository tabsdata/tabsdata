#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._io.connections.connections import Conn
from tabsdata._utils.validation import _td_validate_call
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MongoDBConn(Conn):
    """
    Connection configuration for MongoDB.
    """

    @_td_validate_call(ErrorCode.DECE117)
    def __init__(
        self,
        uri: str,
        credentials: UserPasswordCredentials | None = None,
        connection_options: dict | None = None,
    ):
        """
        Initialize a MongoDBConn instance.

        Args:
            uri: The MongoDB connection URI.
            credentials: Optional UserPasswordCredentials. If None, no
                credentials are used when connecting.
            connection_options: Optional dict of options forwarded to the
                pymongo.MongoClient constructor.
        """
        self.uri = uri
        self.credentials = credentials
        self.connection_options = connection_options

    @property
    def uri(self) -> str:
        """
        The MongoDB connection URI.
        """
        return self._uri

    @uri.setter
    @_td_validate_call(ErrorCode.DECE117)
    def uri(self, uri: str):
        self._uri = uri

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        """
        The MongoDB credentials, if any.
        """
        return self._credentials

    @credentials.setter
    @_td_validate_call(ErrorCode.DECE117)
    def credentials(self, credentials: UserPasswordCredentials | None):
        self._credentials = credentials

    @property
    def connection_options(self) -> dict:
        """
        Options forwarded to the pymongo.MongoClient constructor. Always a
        dict; ``None`` is normalized to ``{}``.
        """
        return self._connection_options

    @connection_options.setter
    @_td_validate_call(ErrorCode.DECE117)
    def connection_options(self, connection_options: dict | None):
        self._connection_options = connection_options or {}

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "MongoDBConn":
        """
        Construct a MongoDBConn from a plain dict of configs, e.g. one loaded
        from a NamedConn JSON catalog entry.

        ``credentials`` may be a nested dict ``{"user": ..., "password": ...}``
        which is hydrated into a ``UserPasswordCredentials`` first.
        """
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = UserPasswordCredentials(**credentials)
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MongoDBConn):
            return False
        return (
            self.uri == other.uri
            and self.credentials == other.credentials
            and self.connection_options == other.connection_options
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"uri={self.uri!r}, "
            f"credentials={self.credentials}, "
            f"connection_options={self.connection_options})"
        )
