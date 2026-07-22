#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from typing import List

from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.outputs.shared_enums import IfTableExistStrategySpec
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.mongodb._connection import MongoDBConn
from tabsdata.connector.mongodb._connector import MongoDBDestination
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MongoDBDest(DestinationPlugin):
    """Write to MongoDB.

    Required: conn (MongoDBConn),
    destination_tables (str or list[str]).
    Destination plugin for writing data to MongoDB. Thin proxy around
    MongoDBDestination that takes a MongoDBConn for the connection
    parameters and forwards all other options.
    """

    @_td_validate_call(ErrorCode.DECE118)
    def __init__(
        self,
        conn: MongoDBConn | NamedConn,
        collections_with_ids: tuple[str, str | None] | List[tuple[str, str | None]],
        if_collection_exists: IfTableExistStrategySpec = "append",
        use_trxs: bool = False,
        docs_per_trx: int = 1000,
        maintain_order: bool = False,
        update_existing: bool = True,
        fail_on_duplicate_key: bool = True,
        log_intermediate_files: bool = False,
        **kwargs,
    ):
        """
        Initialize a MongoDBDest instance.

        Args:
            conn: The MongoDB connection configuration (uri, credentials,
                connection_options).
            collections_with_ids: A tuple or list of tuples
                ``(collection, id_field)`` describing each target. ``id_field``
                may be ``None`` to let MongoDB autogenerate IDs.
            if_collection_exists: ``"append"`` or ``"replace"``. Defaults to
                ``"append"``.
            use_trxs: Whether to wrap writes in MongoDB transactions
                (requires a replica set). Defaults to False.
            docs_per_trx: Maximum documents per transaction. Defaults to 1000.
            maintain_order: Whether to preserve document order. Defaults to
                False.
            update_existing: Whether to upsert existing documents. Defaults to
                True.
            fail_on_duplicate_key: Whether to raise on duplicate-key errors.
                Defaults to True.
            log_intermediate_files: Whether to log per intermediate batch.
                Defaults to False.
        """
        # Stash all non-conn state first so the conn setter can rebuild the
        # inner MongoDBDestination with a complete picture.
        self._collections_with_ids = collections_with_ids
        self._if_collection_exists = if_collection_exists
        self._use_trxs = use_trxs
        self._docs_per_trx = docs_per_trx
        self._maintain_order = maintain_order
        self._update_existing = update_existing
        self._fail_on_duplicate_key = fail_on_duplicate_key
        self._log_intermediate_files = log_intermediate_files
        self._kwargs = kwargs
        self._mongodb_destination: MongoDBDestination | None = None
        # Assigning conn triggers the setter, which (re)builds the inner
        # MongoDBDestination when conn is a resolved MongoDBConn.
        self.conn = conn

    def _build_destination(self) -> MongoDBDestination:
        return MongoDBDestination(
            uri=self._conn.uri,
            collections_with_ids=self._collections_with_ids,
            credentials=self._conn.credentials,
            connection_options=self._conn.connection_options,
            if_collection_exists=self._if_collection_exists,
            use_trxs=self._use_trxs,
            docs_per_trx=self._docs_per_trx,
            maintain_order=self._maintain_order,
            update_existing=self._update_existing,
            fail_on_duplicate_key=self._fail_on_duplicate_key,
            log_intermediate_files=self._log_intermediate_files,
            **self._kwargs,
        )

    @property
    def conn(self) -> MongoDBConn | NamedConn:
        """
        The MongoDB connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.DECE118)
    def conn(self, conn: MongoDBConn | NamedConn):
        self._conn = conn
        if isinstance(conn, MongoDBConn):
            self._mongodb_destination = self._build_destination()
        else:
            self._mongodb_destination = None

    @property
    def collections_with_ids(self) -> List[tuple[str, str | None]]:
        """
        Target collections with their id-field designations.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.collections_with_ids
        return (
            [self._collections_with_ids]
            if isinstance(self._collections_with_ids, tuple)
            else self._collections_with_ids
        )

    @collections_with_ids.setter
    def collections_with_ids(
        self,
        collections_with_ids: tuple[str, str | None] | List[tuple[str, str | None]],
    ):
        if self._mongodb_destination is not None:
            self._mongodb_destination.collections_with_ids = collections_with_ids
        self._collections_with_ids = collections_with_ids

    @property
    def if_collection_exists(self) -> IfTableExistStrategySpec:
        """
        The strategy to follow when the target collection already exists.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.if_collection_exists
        return self._if_collection_exists

    @if_collection_exists.setter
    def if_collection_exists(self, if_collection_exists: IfTableExistStrategySpec):
        if self._mongodb_destination is not None:
            self._mongodb_destination.if_collection_exists = if_collection_exists
        self._if_collection_exists = if_collection_exists

    @property
    def use_trxs(self) -> bool:
        """
        Whether writes are wrapped in MongoDB transactions.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.use_trxs
        return self._use_trxs

    @use_trxs.setter
    def use_trxs(self, use_trxs: bool):
        if self._mongodb_destination is not None:
            self._mongodb_destination.use_trxs = use_trxs
        self._use_trxs = use_trxs

    @property
    def docs_per_trx(self) -> int:
        """
        Maximum documents per transaction / batch.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.docs_per_trx
        return self._docs_per_trx

    @docs_per_trx.setter
    def docs_per_trx(self, docs_per_trx: int):
        if self._mongodb_destination is not None:
            self._mongodb_destination.docs_per_trx = docs_per_trx
        self._docs_per_trx = docs_per_trx

    @property
    def maintain_order(self) -> bool:
        """
        Whether document order is preserved during writes.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.maintain_order
        return self._maintain_order

    @maintain_order.setter
    def maintain_order(self, maintain_order: bool):
        if self._mongodb_destination is not None:
            self._mongodb_destination.maintain_order = maintain_order
        self._maintain_order = maintain_order

    @property
    def update_existing(self) -> bool:
        """
        Whether existing documents are upserted.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.update_existing
        return self._update_existing

    @update_existing.setter
    def update_existing(self, update_existing: bool):
        if self._mongodb_destination is not None:
            self._mongodb_destination.update_existing = update_existing
        self._update_existing = update_existing

    @property
    def fail_on_duplicate_key(self) -> bool:
        """
        Whether duplicate-key errors abort the write.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.fail_on_duplicate_key
        return self._fail_on_duplicate_key

    @fail_on_duplicate_key.setter
    def fail_on_duplicate_key(self, fail_on_duplicate_key: bool):
        if self._mongodb_destination is not None:
            self._mongodb_destination.fail_on_duplicate_key = fail_on_duplicate_key
        self._fail_on_duplicate_key = fail_on_duplicate_key

    @property
    def log_intermediate_files(self) -> bool:
        """
        Whether to log each intermediate batch as it is written.
        """
        if self._mongodb_destination is not None:
            return self._mongodb_destination.log_intermediate_files
        return self._log_intermediate_files

    @log_intermediate_files.setter
    def log_intermediate_files(self, log_intermediate_files: bool):
        if self._mongodb_destination is not None:
            self._mongodb_destination.log_intermediate_files = log_intermediate_files
        self._log_intermediate_files = log_intermediate_files

    def stream(self, working_dir: str, *results):
        """
        Write the results to MongoDB.
        """
        if self._mongodb_destination is None:
            raise RuntimeError(
                "Cannot call stream() before the 'conn' param has been resolved to a "
                "MongoDBConn."
            )
        return self._mongodb_destination.stream(working_dir, *results)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"collections_with_ids={self.collections_with_ids}, "
            f"if_collection_exists={self.if_collection_exists!r}, "
            f"use_trxs={self.use_trxs}, "
            f"docs_per_trx={self.docs_per_trx}, "
            f"maintain_order={self.maintain_order}, "
            f"update_existing={self.update_existing}, "
            f"fail_on_duplicate_key={self.fail_on_duplicate_key}, "
            f"log_intermediate_files={self.log_intermediate_files})"
        )
