#
#  Copyright 2026. Tabs Data Inc.
#
"""MongoDB metadata explorer.

Populates the MongoDB-specific model (collection type, indexes, and
optional validator) rather than inferring a property schema by
sampling -- MongoDB doesn't provide an authoritative field schema.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar

from tabsdatak.conn.common._explorer import BaseConnMetaModel

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.mongodb import MongoDBDestConn
from tabsdatak.conn.mongodb._explorer._model import (
    MongoDBConnMetaModel,
    MongoDBEntity,
    MongoIndex,
    MongoValidator,
)
from tabsdatak.spi import Entity, Explorer

if TYPE_CHECKING:
    from pymongo import MongoClient


class MongoDBConnectionExplorer(Explorer):
    """Explore collection metadata from MongoDB.

    Lists the non-system collections in the connection's default
    database and records each one's type (collection / view /
    timeseries), its indexes, and its validator when one is attached.
    """

    meta_model: ClassVar[BaseConnMetaModel] = MongoDBConnMetaModel()
    entity_type: ClassVar[type[Entity]] = MongoDBEntity

    def check_connect(self, conn: MongoDBDestConn) -> None:
        client = _client(conn)
        try:
            client.admin.command("ping")
        finally:
            client.close()

    def metadata(self, conn: MongoDBDestConn) -> list[MongoDBEntity]:
        client = _client(conn)
        try:
            db = client.get_database()
            entities: list[MongoDBEntity] = []
            for coll_info in db.list_collections():
                name = coll_info.get("name", "")
                if not name or name.startswith("system."):
                    continue

                coll_type = coll_info.get("type", "collection")
                options = coll_info.get("options") or {}

                validator: MongoValidator | None = None
                if options.get("validator"):
                    validator = MongoValidator(
                        rule=options["validator"],
                        validation_level=options.get("validationLevel", ""),
                        validation_action=options.get("validationAction", ""),
                    )

                # Views don't own indexes -- they delegate to their source.
                indexes: list[MongoIndex] = []
                if coll_type != "view":
                    indexes = _list_indexes(db, name)

                entities.append(
                    MongoDBEntity(
                        connection_type=self.meta_model,
                        database=db.name,
                        collection=name,
                        collection_type=coll_type,
                        indexes=indexes,
                        validator=validator,
                    )
                )
            return entities
        finally:
            client.close()

    def data(
        self, conn: MongoDBDestConn, entity: MongoDBEntity, limit: int = 100
    ) -> list[dict]:
        client = _client(conn)
        try:
            db = client.get_database()
            rows: list[dict] = []
            for doc in db[entity.collection].find({}, limit=limit):
                # ``_id`` is an ObjectId -- stringify so callers get a
                # JSON-safe dict.
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                rows.append(doc)
            return rows
        finally:
            client.close()


def _client(conn: MongoDBDestConn) -> MongoClient:
    # pymongo is imported lazily so merely importing this module (e.g. a
    # registry scan that pulls in ``_plugin``) doesn't pay for the driver.
    from pymongo import MongoClient

    kwargs: dict = dict(conn.conn_cfg or {})
    if conn.credentials is not None:
        kwargs["username"] = _convert(conn.credentials.user, str) or None
        kwargs["password"] = _convert(conn.credentials.password, str) or None
    return MongoClient(_convert(conn.uri, str), **kwargs)


def _list_indexes(db, coll_name: str) -> list[MongoIndex]:
    try:
        raw_indexes = list(db[coll_name].list_indexes())
    except Exception as exc:
        print(
            f"  Could not list indexes for {coll_name}: {exc}",
            file=sys.stderr,
        )
        return []

    indexes: list[MongoIndex] = []
    for idx in raw_indexes:
        key = idx.get("key") or {}
        keys: list[tuple[str, int | str]] = []
        for field_name, direction in key.items():
            if isinstance(direction, (int, float)):
                keys.append((field_name, int(direction)))
            else:
                keys.append((field_name, direction))
        indexes.append(
            MongoIndex(
                name=idx.get("name", ""),
                keys=keys,
                unique=bool(idx.get("unique", False)),
            )
        )
    return indexes
