#
#  Copyright 2026. Tabs Data Inc.
#
"""MongoDB metadata extractor.

Populates the MongoDB-specific model (collection type, indexes, and
optional validator) rather than inferring a property schema by
sampling — MongoDB doesn't provide an authoritative field schema.
"""

import fnmatch
import sys
from typing import ClassVar

from pymongo import MongoClient

from tabsdata._cli.tdconn._internal.extractor.api import (
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
)
from tabsdata._cli.tdconn._internal.extractor.connector.mongodb.model import (
    MongoDBConnMetaModel,
    MongoDBEntity,
    MongoIndex,
    MongoValidator,
)
from tabsdata.connector.mongodb._connection import MongoDBConn


class MongoDBConnectionExtractor(ConnectionExtractor):
    """Extract collection metadata from MongoDB.

    The ``info`` dict matches ``MongoDBConn``'s JSON shape:
        uri: full MongoDB connection string
        credentials: {user, password}     (optional — leave off if the
            URI already carries credentials)
        connection_options: optional pymongo kwargs

    Extractor-level options:
        database: database to scan (required).
        collections: optional glob patterns restricting which collections
            are listed (e.g. ``["users", "orders_*"]``). Defaults to
            every non-system collection.
    """

    meta_model: ClassVar[ConnMetaModel] = MongoDBConnMetaModel()
    entity_type: ClassVar[type[Entity]] = MongoDBEntity

    def check_connect(self, info: dict) -> None:
        client = _client(info)
        try:
            client.admin.command("ping")
        finally:
            client.close()

    def metadata(self, info: dict) -> list[MongoDBEntity]:
        db_name = info.get("database", "")
        patterns = info.get("collections") or ["*"]
        client = _client(info)
        try:
            db = client[db_name]
            entities: list[MongoDBEntity] = []
            for coll_info in db.list_collections():
                name = coll_info.get("name", "")
                if not name or name.startswith("system."):
                    continue
                if not any(fnmatch.fnmatch(name, p) for p in patterns):
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

                # Views don't own indexes — they delegate to their source.
                indexes: list[MongoIndex] = []
                if coll_type != "view":
                    indexes = _list_indexes(db, name)

                entities.append(
                    MongoDBEntity(
                        connection_type=self.meta_model,
                        database=db_name,
                        collection=name,
                        collection_type=coll_type,
                        indexes=indexes,
                        validator=validator,
                    )
                )
            return entities
        finally:
            client.close()

    def data(self, info: dict, entity: Entity, limit: int = 100) -> list[dict]:
        mongo_entity: MongoDBEntity = entity  # type: ignore[assignment]
        client = _client(info)
        try:
            db = client[info.get("database", "")]
            cursor = db[mongo_entity.collection].find({}, limit=limit)
            rows: list[dict] = []
            for doc in cursor:
                # _id is an ObjectId — stringify so callers get a JSON-safe dict.
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                rows.append(doc)
            return rows
        finally:
            client.close()


def _client(info: dict) -> MongoClient:
    conn = MongoDBConn._load_from_dict({
        k: v for k, v in info.items() if k not in ("database", "collections")
    })
    kwargs: dict = dict(conn.connection_options or {})
    if conn.credentials is not None:
        kwargs["username"] = conn.credentials.user.secret_value or None
        kwargs["password"] = conn.credentials.password.secret_value or None
    return MongoClient(conn.uri, **kwargs)


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
