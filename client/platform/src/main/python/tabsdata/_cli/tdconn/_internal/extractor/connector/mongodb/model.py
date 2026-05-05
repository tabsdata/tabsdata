#
#  Copyright 2026. Tabs Data Inc.
#
"""MongoDB-specific meta-model and entity.

MongoDB doesn't expose an authoritative field schema — collections are
schemaless by default — but it does carry stable metadata at the
*collection* level: the collection type (regular / view / timeseries),
the list of indexes, and an optional validator (commonly a
``$jsonSchema`` document). Those are what this model captures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Union

from tabsdata._cli.tdconn._internal.extractor.api import ConnMetaModel, Entity


@dataclass
class MongoIndex:
    """An index defined on a MongoDB collection.

    ``keys`` is the ordered list of ``(field, direction)`` pairs. Direction
    is an ``int`` for B-tree indexes (``1`` ascending, ``-1`` descending)
    and a string for specialized indexes (e.g. ``"text"``, ``"2dsphere"``).
    """

    name: str
    keys: list[tuple[str, Union[int, str]]] = field(default_factory=list)
    unique: bool = False

    def to_dict(self) -> dict:
        d: dict = {
            "name": self.name,
            "keys": [[k, v] for k, v in self.keys],
        }
        if self.unique:
            d["unique"] = True
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "MongoIndex":
        return cls(
            name=data.get("name", ""),
            keys=[(k, v) for k, v in data.get("keys", [])],
            unique=bool(data.get("unique", False)),
        )


@dataclass
class MongoValidator:
    """A validator attached to a MongoDB collection.

    ``rule`` is the raw validator document, stored verbatim so that both
    ``$jsonSchema`` and query-style validators round-trip unchanged.
    """

    rule: dict = field(default_factory=dict)
    validation_level: str = ""
    validation_action: str = ""

    def to_dict(self) -> dict:
        d: dict = {"rule": self.rule}
        if self.validation_level:
            d["validation_level"] = self.validation_level
        if self.validation_action:
            d["validation_action"] = self.validation_action
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "MongoValidator":
        return cls(
            rule=data.get("rule", {}),
            validation_level=data.get("validation_level", ""),
            validation_action=data.get("validation_action", ""),
        )


@dataclass
class MongoDBEntity(Entity):
    """A MongoDB collection, view, or timeseries collection."""

    database: str = ""
    collection: str = ""
    collection_type: str = "collection"
    indexes: list[MongoIndex] = field(default_factory=list)
    validator: Optional[MongoValidator] = None

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "database": self.database,
            "collection": self.collection,
        })

    def field_names(self) -> list[str]:
        """Return field names derived from indexed fields.

        Without a validator, indexed fields are the most reliable field
        list — a validator's ``$jsonSchema`` document (available on
        :attr:`validator`) is the authoritative source when present.
        """
        seen: set[str] = set()
        for idx in self.indexes:
            for fname, _direction in idx.keys:
                seen.add(fname)
        return sorted(seen)

    def to_dict(self) -> dict:
        d: dict = {}
        if self.database:
            d["database"] = self.database
        d["collection"] = self.collection
        d["collection_type"] = self.collection_type
        d["fqn"] = self.fqn()
        if self.indexes:
            d["indexes"] = [i.to_dict() for i in self.indexes]
        if self.validator is not None:
            d["validator"] = self.validator.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: dict, connection_type) -> "MongoDBEntity":
        validator_data = data.get("validator")
        return cls(
            connection_type=connection_type,
            database=data.get("database", ""),
            collection=data.get("collection", ""),
            collection_type=data.get("collection_type", "collection"),
            indexes=[MongoIndex.from_dict(i) for i in data.get("indexes", [])],
            validator=MongoValidator.from_dict(validator_data)
            if validator_data
            else None,
        )


@dataclass
class MongoDBConnMetaModel(ConnMetaModel):
    """MongoDB: database → collection, with indexes and optional validators."""

    type: str = "mongodb"
    description: str = (
        "MongoDB databases with collections, collection types "
        "(collection / view / timeseries), indexes, and optional validators"
    )
    hierarchy: list[str] = field(default_factory=lambda: ["database", "collection"])
    entity_level: str = "collection"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["database", "collection"],
    )

    def entity_fqn(self, entity: dict) -> str:
        return ".".join(entity.get(k, "") for k in self.entity_fully_qualified_name)
