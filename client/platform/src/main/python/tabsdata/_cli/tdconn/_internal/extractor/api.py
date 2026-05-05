#
# Copyright (c) 2026 Tabsdata Inc.
#
"""Connector extraction API.

This module defines the shared vocabulary used by every per-system
extractor under :mod:`tabsdata._cli.tdconn._internal.extractor.connector`:

* A **meta-model** (:class:`ConnMetaModel`) describing *what kind* of
  system a connection targets — SQL, objects, reporting, file storage,
  or tabsdata — and how its entities are named.
* **Entities** (:class:`Entity` and subclasses) — the individual units a
  connector can enumerate and sample (a SQL table, a CRM object, a BI
  report, a file pattern, a tabsdata table).
* **Extractors** (:class:`ConnectionExtractor`) — the code that, given a
  connection's ``conn_configs``, can verify connectivity, list
  entities, and fetch sample rows.

Values inside ``conn_configs`` may be given as ``"env:VAR_NAME"`` keys
to pull secrets from environment variables at use time;
:func:`resolve_info` performs that resolution.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar

# ---------------------------------------------------------------------------
# Entity definitions — model the schema of entities per connection type
# ---------------------------------------------------------------------------


@dataclass
class Column:
    """A column/field in a SQL, reporting, or tabsdata entity."""

    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    description: str = ""

    def to_dict(self) -> dict:
        """Return a compact dict representation, omitting falsy extras."""
        d = {"name": self.name, "type": self.type}
        if self.primary_key:
            d["primary_key"] = True
        if self.description:
            d["description"] = self.description
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "Column":
        """Rebuild a :class:`Column` from its :meth:`to_dict` output."""
        return cls(
            name=data.get("name", ""),
            type=data.get("type", ""),
            nullable=data.get("nullable", True),
            primary_key=bool(data.get("primary_key", False)),
            description=data.get("description", ""),
        )


@dataclass
class Property:
    """A property in an object-type entity (CRM, ERP, SaaS)."""

    name: str
    type: str
    required: bool = False
    description: str = ""

    def to_dict(self) -> dict:
        """Return a compact dict representation, omitting falsy extras."""
        d = {"name": self.name, "type": self.type}
        if self.required:
            d["required"] = True
        if self.description:
            d["description"] = self.description
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "Property":
        """Rebuild a :class:`Property` from its :meth:`to_dict` output."""
        return cls(
            name=data.get("name", ""),
            type=data.get("type", ""),
            required=bool(data.get("required", False)),
            description=data.get("description", ""),
        )


@dataclass
class FilePattern:
    """A filename glob inside a file-storage entity."""

    pattern: str
    format: str

    def to_dict(self) -> dict:
        """Return a compact dict representation."""
        return {"pattern": self.pattern, "format": self.format}

    @classmethod
    def from_dict(cls, data: dict) -> "FilePattern":
        """Rebuild a :class:`FilePattern` from its :meth:`to_dict` output."""
        return cls(pattern=data.get("pattern", ""), format=data.get("format", ""))


@dataclass
class Entity(ABC):
    """Abstract base for an entity within a connection.

    Every concrete entity carries a back-reference to the
    :class:`ConnMetaModel` that produced it so that :meth:`fqn` can
    format the entity's fully qualified name using the hierarchy
    declared by the meta-model.
    """

    connection_type: "ConnMetaModel"

    @abstractmethod
    def fqn(self) -> str:
        """Return the fully qualified name of this entity.

        The format is dictated by the meta-model's
        ``entity_fully_qualified_name`` hierarchy (e.g. SQL uses
        ``"database.schema.table"``, storage uses ``"prefix/path"``).
        """

    @abstractmethod
    def field_names(self) -> list[str]:
        """Return the names of all fields/columns/properties in this entity."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize the entity to a dict, omitting falsy values."""

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict, connection_type: "ConnMetaModel") -> "Entity":
        """Rebuild an entity from :meth:`to_dict` output.

        The caller supplies *connection_type* explicitly because it's
        not serialized inside the dict — the meta-model instance is a
        runtime reference, not a persisted payload.
        """


@dataclass
class SQLEntity(Entity):
    """A table or view in a SQL database."""

    database: str = ""
    schema: str = ""
    table: str = ""
    columns: list[Column] = field(default_factory=list)

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "database": self.database,
            "schema": self.schema,
            "owner": self.schema,
            "catalog": self.database,
            "table": self.table,
        })

    def field_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict:
        d = {}
        if self.database:
            d["database"] = self.database
        if self.schema:
            d["schema"] = self.schema
        d["table"] = self.table
        d["fqn"] = self.fqn()
        d["columns"] = [c.to_dict() for c in self.columns]
        return d

    @classmethod
    def from_dict(cls, data: dict, connection_type: "ConnMetaModel") -> "SQLEntity":
        return cls(
            connection_type=connection_type,
            database=data.get("database", ""),
            schema=data.get("schema", ""),
            table=data.get("table", ""),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
        )


@dataclass
class ObjectsEntity(Entity):
    """An object class (type) in a CRM/ERP/SaaS system."""

    namespace: str = ""
    object: str = ""
    properties: list[Property] = field(default_factory=list)

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "namespace": self.namespace,
            "object": self.object,
        })

    def field_names(self) -> list[str]:
        return [p.name for p in self.properties]

    def to_dict(self) -> dict:
        d = {}
        if self.namespace:
            d["namespace"] = self.namespace
        d["object"] = self.object
        d["fqn"] = self.fqn()
        d["properties"] = [p.to_dict() for p in self.properties]
        return d

    @classmethod
    def from_dict(cls, data: dict, connection_type: "ConnMetaModel") -> "ObjectsEntity":
        return cls(
            connection_type=connection_type,
            namespace=data.get("namespace", ""),
            object=data.get("object", ""),
            properties=[Property.from_dict(p) for p in data.get("properties", [])],
        )


@dataclass
class ReportingEntity(Entity):
    """A saved report in a BI/reporting system."""

    workspace: str = ""
    report: str = ""
    columns: list[Column] = field(default_factory=list)

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "workspace": self.workspace,
            "folder": self.workspace,
            "report": self.report,
        })

    def field_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict:
        d = {}
        if self.workspace:
            d["workspace"] = self.workspace
        d["report"] = self.report
        d["fqn"] = self.fqn()
        d["columns"] = [c.to_dict() for c in self.columns]
        return d

    @classmethod
    def from_dict(
        cls, data: dict, connection_type: "ConnMetaModel"
    ) -> "ReportingEntity":
        return cls(
            connection_type=connection_type,
            workspace=data.get("workspace", ""),
            report=data.get("report", ""),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
        )


@dataclass
class FileStorageEntity(Entity):
    """A prefix / path inside a file- or object-storage system."""

    bucket: str = ""
    prefix: str = ""
    files: list[FilePattern] = field(default_factory=list)

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "bucket": self.bucket,
            "container": self.bucket,
            "prefix": self.prefix,
            "path": self.prefix,
        })

    def field_names(self) -> list[str]:
        return [f.pattern for f in self.files]

    def to_dict(self) -> dict:
        d = {}
        if self.bucket:
            d["bucket"] = self.bucket
        if self.prefix:
            d["prefix"] = self.prefix
        d["fqn"] = self.fqn()
        d["files"] = [f.to_dict() for f in self.files]
        return d

    @classmethod
    def from_dict(
        cls, data: dict, connection_type: "ConnMetaModel"
    ) -> "FileStorageEntity":
        return cls(
            connection_type=connection_type,
            bucket=data.get("bucket", ""),
            prefix=data.get("prefix", ""),
            files=[FilePattern.from_dict(f) for f in data.get("files", [])],
        )


@dataclass
class TabsdataEntity(Entity):
    """A table inside a Tabsdata collection."""

    collection: str = ""
    table: str = ""
    columns: list[Column] = field(default_factory=list)

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "collection": self.collection,
            "table": self.table,
        })

    def field_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict:
        d = {}
        if self.collection:
            d["collection"] = self.collection
        d["table"] = self.table
        d["fqn"] = self.fqn()
        d["columns"] = [c.to_dict() for c in self.columns]
        return d

    @classmethod
    def from_dict(
        cls, data: dict, connection_type: "ConnMetaModel"
    ) -> "TabsdataEntity":
        return cls(
            connection_type=connection_type,
            collection=data.get("collection", ""),
            table=data.get("table", ""),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
        )


# ---------------------------------------------------------------------------
# Meta-models — one per family of systems
# ---------------------------------------------------------------------------


@dataclass
class ConnMetaModel(ABC):
    """Abstract base for a connection *kind* (SQL, objects, storage, …).

    Describes the hierarchy of containers (database → schema → table,
    bucket → prefix, etc.), the level at which entities live, and the
    components that make up an entity's fully qualified name.
    """

    kind: str = "connectionType"
    apiVersion: str = "1.0"
    name: str = ""
    type: str = ""
    description: str = ""
    hierarchy: list[str] = field(default_factory=list)
    entity_level: str = ""
    entity_fully_qualified_name: list[str] = field(default_factory=list)

    @abstractmethod
    def entity_fqn(self, entity: dict) -> str:
        """Format the fully qualified name of an entity.

        The input is a flat dict of the parts that make up an entity's
        coordinates (e.g. ``{"database": "...", "schema": "...",
        "table": "..."}``). Implementations pick the parts that apply
        and join them with the separator appropriate for the system.
        """


@dataclass
class SQLConnMetaModel(ConnMetaModel):
    """SQL databases: database → schema → table → column."""

    type: str = "common_sql"
    description: str = (
        "Relational database systems with databases, schemas, tables, and columns"
    )
    hierarchy: list[str] = field(
        default_factory=lambda: ["database", "schema", "table", "column"]
    )
    entity_level: str = "table"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["database", "schema", "table"]
    )

    def entity_fqn(self, entity: dict) -> str:
        return ".".join(entity.get(k, "") for k in self.entity_fully_qualified_name)


@dataclass
class ObjectsConnMetaModel(ConnMetaModel):
    """CRM/ERP/SaaS: namespace → object (class) with properties."""

    type: str = "objects"
    description: str = (
        "Object-oriented systems (CRM, ERP, SaaS) with object classes and properties"
    )
    hierarchy: list[str] = field(default_factory=lambda: ["namespace", "object"])
    entity_level: str = "object"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["namespace", "object"]
    )

    def entity_fqn(self, entity: dict) -> str:
        return ".".join(entity.get(k, "") for k in self.entity_fully_qualified_name)


@dataclass
class ReportingConnMetaModel(ConnMetaModel):
    """BI/reporting: workspace → report with columnar schemas."""

    type: str = "reporting"
    description: str = (
        "Reporting and BI systems with reports, dashboards, and columnar schemas"
    )
    hierarchy: list[str] = field(default_factory=lambda: ["workspace", "report"])
    entity_level: str = "report"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["workspace", "report"]
    )

    def entity_fqn(self, entity: dict) -> str:
        return ".".join(entity.get(k, "") for k in self.entity_fully_qualified_name)


@dataclass
class FileStorageConnMetaModel(ConnMetaModel):
    """File/object storage: bucket → prefix → file, joined by ``/``."""

    type: str = "filestorage"
    description: str = (
        "File and object storage systems with"
        " directory structures, files, and file types"
    )
    hierarchy: list[str] = field(default_factory=lambda: ["bucket", "prefix", "file"])
    entity_level: str = "prefix"
    entity_fully_qualified_name: list[str] = field(default_factory=lambda: ["prefix"])

    def entity_fqn(self, entity: dict) -> str:
        parts = [entity.get(k, "") for k in self.entity_fully_qualified_name]
        return "/".join(p for p in parts if p)


@dataclass
class TabsdataConnMetaModel(ConnMetaModel):
    """Tabsdata-managed collections: collection → table."""

    type: str = "tabsdata"
    description: str = (
        "Tabsdata collections with tables and columns, managed by the platform"
    )
    hierarchy: list[str] = field(default_factory=lambda: ["collection", "table"])
    entity_level: str = "table"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["collection", "table"]
    )

    def entity_fqn(self, entity: dict) -> str:
        return ".".join(entity.get(k, "") for k in self.entity_fully_qualified_name)


# ---------------------------------------------------------------------------
# Info helpers
# ---------------------------------------------------------------------------


def connection_info(conn_data: dict) -> dict:
    """Return the connection's info dict, accepting either storage shape.

    The legacy shape exposes the details under a top-level ``info`` key;
    the newer shape stores them under ``conn_configs``. This helper hides
    the difference from callers. Raises :class:`ValueError` if neither
    key is present.
    """
    if "info" in conn_data:
        return conn_data["info"]
    if "conn_configs" in conn_data:
        return conn_data["conn_configs"]
    raise ValueError("connection has neither 'info' nor 'conn_configs'")


def resolve_info(info: dict) -> dict:
    """Resolve ``env:VAR_NAME`` references inside a connection info dict.

    Keys prefixed with ``"env:"`` have their values read from the named
    environment variable; the prefix is stripped from the key in the
    resolved output. Numeric env values are coerced to ``int`` so that
    ports and similar fields arrive with the right type.

    Example::

        {"host": "localhost", "env:password": "DB_PASSWORD"}
        -> {"host": "localhost", "password": "s3cret"}

    Raises :class:`ValueError` if a referenced env var is unset. Returns
    a new dict — the input is never mutated.
    """
    resolved = {}
    for key, value in info.items():
        if key.startswith("env:"):
            prop_name = key[4:]
            env_name = value
            env_value = os.environ.get(env_name)
            if env_value is None:
                raise ValueError(f"Environment variable {env_name} is not set")
            if env_value.isdigit():
                resolved[prop_name] = int(env_value)
            else:
                resolved[prop_name] = env_value
        else:
            resolved[key] = value
    return resolved


# ---------------------------------------------------------------------------
# Connection extractor — verifies, enumerates, and samples a connection
# ---------------------------------------------------------------------------


class ConnectionExtractor(ABC):
    """Abstract base for extracting metadata and data from a connection.

    Concrete subclasses must implement :meth:`check_connect` (a
    lightweight connectivity probe) and :meth:`metadata` (entity
    enumeration), and must set the :attr:`meta_model` class attribute
    to the :class:`ConnMetaModel` instance that describes the system
    they target. Leaving :attr:`meta_model` as a class attribute (not
    a constructor parameter) keeps the binding between an extractor
    and its meta-model rigid — each extractor always reports the same
    model, callers never pass one.

    :meth:`data` is optional — the default returns ``None`` to
    signal "this connector does not support row-level previews".
    Storage-like systems (S3, GCS, Azure) inherit the default;
    relational/object extractors override it to issue a
    ``SELECT``-style probe.

    Every public method takes an already-resolved ``info`` dict. The
    ``resolve_and_*`` convenience wrappers call :func:`resolve_info`
    so that callers holding a raw info dict with ``env:`` references
    need not resolve it themselves.
    """

    meta_model: ClassVar[ConnMetaModel]
    entity_type: ClassVar[type[Entity]]

    @abstractmethod
    def check_connect(self, info: dict) -> None:
        """Verify that a connection to the system can be established.

        Should perform the smallest possible round-trip (open a session,
        issue a single-row query, list one resource, …) and raise if the
        attempt fails. Returns ``None`` on success.

        Args:
            info: Connection details with ``env:`` references already
                resolved (see :func:`resolve_info`).
        """

    @abstractmethod
    def metadata(self, info: dict) -> list[Entity]:
        """Return the entity catalog available through this connection.

        Args:
            info: Connection details with ``env:`` references already
                resolved.

        Returns:
            A list of concrete :class:`Entity` instances describing the
            tables / objects / reports / prefixes visible to the caller.
        """

    def data(
        self,
        info: dict,
        entity: Entity,
        limit: int = 100,
    ) -> list[dict] | None:
        """Return up to *limit* sample rows from *entity*.

        The base implementation returns ``None`` to signal that this
        connector does not support a row-level data preview (file /
        object storage inherits this). SQL-like extractors override
        to issue a ``SELECT`` and return a list (possibly empty when
        the entity has no rows — still distinct from ``None``).

        Args:
            info: Connection details with ``env:`` references already
                resolved.
            entity: The entity produced by a previous :meth:`metadata`
                call.
            limit: Maximum number of rows to return.

        Returns:
            A list of row dicts keyed by column/field name, or
            ``None`` when the connector has no data-preview surface.
            ``[]`` means "supported but empty".
        """
        return None

    def resolve_and_check_connect(self, info: dict) -> None:
        """Resolve env: references in *info*, then verify connectivity."""
        self.check_connect(resolve_info(info))

    def resolve_and_metadata(self, info: dict) -> list[Entity]:
        """Resolve env: references in *info*, then list entity metadata."""
        return self.metadata(resolve_info(info))
