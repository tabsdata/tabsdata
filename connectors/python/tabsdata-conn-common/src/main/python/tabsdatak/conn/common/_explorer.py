from abc import abstractmethod
from dataclasses import field

import tabsdatak._api as _api
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.spi import ConnMetaModel, Entity


class BaseConnMetaModel(ConnMetaModel):
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


@_api.dataclass(ConnErrorCode.CONN_11)
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


@_api.dataclass(ConnErrorCode.CONN_16)
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


@_api.dataclass(ConnErrorCode.CONN_12)
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


@_api.dataclass(ConnErrorCode.CONN_13)
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


@_api.dataclass(ConnErrorCode.CONN_14)
class ReportingConnMetaModel(BaseConnMetaModel):
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


@_api.dataclass(ConnErrorCode.CONN_15)
class ObjectsConnMetaModel(BaseConnMetaModel):
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
