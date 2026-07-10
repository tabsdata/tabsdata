from abc import abstractmethod
from dataclasses import field

import tabsdatak._api as _api
from tabsdatak.error import ApiErrorCode
from tabsdatak.spi import ConnMetaModel
from tabsdatak.conn.common.error import ConnErrorCode


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
