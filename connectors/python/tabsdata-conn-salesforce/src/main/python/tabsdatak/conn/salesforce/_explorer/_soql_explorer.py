#
# Copyright 2026 Tabsdata Inc.
#

#
# Copyright (c) 2026 TabsData, Inc.
#

"""Salesforce SOQL (objects) metadata explorer."""

import sys
from typing import ClassVar

from tabsdatak.conn.common._explorer import (
    BaseConnMetaModel,
    ObjectsConnMetaModel,
    ObjectsEntity,
    Property,
)
from tabsdatak.conn.salesforce import SalesforceSrcConn
from tabsdatak.conn.salesforce._explorer._common import (
    check_connect as _sf_check_connect,
    connect,
)
from tabsdatak.spi import Entity, Explorer


class SalesforceSOQLConnectionExplorer(Explorer):
    """Explore object (sobject) metadata from Salesforce via ``describe``.

    Reads the target sobject names from ``conn.objects`` (the property
    already collapses a ``category`` shortcut to its canonical list) and
    calls ``sf.<Name>.describe()`` per entry. It deliberately avoids the
    global ``sf.describe()``, so a 500-1000 sobject org doesn't stall a
    refresh. Unknown names are logged to stderr and skipped; an absent
    or empty list yields no metadata.
    """

    meta_model: ClassVar[BaseConnMetaModel] = ObjectsConnMetaModel()
    entity_type: ClassVar[type[Entity]] = ObjectsEntity

    def check_connect(self, conn: SalesforceSrcConn) -> None:
        _sf_check_connect(conn)

    def metadata(self, conn: SalesforceSrcConn) -> list[ObjectsEntity]:
        names = _to_list(conn.objects)
        if not names:
            # Nothing requested -- don't even open a session. The
            # connection can still be verified via ``check_connect``;
            # metadata extraction is opt-in.
            return []

        sf = connect(conn)
        entities: list[ObjectsEntity] = []
        for obj_name in names:
            try:
                obj_desc = getattr(sf, obj_name).describe()
            except Exception as exc:
                # Unknown / inaccessible sobject -- surface it so a typo
                # in ``objects`` doesn't silently yield zero entities,
                # then keep going on the rest.
                print(
                    f"  Ignoring object {obj_name}: cannot describe ({exc})",
                    file=sys.stderr,
                )
                continue

            properties = [
                Property(
                    name=field["name"],
                    type=field.get("type", "string"),
                    required=not field.get("nillable", True),
                    description=field.get("label", ""),
                )
                for field in obj_desc.get("fields", [])
            ]

            entities.append(
                ObjectsEntity(
                    connection_type=self.meta_model,
                    namespace="",
                    object=obj_name,
                    properties=properties,
                )
            )

        return entities

    def data(
        self, conn: SalesforceSrcConn, entity: ObjectsEntity, limit: int = 100
    ) -> list[dict]:
        field_names = [p.name for p in entity.properties]
        fields_csv = ", ".join(field_names) if field_names else "Id"
        query = f"SELECT {fields_csv} FROM {entity.object} LIMIT {limit}"

        result = connect(conn).query(query)
        return [
            {f: record.get(f) for f in field_names}
            for record in result.get("records", [])
        ]


def _to_list(objects) -> list[str]:
    """Normalize ``SalesforceSrcConn.objects`` to a flat list of names.

    The attribute is ``str | list[str] | None``; a ``category`` shortcut
    has already collapsed to ``list[str]`` by the time we see it. A bare
    string is treated as a single sobject name -- no comma/space
    splitting, since a multi-name string is the caller's mistake and
    silently matching a synthetic concatenated name is worse than a
    clear 404.
    """
    if objects is None:
        return []
    if isinstance(objects, str):
        return [objects]
    return list(objects)
