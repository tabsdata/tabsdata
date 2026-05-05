#
# Copyright (c) 2026 Tabsdata Inc.
#

"""Salesforce SOQL (objects) metadata extractor."""

import sys
from typing import ClassVar

from tabsdata._cli.tdconn._internal.extractor.api import (
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    ObjectsConnMetaModel,
    ObjectsEntity,
    Property,
)
from tabsdata._cli.tdconn._internal.extractor.connector.salesforce.common import (
    check_connect as _sf_check_connect,
    connect,
    load_conn,
)


class SalesforceSOQLConnectionExtractor(ConnectionExtractor):
    """Extract object metadata from Salesforce via SOQL/describe.

    The ``info`` dict matches ``SalesforceConn``'s JSON shape:
        credentials: {username, password, security_token}
        instance_url: optional Salesforce instance URL
        category or objects: describe which sobjects to inspect.
            ``category`` picks a curated bundle (e.g. ``"sales_crm"``
            → Account, Contact, Lead, …); ``objects`` is an ad-hoc
            name or list of names. Exactly one side may be set per
            :class:`SalesforceConn`'s constructor contract.

    The extractor reads the effective list from ``conn.objects`` —
    the property already collapses ``category`` to its canonical
    list — and calls ``sf.<Name>.describe()`` per entry. It does NOT
    invoke the global ``sf.describe()``, so a typical 500–1000
    sobject org doesn't stall a refresh. Unknown names log to stderr
    and are skipped; absent / empty list ⇒ no metadata extracted.
    """

    meta_model: ClassVar[ConnMetaModel] = ObjectsConnMetaModel()
    entity_type: ClassVar[type[Entity]] = ObjectsEntity

    def check_connect(self, info: dict) -> None:
        _sf_check_connect(info)

    def metadata(self, info: dict) -> list[ObjectsEntity]:
        conn = load_conn(info)
        names = _to_list(conn.objects)
        if not names:
            # Nothing asked for — don't even open a session. The
            # connection itself can still be verified via
            # ``check_connect``; metadata extraction is opt-in.
            return []

        sf = connect(info)
        entities = []
        for obj_name in names:
            try:
                obj_desc = getattr(sf, obj_name).describe()
            except Exception as exc:
                # Unknown / inaccessible sobject — surface it so a
                # typo in ``objects`` doesn't silently yield zero
                # entities, then keep going on the rest.
                print(
                    f"  Ignoring object: {obj_name} reason: cannot describe ({exc})",
                    file=sys.stderr,
                )
                continue

            properties = []
            for field in obj_desc.get("fields", []):
                properties.append(
                    Property(
                        name=field["name"],
                        type=field.get("type", "string"),
                        required=not field.get("nillable", True),
                        description=field.get("label", ""),
                    )
                )

            entities.append(
                ObjectsEntity(
                    connection_type=self.meta_model,
                    namespace="",
                    object=obj_name,
                    properties=properties,
                )
            )

        return entities

    def data(self, info: dict, entity: Entity, limit: int = 100) -> list[dict]:
        sf = connect(info)
        obj_entity = entity  # type: ObjectsEntity
        obj_name = obj_entity.object
        field_names = [p.name for p in obj_entity.properties]

        # Build SOQL query with all fields
        fields_csv = ", ".join(field_names) if field_names else "Id"
        query = f"SELECT {fields_csv} FROM {obj_name} LIMIT {limit}"

        result = sf.query(query)
        rows = []
        for record in result.get("records", []):
            row = {}
            for f in field_names:
                val = record.get(f)
                row[f] = val
            rows.append(row)
        return rows


def _to_list(objects) -> list[str]:
    """Normalize :attr:`SalesforceConn.objects` to a flat list of names.

    The attribute is typed ``str | list[str] | None``; the category
    shortcut collapses to ``list[str]`` before we see it. A bare
    string is treated as a single sobject name — no comma/space
    splitting, because a multi-name string is the caller's mistake
    and silently doing the wrong thing (matching a synthetic
    concatenated name) is worse than a clear 404.
    """
    if objects is None:
        return []
    if isinstance(objects, str):
        return [objects]
    return list(objects)
