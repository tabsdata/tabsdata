#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.common._catalog import canonical_entity, schema_fingerprint


class _Entity:
    def __init__(self, doc: dict):
        self.doc = doc

    def to_dict(self) -> dict:
        return dict(self.doc)


def _entities() -> list[_Entity]:
    return [
        _Entity({
            "table": "orders",
            "schema": "public",
            "fqn": "crm.public.orders",
            "columns": [
                {"name": "total", "type": "numeric"},
                {"name": "id", "type": "bigint"},
            ],
        }),
        _Entity({
            "table": "accounts",
            "fqn": "crm.public.accounts",
            "columns": [
                {"name": "name", "type": "text"},
                {"name": "id", "type": "bigint"},
            ],
        }),
        _Entity({
            "fqn": "file:///data/a.csv",
            "file": {"pattern": "a.csv", "format": "csv"},
            "size": 12,
        }),
    ]


def test_the_fingerprint_is_stable():
    assert (
        schema_fingerprint(_entities())
        == "c5e10cc9dc1ee926fe019ae6d8174c501fc3b3ac4b1a6c6f67d4ac7549234296"
    )


def test_the_fingerprint_ignores_a_file_size_and_modification_time():
    entities = _entities()
    rewritten = [
        _Entity({**e.doc, "size": 99, "last_modified": "2026-09-30T00:00:00+00:00"})
        if "file" in e.doc
        else e
        for e in entities
    ]
    assert schema_fingerprint(rewritten) == schema_fingerprint(entities)
    added = [*entities[:2], _Entity({**entities[2].doc, "columns": [{"name": "id", "type": "int"}]})]
    assert schema_fingerprint(added) != schema_fingerprint(entities)


def test_the_fingerprint_ignores_entity_and_column_order():
    entities = _entities()
    reordered = [
        _Entity({**e.doc, "columns": list(reversed(e.doc["columns"]))})
        if "columns" in e.doc
        else e
        for e in reversed(entities)
    ]
    assert schema_fingerprint(reordered) == schema_fingerprint(entities)


def test_a_structural_change_changes_the_fingerprint():
    entities = _entities()
    entities[0].doc["columns"].append({"name": "currency", "type": "text"})
    assert schema_fingerprint(entities) != schema_fingerprint(_entities())


def test_the_canonical_entity_sorts_columns_and_leaves_the_entity_untouched():
    entity = _entities()[0]
    canonical = canonical_entity(entity)
    assert [c["name"] for c in canonical["columns"]] == ["id", "total"]
    assert [c["name"] for c in entity.doc["columns"]] == ["total", "id"]


def test_an_empty_catalog_has_a_stable_fingerprint():
    assert schema_fingerprint([]) == schema_fingerprint([])
