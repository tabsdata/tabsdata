#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Literal, TypeAlias

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import Action, SchemaChange, SchemaDelta
from tabsdatak.conn.common.sql.error import SqlCommonErrorCode

SchemaEvolution: TypeAlias = Literal["strict", "update"]

STRICT: SchemaEvolution = "strict"
UPDATE: SchemaEvolution = "update"

GOVERNED = (Action.ADD_COLUMN, Action.ALTER_TYPE, Action.SET_NULLABLE)


def governed(delta: SchemaDelta, evolution: SchemaEvolution) -> SchemaDelta:
    if delta.is_create:
        return delta
    changes = [
        change
        for change in delta.changes
        if change.action in GOVERNED and not change.detail.get("fits")
    ]
    refused = [change for change in changes if _refused(change, evolution)]
    if refused:
        raise SqlCommonErrorCode.SQL_COMMON_10.exception(
            table=delta.table_name,
            evolution=evolution,
            changes="; ".join(_described(change) for change in refused),
        )
    return SchemaDelta(table_name=delta.table_name, is_create=False, changes=changes)


def _refused(change: SchemaChange, evolution: SchemaEvolution) -> bool:
    if evolution == STRICT:
        return True
    return change.action is Action.ALTER_TYPE and not change.safe


def _described(change: SchemaChange) -> str:
    if change.action is Action.ADD_COLUMN:
        return f"new column {change.column!r}"
    if change.action is Action.SET_NULLABLE:
        return f"column {change.column!r} to nullable"
    old = change.detail.get("old_type")
    new = change.detail.get("new_type")
    return f"column {change.column!r} from {old} to {new}"
