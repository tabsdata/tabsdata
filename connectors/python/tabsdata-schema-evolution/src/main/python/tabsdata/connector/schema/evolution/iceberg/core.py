#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata.connector.schema.evolution.iceberg.models import (
    Action,
    SchemaChange,
    SchemaDelta,
    TableSchema,
    TypeWideningMap,
    UnsafeSchemaChangeError,
)


def compute_delta(
    s1: TableSchema | None,
    s2: TableSchema,
    widen_map: TypeWideningMap,
) -> SchemaDelta:
    """Compute the schema delta between s1 (current) and s2 (desired).

    Args:
        s1: Current table schema, or None if the table does not exist.
        s2: Desired table schema.
        widen_map: Map of new_type -> set of current_types for safe widening.

    Returns:
        A SchemaDelta describing the changes needed to evolve s1 into s2.
    """
    delta = SchemaDelta(table_name=s2.table_name)

    # -- CASE 1: Table does not exist --
    if s1 is None or not s1.columns:
        delta.is_create = True
        for i, col in enumerate(s2.columns):
            delta.changes.append(
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column=col.name,
                    safe=True,
                    detail={
                        "type": col.type,
                        "nullable": col.nullable,
                        "default": col.default,
                        "position": i,
                    },
                )
            )
        return delta

    # -- CASE 2: Both schemas have columns --
    delta.is_create = False

    s1_map = {col.name: col for col in s1.columns}
    s2_map = {col.name: col for col in s2.columns}
    s1_names = [col.name for col in s1.columns]
    s2_names = [col.name for col in s2.columns]
    s1_name_set = set(s1_names)
    s2_name_set = set(s2_names)

    # -- 2a: Dropped columns (in S1 but not in S2) --
    for name in s1_names:
        if name not in s2_name_set:
            delta.changes.append(
                SchemaChange(
                    action=Action.DROP_COLUMN,
                    column=name,
                    safe=True,
                    detail={"old_type": s1_map[name].type},
                )
            )

    # -- 2b: Added columns (in S2 but not in S1) --
    for i, name in enumerate(s2_names):
        if name not in s1_name_set:
            col = s2_map[name]
            # Iceberg rule: new columns must be optional or have a default
            safe = col.nullable or col.default is not None
            delta.changes.append(
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column=name,
                    safe=safe,
                    detail={
                        "type": col.type,
                        "nullable": col.nullable,
                        "default": col.default,
                        "position": i,
                    },
                )
            )

    # -- 2c: Modified columns (in both S1 and S2) --
    for name in s1_names:
        if name not in s2_name_set:
            continue
        old = s1_map[name]
        new = s2_map[name]

        # 2c-i: Type change
        if old.type != new.type:
            safe = new.type in widen_map and old.type in widen_map[new.type]
            delta.changes.append(
                SchemaChange(
                    action=Action.ALTER_TYPE,
                    column=name,
                    safe=safe,
                    detail={
                        "old_type": old.type,
                        "new_type": new.type,
                        "nullable": new.nullable,
                    },
                )
            )

        # 2c-ii: Nullability change
        if old.nullable != new.nullable:
            if not old.nullable and new.nullable:
                # required -> optional: SAFE
                delta.changes.append(
                    SchemaChange(
                        action=Action.SET_NULLABLE,
                        column=name,
                        safe=True,
                        detail={"type": new.type},
                    )
                )
            else:
                # optional -> required: UNSAFE
                delta.changes.append(
                    SchemaChange(
                        action=Action.DROP_NULLABLE,
                        column=name,
                        safe=False,
                        detail={"type": new.type},
                    )
                )

        # 2c-iii: Default change
        if old.default != new.default:
            if new.default is None:
                delta.changes.append(
                    SchemaChange(
                        action=Action.DROP_DEFAULT,
                        column=name,
                        safe=True,
                        detail={
                            "type": new.type,
                            "old_default": old.default,
                        },
                    )
                )
            else:
                delta.changes.append(
                    SchemaChange(
                        action=Action.SET_DEFAULT,
                        column=name,
                        safe=True,
                        detail={
                            "type": new.type,
                            "nullable": new.nullable,
                            "new_default": new.default,
                        },
                    )
                )

    # -- 2d: Positional changes --
    common_in_s1_order = [n for n in s1_names if n in s2_name_set]
    common_in_s2_order = [n for n in s2_names if n in s1_name_set]

    if common_in_s1_order != common_in_s2_order:
        for new_pos, name in enumerate(common_in_s2_order):
            old_pos = common_in_s1_order.index(name)
            if old_pos != new_pos:
                after_col = common_in_s2_order[new_pos - 1] if new_pos > 0 else None
                col = s2_map[name]
                delta.changes.append(
                    SchemaChange(
                        action=Action.REORDER_COLUMN,
                        column=name,
                        safe=True,
                        detail={
                            "type": col.type,
                            "nullable": col.nullable,
                            "default": col.default,
                            "old_position": old_pos,
                            "new_position": new_pos,
                            "after_column": after_col,
                        },
                    )
                )

    return delta


def validate_delta(delta: SchemaDelta, strict: bool = False) -> bool:
    """Validate a schema delta.

    Args:
        delta: The SchemaDelta to validate.
        strict: If True, raise UnsafeSchemaChangeError on any unsafe change.

    Returns:
        True if all changes are safe.

    Raises:
        UnsafeSchemaChangeError: If strict=True and any change is unsafe.
    """
    unsafe = [c for c in delta.changes if not c.safe]

    if strict and unsafe:
        raise UnsafeSchemaChangeError(unsafe)

    return len(unsafe) == 0
