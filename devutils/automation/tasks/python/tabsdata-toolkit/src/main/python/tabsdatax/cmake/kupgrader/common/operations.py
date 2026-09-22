#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass, field
from typing import Any

QUOTED = '"'


@dataclass
class Operation:
    op: str
    fields: dict[str, Any]

    def rendered(self) -> dict[str, Any]:
        return {"op": self.op, **self.fields}


@dataclass
class Gap:
    leaf: str
    shape: str
    detail: str


@dataclass
class Synthesis:
    operations: list[Operation] = field(default_factory=list)
    gaps: list[Gap] = field(default_factory=list)

    @property
    def expressible(self) -> bool:
        return not self.gaps


def quote_segment(segment: str) -> str:
    return f"{QUOTED}{segment}{QUOTED}" if "." in segment else segment


def render_leaf(path: tuple[str, ...]) -> str:
    return ".".join(quote_segment(segment) for segment in path)


def is_mapping(value: Any) -> bool:
    return isinstance(value, dict)


def is_sequence(value: Any) -> bool:
    return isinstance(value, list)


def flat_mapping(value: Any) -> bool:
    return (
        is_mapping(value)
        and bool(value)
        and not any(is_mapping(child) or is_sequence(child) for child in value.values())
    )


def nested_under(previous: Any, current: Any) -> str | None:
    if not flat_mapping(previous) or not is_mapping(current):
        return None
    if any(key in previous for key in current):
        return None
    for key, value in current.items():
        if is_mapping(value) and set(value) == set(previous):
            return key
    return None


def sequence_gap(path: tuple[str, ...], previous: Any, current: Any) -> Gap:
    leaf = render_leaf(path) or "<document>"
    if not is_sequence(previous) or not is_sequence(current):
        return Gap(
            leaf=leaf,
            shape="Sequence replaced a scalar or mapping",
            detail=(
                "The value changed shape between a sequence and something else. "
                "An operation able to rewrite a whole node is required."
            ),
        )
    if len(current) > len(previous) and current[: len(previous)] == previous:
        added = len(current) - len(previous)
        return Gap(
            leaf=leaf,
            shape="Sequence items appended",
            detail=(
                f"{added} item(s) were appended. An operation able to append an "
                "item to a sequence when it is absent is required."
            ),
        )
    if len(current) < len(previous) and previous[: len(current)] == current:
        return Gap(
            leaf=leaf,
            shape="Sequence items removed",
            detail=(
                f"{len(previous) - len(current)} trailing item(s) were removed. An "
                "operation able to remove a sequence item is required."
            ),
        )
    return Gap(
        leaf=leaf,
        shape="Sequence items changed",
        detail=(
            "Items were inserted, reordered or rewritten. An operation able to "
            "address a sequence item is required."
        ),
    )


def synthesize(previous: Any, current: Any) -> Synthesis:
    synthesis = Synthesis()
    walk(synthesis, (), previous, current)
    return synthesis


def walk(
    synthesis: Synthesis,
    path: tuple[str, ...],
    previous: Any,
    current: Any,
) -> None:
    if previous == current:
        return

    if is_mapping(previous) and is_mapping(current):
        walk_mapping(synthesis, path, previous, current)
        return

    if is_sequence(previous) or is_sequence(current):
        synthesis.gaps.append(sequence_gap(path, previous, current))
        return

    synthesis.operations.append(
        Operation(
            op="modify-leaf-value-if",
            fields={"leaf": render_leaf(path), "from": previous, "to": current},
        )
    )


def walk_mapping(
    synthesis: Synthesis,
    path: tuple[str, ...],
    previous: dict,
    current: dict,
) -> None:
    under = nested_under(previous, current)
    if under is not None:
        synthesis.operations.append(
            Operation(
                op="nest-children-if-flat",
                fields={"leaf": render_leaf(path), "under": under},
            )
        )
        previous = {under: previous}

    for key in previous:
        if key not in current:
            synthesis.gaps.append(
                Gap(
                    leaf=render_leaf((*path, str(key))),
                    shape="Key removed",
                    detail=(
                        "No operation removes a mapping key. An operation able to "
                        "drop a key is required."
                    ),
                )
            )

    keys = list(current)
    for position, key in enumerate(keys):
        if key in previous:
            walk(synthesis, (*path, str(key)), previous[key], current[key])
            continue

        anchor = anchor_of(keys, position, previous)
        if anchor is None:
            synthesis.operations.append(
                Operation(
                    op="insert-child-if-missing",
                    fields={
                        "leaf": render_leaf(path),
                        "child": str(key),
                        "value": current[key],
                    },
                )
            )
            continue

        sibling, placement = anchor
        synthesis.operations.append(
            Operation(
                op="insert-sibling-if-existing",
                fields={
                    "leaf": render_leaf((*path, str(sibling))),
                    "sibling": str(key),
                    "placement": placement,
                    "value": current[key],
                },
            )
        )


def anchor_of(keys: list, position: int, previous: dict) -> tuple[Any, str] | None:
    for offset in range(position - 1, -1, -1):
        if keys[offset] in previous:
            return keys[offset], "after"
    for offset in range(position + 1, len(keys)):
        if keys[offset] in previous:
            return keys[offset], "before"
    return None
