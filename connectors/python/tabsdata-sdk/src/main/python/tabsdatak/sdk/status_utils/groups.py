#
# Copyright 2026 Tabsdata Inc.
#

"""Canonical status semantics, per entity: the same code means different
things on different entities (``F`` is Finished on an execution and Failed on
a function run). ``health`` is how urgent a status is; ``final`` is whether the
server moves it any further on its own."""

from enum import Enum

from tabsdatak.sdk.status_utils.data_version import DATA_VERSION_STATUS_MAPPING
from tabsdatak.sdk.status_utils.execution import EXECUTION_STATUS_MAPPING
from tabsdatak.sdk.status_utils.function_run import FUNCTION_RUN_STATUS_MAPPING
from tabsdatak.sdk.status_utils.transaction import TRANSACTION_STATUS_MAPPING
from tabsdatak.sdk.status_utils.worker import WORKER_STATUS_MAPPING


class Health(str, Enum):
    OK = "ok"
    RUNNING = "running"
    WARNING = "warning"
    ERROR = "error"


class Entity(str, Enum):
    EXECUTION = "execution"
    TRANSACTION = "transaction"
    FUNCTION_RUN = "function_run"
    DATA_VERSION = "data_version"
    WORKER = "worker"


_RUN_GROUPS = {
    Health.RUNNING: frozenset({"S", "RR", "RS", "R", "D"}),
    Health.WARNING: frozenset({"E", "H", "Y"}),
    Health.ERROR: frozenset({"F"}),
    Health.OK: frozenset({"C", "X"}),
}
_RUN_FINAL = frozenset({"F", "H", "Y", "C", "X"})

STATUS_GROUPS: dict[Entity, dict[Health, frozenset[str]]] = {
    Entity.FUNCTION_RUN: _RUN_GROUPS,
    Entity.DATA_VERSION: _RUN_GROUPS,
    Entity.EXECUTION: {
        Health.RUNNING: frozenset({"S", "R"}),
        Health.WARNING: frozenset(),
        Health.ERROR: frozenset({"L", "U"}),
        Health.OK: frozenset({"F"}),
    },
    Entity.TRANSACTION: {
        Health.RUNNING: frozenset({"S", "R"}),
        Health.WARNING: frozenset({"Y"}),
        Health.ERROR: frozenset({"L", "U"}),
        Health.OK: frozenset({"C", "X"}),
    },
    Entity.WORKER: {
        Health.RUNNING: frozenset({"S", "RR", "R", "SPS", "SPR"}),
        Health.WARNING: frozenset({"E"}),
        Health.ERROR: frozenset({"F"}),
        Health.OK: frozenset({"D", "SP", "X"}),
    },
}

FINAL: dict[Entity, frozenset[str]] = {
    Entity.FUNCTION_RUN: _RUN_FINAL,
    Entity.DATA_VERSION: _RUN_FINAL,
    Entity.EXECUTION: frozenset({"F", "L", "U"}),
    Entity.TRANSACTION: frozenset({"C", "X", "Y", "L", "U"}),
    Entity.WORKER: frozenset({"D", "SP", "X", "F"}),
}

_NAMES: dict[Entity, dict[str, str]] = {
    Entity.FUNCTION_RUN: FUNCTION_RUN_STATUS_MAPPING,
    Entity.DATA_VERSION: DATA_VERSION_STATUS_MAPPING,
    Entity.EXECUTION: EXECUTION_STATUS_MAPPING,
    Entity.TRANSACTION: TRANSACTION_STATUS_MAPPING,
    Entity.WORKER: WORKER_STATUS_MAPPING,
}


def _code(entity: Entity, status: str | None) -> str | None:
    if status is None:
        return None
    names = _NAMES[entity]
    if status in names:
        return status
    for code, name in names.items():
        if name == status:
            return code
    return None


def status_name(entity: Entity | str, status: str | None) -> str | None:
    """The display name of a status given as a code or a name."""
    entity = Entity(entity)
    code = _code(entity, status)
    return _NAMES[entity].get(code) if code is not None else None


def health(entity: Entity | str, status: str | None) -> Health | None:
    """How urgent a status (code or name) is; ``None`` when it is unknown."""
    entity = Entity(entity)
    code = _code(entity, status)
    for group, codes in STATUS_GROUPS[entity].items():
        if code in codes:
            return group
    return None


def is_final(entity: Entity | str, status: str | None) -> bool:
    """Whether a status (code or name) no longer changes on its own."""
    entity = Entity(entity)
    return _code(entity, status) in FINAL[entity]


def annotate(entity: Entity | str, row: dict) -> dict:
    """A copy of ``row`` with ``status_name``, ``health`` and ``final`` added
    next to its ``status``, and, next to a ``function_run_status_count`` keyed
    by code, ``function_runs_by_status``: its nonzero counts keyed by name."""
    status = row.get("status")
    group = health(entity, status)
    named = {}
    counts = row.get("function_run_status_count")
    if isinstance(counts, dict):
        named = {
            "function_runs_by_status": {
                status_name(Entity.FUNCTION_RUN, code) or code: count
                for code, count in counts.items()
                if count
            }
        }
    return {
        **row,
        **named,
        "status_name": status_name(entity, status),
        "health": group.value if group is not None else None,
        "final": is_final(entity, status),
    }


def status_groups_document() -> dict:
    """The per-entity model as the JSON document the UI consumes."""
    return {
        "version": 1,
        "entities": {
            entity.value: {
                "groups": {
                    group.value: sorted(codes)
                    for group, codes in STATUS_GROUPS[entity].items()
                },
                "final": sorted(FINAL[entity]),
            }
            for entity in Entity
        },
    }
