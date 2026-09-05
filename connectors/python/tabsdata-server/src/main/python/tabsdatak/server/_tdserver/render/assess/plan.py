#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdserver.render.assess.task import Task, kcheck

CONCERN_LOCAL_REGISTRY = "Local registry"

INSTANCE_STATE = "Instance state"
DEFINITION_MATCH = "Definition match"
MANDATORY_VALUES = "Mandatory values"

TASKS: tuple[Task, ...] = (
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=INSTANCE_STATE,
        run=kcheck("instance-state"),
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=DEFINITION_MATCH,
        run=kcheck("definition-match"),
        needs=(f"{CONCERN_LOCAL_REGISTRY}/{INSTANCE_STATE}",),
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=MANDATORY_VALUES,
        run=kcheck("mandatory-values"),
        needs=(f"{CONCERN_LOCAL_REGISTRY}/{INSTANCE_STATE}",),
    ),
)
