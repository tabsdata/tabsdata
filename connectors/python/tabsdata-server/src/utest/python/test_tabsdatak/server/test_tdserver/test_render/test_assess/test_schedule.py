#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess.core import task as task_module
from tabsdatak.server._tdserver.render.assess.core.provider import Provider
from tabsdatak.server._tdserver.render.assess.core.schedule import scheduled
from tabsdatak.server._tdserver.render.assess.core.task import Task
from tabsdatak.server._tdserver.render.assess.core.vantage import Tier

pytestmark = pytest.mark.unit

EVERY = frozenset(Provider)
CLOUD = frozenset({Provider.EKS, Provider.AKS, Provider.GKE})
LOCAL_ONLY = frozenset({Provider.KMINUS})


def task(concern, check, **rest):
    return Task(concern=concern, check=check, run=lambda target: None, **rest)


def plan():
    return [
        task("Local", "state", gate=True),
        task("Local", "ports", providers=LOCAL_ONLY),
        task("Local", "config"),
        task("Cluster", "reach", gate=True, tier=Tier.CLUSTER, providers=CLOUD),
        task("Cluster", "rbac", tier=Tier.CLUSTER, providers=CLOUD),
        task(
            "Remote",
            "database",
            tier=Tier.REMOTE,
            needs=("Cluster/reach",),
            providers=EVERY,
        ),
    ]


def keys(tasks):
    return [item.key for item in tasks]


def needs_of(tasks, key):
    return next(item.needs for item in tasks if item.key == key)


def test_a_gate_is_required_by_everything_after_it():
    planned = scheduled(plan(), Provider.AKS)
    assert needs_of(planned, "Cluster/rbac") == ("Local/state", "Cluster/reach")


def test_a_gate_does_not_require_itself():
    planned = scheduled(plan(), Provider.AKS)
    assert needs_of(planned, "Local/state") == ()


def test_a_gate_requires_only_the_gates_before_it():
    planned = scheduled(plan(), Provider.AKS)
    assert needs_of(planned, "Cluster/reach") == ("Local/state",)


def test_an_explicit_need_that_is_also_a_gate_appears_once():
    planned = scheduled(plan(), Provider.AKS)
    assert needs_of(planned, "Remote/database") == ("Cluster/reach", "Local/state")


def test_checks_of_other_providers_are_dropped():
    planned = scheduled(plan(), Provider.AKS)
    assert "Local/ports" not in keys(planned)

    planned = scheduled(plan(), Provider.KMINUS)
    assert "Local/ports" in keys(planned)
    assert "Cluster/rbac" not in keys(planned)


def test_a_need_on_a_dropped_check_is_dropped_with_it():
    planned = scheduled(plan(), Provider.KMINUS)
    assert needs_of(planned, "Remote/database") == ("Local/state",)


def test_no_provider_keeps_every_check():
    assert len(scheduled(plan(), None)) == len(plan())


def test_an_enterprise_check_is_dropped_from_the_standard_edition(monkeypatch):
    monkeypatch.setattr(task_module, "ENTERPRISE", False)
    wanted = [*plan(), task("Local", "agent", enterprise=True)]
    assert "Local/agent" not in keys(scheduled(wanted, Provider.AKS))


def test_an_enterprise_check_is_kept_in_the_enterprise_edition(monkeypatch):
    monkeypatch.setattr(task_module, "ENTERPRISE", True)
    wanted = [*plan(), task("Local", "agent", enterprise=True)]
    assert "Local/agent" in keys(scheduled(wanted, Provider.AKS))


def test_a_need_on_an_enterprise_check_is_dropped_with_it(monkeypatch):
    monkeypatch.setattr(task_module, "ENTERPRISE", False)
    wanted = [
        *plan(),
        task("Local", "agent", enterprise=True),
        task("Local", "keys", needs=("Local/agent",)),
    ]
    found = needs_of(scheduled(wanted, Provider.AKS), "Local/keys")
    assert "Local/agent" not in found
