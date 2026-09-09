#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess.core.plan import TASKS
from tabsdatak.server._tdserver.render.assess.core.provider import Provider
from tabsdatak.server._tdserver.render.assess.core.schedule import scheduled

pytestmark = pytest.mark.unit


def test_every_check_is_named_once():
    keys = [task.key for task in TASKS]
    assert len(keys) == len(set(keys))


def test_every_check_explains_itself():
    silent = [task.key for task in TASKS if not task.asserts]
    assert silent == []


def test_every_explanation_reads_as_a_sentence():
    lowercase = [
        task.key
        for task in TASKS
        if (task.asserts and task.asserts[0].islower())
        or (task.particulars and task.particulars[0].islower())
    ]
    assert lowercase == []


def test_every_check_applies_to_a_provider():
    orphaned = [task.key for task in TASKS if not task.providers]
    assert orphaned == []


@pytest.mark.parametrize("provider", list(Provider))
def test_every_dependency_is_a_check_that_runs(provider):
    planned = scheduled(TASKS, provider)
    known = {task.key for task in planned}
    for task in planned:
        assert set(task.needs) <= known, task.key


@pytest.mark.parametrize("provider", list(Provider))
def test_no_check_waits_on_one_that_comes_later(provider):
    planned = scheduled(TASKS, provider)
    seen: set[str] = set()
    for task in planned:
        assert set(task.needs) <= seen, task.key
        seen.add(task.key)


@pytest.mark.parametrize("provider", list(Provider))
def test_no_check_waits_on_itself(provider):
    for task in scheduled(TASKS, provider):
        assert task.key not in task.needs


def test_every_provider_keeps_the_local_registry():
    for provider in Provider:
        planned = scheduled(TASKS, provider)
        concerns = {task.concern for task in planned}
        assert "Local registry" in concerns


def test_a_local_instance_keeps_fewer_checks_than_a_hosted_one():
    local = scheduled(TASKS, Provider.KMINUS)
    hosted = scheduled(TASKS, Provider.AKS)
    assert len(local) <= len(hosted)
