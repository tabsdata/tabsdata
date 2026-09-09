#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess.core.task import Target
from tabsdatak.server._tdserver.render.assess.render.subject import UNSET, described

pytestmark = pytest.mark.unit

SETTINGS = {
    "state": "initialized",
    "name": "probe",
    "provider": "aks",
    "version": "2.1.0",
    "edition": "enterprise",
    "cluster": {
        "adjusted": {"context": "a-context", "namespace": "a-namespace"},
        "provided": {},
    },
    "ports": [],
}


def shown(target) -> dict[str, str]:
    return dict(described(target))


def test_the_panel_names_what_is_being_assessed(prepared):
    prepared("probe", settings=SETTINGS)
    found = shown(Target(instance="probe"))
    assert found["instance"] == "probe"
    assert found["provider"] == "aks"
    assert found["state"] == "initialized"


def test_the_panel_shows_the_version_and_the_edition(prepared):
    prepared("probe", settings=SETTINGS)
    found = shown(Target(instance="probe"))
    assert found["version"] == "2.1.0"
    assert found["edition"] == "enterprise"


def test_what_the_caller_gives_wins_over_what_was_persisted(prepared):
    prepared("probe", settings=SETTINGS)
    found = shown(Target(instance="probe", namespace="another"))
    assert found["namespace"] == "another"


def test_what_the_caller_omits_comes_from_the_instance(prepared):
    prepared("probe", settings=SETTINGS)
    found = shown(Target(instance="probe"))
    assert found["context"] == "a-context"
    assert found["namespace"] == "a-namespace"


def test_a_hosted_instance_shows_where_its_cluster_is(prepared):
    prepared("probe", settings=SETTINGS)
    assert "kubeconfig" in shown(Target(instance="probe"))


def test_a_local_instance_shows_its_ports_instead(prepared):
    settings = dict(SETTINGS)
    settings["provider"] = "kminus"
    settings["ports"] = [
        {"role": "apiserver_public_port", "caption": "apiserver", "port": 2457},
        {"role": "kminus_api_port", "caption": "kminus", "port": 2461},
    ]
    prepared("probe", settings=settings)
    found = shown(Target(instance="probe"))
    assert found["ports"] == "2457 to 2461"
    assert "kubeconfig" not in found


def test_an_unknown_value_says_so_rather_than_showing_nothing(prepared):
    prepared("probe", settings={**SETTINGS, "cluster": {}})
    found = shown(Target(instance="probe", provider="aks"))
    assert found["context"] == UNSET
    assert found["namespace"] == UNSET


def test_the_current_context_is_named_as_such(prepared):
    prepared("probe", settings={**SETTINGS, "cluster": {}})
    found = shown(Target(instance="probe", provider="aks", current=True))
    assert found["context"] == "the current one"


def test_an_instance_that_does_not_exist_still_describes_what_was_asked_for():
    found = shown(Target(instance="absent", provider="gke"))
    assert found["instance"] == "absent"
    assert found["provider"] == "gke"
    assert "folder" not in found
