#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
import yaml

from tabsdatak.server._tdserver.render.assess.checks.cluster import (
    DEFAULT_NAMESPACE,
    first_line,
    invoked,
    kubeconfig_of,
    namespace_of,
    reason_of,
    sign_in_help,
)
from tabsdatak.server._tdserver.render.assess.core.provider import Provider
from tabsdatak.server._tdserver.render.assess.core.task import Target

pytestmark = pytest.mark.unit

KUBECONFIG = {
    "current-context": "aks",
    "contexts": [
        {"name": "aks", "context": {"user": "azure-user"}},
        {"name": "eks", "context": {"user": "amazon-user"}},
        {"name": "gke", "context": {"user": "google-user"}},
    ],
    "users": [
        {"name": "azure-user", "user": {"exec": {"command": "kubelogin"}}},
        {"name": "amazon-user", "user": {"token": "a-token"}},
        {
            "name": "google-user",
            "user": {"exec": {"command": "/opt/google/gke-gcloud-auth-plugin"}},
        },
    ],
}


@pytest.fixture
def written(tmp_path):
    def make(document) -> str:
        path = tmp_path / "config"
        path.write_text(
            document if isinstance(document, str) else yaml.safe_dump(document)
        )
        return str(path)

    return make


def test_an_instance_naming_no_namespace_lands_in_the_default_one():
    assert namespace_of(Target(instance="probe")) == DEFAULT_NAMESPACE


def test_a_named_namespace_wins():
    assert namespace_of(Target(instance="probe", namespace="chosen")) == "chosen"


def test_the_named_kubeconfig_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "/from/environment")
    found = kubeconfig_of(Target(instance="probe", kubeconfig="/from/argument"))
    assert str(found) == "/from/argument"


def test_the_environment_is_read_when_no_kubeconfig_is_named(monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "/from/environment")
    assert str(kubeconfig_of(Target(instance="probe"))) == "/from/environment"


def test_only_the_first_entry_of_a_kubeconfig_list_is_read(monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "/first:/second:/third")
    assert str(kubeconfig_of(Target(instance="probe"))) == "/first"


def test_an_empty_kubeconfig_variable_falls_back_to_the_home_of_the_user(monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "")
    found = kubeconfig_of(Target(instance="probe"))
    assert found.parts[-2:] == (".kube", "config")


def test_the_helper_of_the_named_context_is_read(written):
    target = Target(instance="probe", kubeconfig=written(KUBECONFIG), context="gke")
    assert invoked(target) == "/opt/google/gke-gcloud-auth-plugin"


def test_the_current_context_is_read_when_none_is_named(written):
    target = Target(instance="probe", kubeconfig=written(KUBECONFIG))
    assert invoked(target) == "kubelogin"


def test_asking_for_the_current_context_ignores_the_one_given(written):
    target = Target(
        instance="probe", kubeconfig=written(KUBECONFIG), context="gke", current=True
    )
    assert invoked(target) == "kubelogin"


def test_a_context_minting_its_own_token_names_no_helper(written):
    target = Target(instance="probe", kubeconfig=written(KUBECONFIG), context="eks")
    assert invoked(target) is None


def test_a_context_that_is_not_in_the_kubeconfig_names_no_helper(written):
    target = Target(instance="probe", kubeconfig=written(KUBECONFIG), context="absent")
    assert invoked(target) is None


def test_a_kubeconfig_that_is_not_there_names_no_helper(tmp_path):
    target = Target(instance="probe", kubeconfig=str(tmp_path / "absent"))
    assert invoked(target) is None


def test_an_unreadable_kubeconfig_names_no_helper(written):
    target = Target(instance="probe", kubeconfig=written("contexts: [unclosed"))
    assert invoked(target) is None


def test_an_empty_kubeconfig_names_no_helper(written):
    target = Target(instance="probe", kubeconfig=written(""))
    assert invoked(target) is None


class Refusal(Exception):
    def __init__(self, status: int):
        super().__init__(f"({status})")
        self.status = status


def test_a_refused_credential_is_told_from_a_refused_request():
    assert "refused the credentials" in reason_of(Refusal(401), Provider.AKS)
    assert "refused the request" in reason_of(Refusal(403), Provider.AKS)


def test_a_failing_helper_is_named_in_the_reason():
    error = Exception("exec: executable kubelogin not found")
    assert reason_of(error, Provider.AKS) == (
        "the authentication plugin kubelogin could not be run"
    )


def test_a_provider_without_a_helper_reports_what_went_wrong():
    error = Exception("connection refused")
    assert reason_of(error, Provider.EKS) == "connection refused"


def test_an_unknown_provider_still_reports_what_went_wrong():
    assert reason_of(Exception("no route to host"), None) == "no route to host"


def test_only_the_first_line_of_an_error_is_kept():
    assert first_line(Exception("first\nsecond\nthird")) == "first"


def test_an_error_that_says_nothing_is_shown_as_itself():
    assert "ValueError" in first_line(ValueError())


def test_a_hosted_provider_offers_the_way_back_in():
    found = sign_in_help(Provider.AKS)
    assert [entry.command for entry in found if entry.command] == list(
        Provider.AKS.sign_in
    )


def test_a_local_cluster_offers_no_way_back_in():
    assert sign_in_help(Provider.KMINUS) == []


def test_an_unknown_provider_offers_no_way_back_in():
    assert sign_in_help(None) == []
