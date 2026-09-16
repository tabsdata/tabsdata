#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess import State
from tabsdatak.server._tdserver.render.assess.checks import authorization
from tabsdatak.server._tdserver.render.assess.checks.authorization import (
    AUTHORING,
    CLUSTER_WIDE,
    GRANTED,
    TEARDOWN,
    UNCLAIMED,
    WORKLOAD,
    Verb,
    allowed,
    assess_cluster_reads,
    assess_rights_breadth,
    assess_workload_rights,
    refused,
    verbs,
)
from tabsdatak.server._tdserver.render.assess.checks.cluster import Reach
from tabsdatak.server._tdserver.render.assess.core.task import Target

pytestmark = pytest.mark.unit


class Answer:
    def __init__(self, allowed: bool):
        self.status = type("Status", (), {"allowed": allowed})()


class Authorization:
    def __init__(self, permitted=None, refusing=None):
        self.permitted = permitted
        self.refusing = refusing or set()
        self.asked: list[tuple] = []

    def create_self_subject_access_review(self, review):
        attributes = review.spec.resource_attributes
        seen = (
            attributes.group,
            attributes.resource,
            attributes.subresource,
            attributes.verb,
            attributes.namespace,
        )
        self.asked.append(seen)
        if self.permitted is not None:
            return Answer(self.permitted)
        return Answer((attributes.resource, attributes.verb) not in self.refusing)


def reached(authority) -> Reach:
    return Reach(
        core=None, authorization=authority, custom=None, namespace="a-namespace"
    )


@pytest.fixture
def cluster(monkeypatch):
    def install(authority):
        found = reached(authority)
        monkeypatch.setattr(authorization, "reach", lambda target: found)
        return found

    return install


def test_a_verb_names_the_resource_it_acts_on():
    assert Verb("", "pods", "get").caption == "get pods"


def test_a_verb_on_a_subresource_names_it_too():
    assert Verb("", "pods", "get", "log").caption == "get pods/log"


def test_verbs_expands_one_resource_across_its_names():
    found = verbs("apps", "deployments", "get", "list")
    assert [entry.caption for entry in found] == ["get deployments", "list deployments"]
    assert {entry.group for entry in found} == {"apps"}


def test_the_probe_names_a_resource_nothing_declares():
    assert UNCLAIMED.group == "tabsdata.com"
    assert UNCLAIMED.verb == "create"


def test_a_permitted_verb_is_reported_as_allowed():
    authority = Authorization(permitted=True)
    assert allowed(reached(authority), Verb("", "pods", "get"), "a-namespace")


def test_a_refused_verb_is_reported_as_not_allowed():
    authority = Authorization(permitted=False)
    assert not allowed(reached(authority), Verb("", "pods", "get"), "a-namespace")


def test_the_review_carries_what_is_being_asked():
    authority = Authorization(permitted=True)
    allowed(reached(authority), Verb("apps", "deployments", "create"), "a-namespace")
    assert authority.asked == [("apps", "deployments", None, "create", "a-namespace")]


def test_an_empty_group_is_sent_as_nothing_rather_than_an_empty_string():
    authority = Authorization(permitted=True)
    allowed(reached(authority), Verb("", "pods", "get"), "a-namespace")
    assert authority.asked[0][0] is None


def test_a_cluster_wide_question_carries_no_namespace():
    authority = Authorization(permitted=True)
    allowed(reached(authority), Verb("", "namespaces", "get"), None)
    assert authority.asked[0][4] is None


def test_nothing_refused_yields_an_empty_list():
    authority = Authorization(permitted=True)
    assert refused(reached(authority), WORKLOAD, "a-namespace") == []


def test_every_refusal_is_named():
    authority = Authorization(refusing={("secrets", "create")})
    found = refused(reached(authority), WORKLOAD, "a-namespace")
    assert found == ["create secrets"]


def test_every_wanted_verb_is_asked_about():
    authority = Authorization(permitted=True)
    refused(reached(authority), TEARDOWN, "a-namespace")
    assert len(authority.asked) == len(TEARDOWN)


def test_a_local_cluster_enforces_no_permissions_of_its_own():
    found = assess_workload_rights(Target(instance="probe", provider="kminus"))
    assert found.state is State.WAIVED


def test_an_identity_holding_everything_wanted_passes(cluster):
    cluster(Authorization(permitted=True))
    found = assess_workload_rights(Target(instance="probe", provider="aks"))
    assert found.state is State.PASSED


def test_an_identity_missing_a_verb_fails_and_names_it(cluster):
    cluster(Authorization(refusing={("jobs", "delete")}))
    found = assess_workload_rights(Target(instance="probe", provider="aks"))
    assert found.state is State.FAILED
    assert found.facts == ["delete jobs"]


def test_an_api_server_that_refuses_the_question_only_warns(cluster):
    class Broken:
        def create_self_subject_access_review(self, review):
            raise RuntimeError("the review api is not served")

    cluster(Broken())
    found = assess_workload_rights(Target(instance="probe", provider="aks"))
    assert found.state is State.WARNING


def test_the_cluster_wide_reads_are_asked_without_a_namespace(cluster):
    authority = Authorization(permitted=True)
    cluster(authority)
    assess_cluster_reads(Target(instance="probe", provider="aks"))
    assert {seen[4] for seen in authority.asked} == {None}


def test_the_namespaced_rights_are_asked_within_the_namespace(cluster):
    authority = Authorization(permitted=True)
    cluster(authority)
    assess_workload_rights(Target(instance="probe", provider="aks"))
    assert {seen[4] for seen in authority.asked} == {"a-namespace"}


def test_an_identity_refused_the_unclaimed_resource_is_bounded(cluster):
    cluster(Authorization(refusing={("assess-probes", "create")}))
    found = assess_rights_breadth(Target(instance="probe", provider="aks"))
    assert found.state is State.PASSED


def test_an_identity_permitted_the_unclaimed_resource_is_unbounded(cluster):
    cluster(Authorization(permitted=True))
    found = assess_rights_breadth(Target(instance="probe", provider="aks"))
    assert found.state is State.WARNING


def test_the_breadth_of_an_unreadable_identity_only_warns(cluster):
    class Broken:
        def create_self_subject_access_review(self, review):
            raise RuntimeError("the review api is not served")

    cluster(Broken())
    found = assess_rights_breadth(Target(instance="probe", provider="aks"))
    assert found.state is State.WARNING


def test_the_granted_rights_are_a_subset_of_what_the_operator_needs():
    assert {verb.caption for verb in GRANTED} <= {verb.caption for verb in WORKLOAD}


def test_bulk_deletion_is_asked_for_on_every_workload_kind():
    assert all(verb.verb == "deletecollection" for verb in TEARDOWN)
    assert {verb.resource for verb in TEARDOWN} == {
        "pods",
        "deployments",
        "jobs",
        "cronjobs",
    }


def test_authoring_covers_both_the_role_and_its_binding():
    assert {verb.resource for verb in AUTHORING} == {"roles", "rolebindings"}


def test_the_cluster_wide_reads_are_only_reads():
    assert {verb.verb for verb in CLUSTER_WIDE} == {"get", "list"}
