#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.checks.cluster import (
    ClusterUnavailable,
    hosted_only,
    reach,
    unreachable,
)
from tabsdatak.server._tdserver.render.assess.core.task import Target

CONCERN = "Operator rights"

RIGHTS_BREADTH = "Rights breadth"
WORKLOAD_RIGHTS = "Workload rights"
TEARDOWN_RIGHTS = "Teardown rights"
AUTHORING_RIGHTS = "Authoring rights"
ESCALATION_RIGHTS = "Granted rights"
CLUSTER_READS = "Cluster reads"

CODE_REFUSED = "assess::rights::refused"
CODE_ESCALATION = "assess::rights::escalation"
CODE_BREADTH = "assess::rights::breadth"

PROBE_WORKERS = 12


@dataclass(frozen=True)
class Verb:
    group: str
    resource: str
    verb: str
    subresource: str = ""

    @property
    def caption(self) -> str:
        name = (
            f"{self.resource}/{self.subresource}" if self.subresource else self.resource
        )
        return f"{self.verb} {name}"


def verbs(group: str, resource: str, *names: str, subresource: str = "") -> list[Verb]:
    return [Verb(group, resource, name, subresource) for name in names]


WORKLOAD: list[Verb] = [
    *verbs("", "pods", "get", "list", "watch", "create", "delete"),
    *verbs("", "pods", "get", subresource="log"),
    *verbs("", "pods", "create", subresource="portforward"),
    *verbs("", "configmaps", "get", "list", "create", "update", "delete"),
    *verbs("", "secrets", "get", "list", "create", "update", "delete"),
    *verbs("", "services", "get", "list", "create", "update", "delete"),
    *verbs("", "serviceaccounts", "create", "delete"),
    *verbs("apps", "deployments", "get", "list", "create", "delete"),
    *verbs("batch", "jobs", "get", "list", "create", "delete"),
    *verbs("batch", "cronjobs", "get", "list", "create", "delete"),
    *verbs("policy", "poddisruptionbudgets", "create", "update"),
]

TEARDOWN: list[Verb] = [
    Verb("", "pods", "deletecollection"),
    Verb("apps", "deployments", "deletecollection"),
    Verb("batch", "jobs", "deletecollection"),
    Verb("batch", "cronjobs", "deletecollection"),
]

AUTHORING: list[Verb] = [
    *verbs("rbac.authorization.k8s.io", "roles", "create", "delete"),
    *verbs("rbac.authorization.k8s.io", "rolebindings", "create", "delete"),
]

GRANTED: list[Verb] = [
    *verbs("", "pods", "get", "list", "watch", "create", "delete"),
    *verbs("", "pods", "get", subresource="log"),
    *verbs("", "configmaps", "get", "list", "create", "update", "delete"),
    *verbs("", "secrets", "get", "list", "create", "update", "delete"),
    *verbs("batch", "jobs", "get", "list", "create", "delete"),
    *verbs("batch", "cronjobs", "get", "list", "create", "delete"),
]

CLUSTER_WIDE: list[Verb] = [
    Verb("", "namespaces", "get"),
    Verb("", "services", "list"),
]

UNCLAIMED = Verb("tabsdata.com", "assess-probes", "create")


def allowed(found, verb: Verb, namespace: str | None) -> bool:
    from kubernetes.client import (
        V1ResourceAttributes,
        V1SelfSubjectAccessReview,
        V1SelfSubjectAccessReviewSpec,
    )

    review = V1SelfSubjectAccessReview(
        spec=V1SelfSubjectAccessReviewSpec(
            resource_attributes=V1ResourceAttributes(
                group=verb.group or None,
                resource=verb.resource,
                subresource=verb.subresource or None,
                verb=verb.verb,
                namespace=namespace,
            )
        )
    )
    answer = found.authorization.create_self_subject_access_review(review)
    return bool(answer.status.allowed)


def refused(found, wanted: list[Verb], namespace: str | None) -> list[str]:
    with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
        outcomes = pool.map(
            lambda verb: (verb, allowed(found, verb, namespace)), wanted
        )
    return [verb.caption for verb, ok in outcomes if not ok]


def group(
    target: Target,
    check: str,
    wanted: list[Verb],
    title: str,
    guidance: str,
    namespaced: bool = True,
    code: str = CODE_REFUSED,
) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, check, "a kminus cluster enforces no permissions of its own"
    )
    if skip:
        return skip

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(check, started, error)

    namespace = found.namespace if namespaced else None
    try:
        missing = refused(found, wanted, namespace)
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=check,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=code,
            title="The rights of this identity could not be read",
            facts=[str(error).strip().splitlines()[0]],
        )

    if not missing:
        return Check(
            concern=CONCERN,
            check=check,
            state=State.PASSED,
            seconds=time.monotonic() - started,
        )

    return Check(
        concern=CONCERN,
        check=check,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=code,
        title=title,
        facts=missing,
        help=[
            Help(text=guidance),
            Help(
                text="Ask whoever administers the cluster to grant them in this "
                "namespace"
            ),
        ],
    )


def assess_rights_breadth(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, RIGHTS_BREADTH, "a kminus cluster enforces no permissions of its own"
    )
    if skip:
        return skip

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(RIGHTS_BREADTH, started, error)

    try:
        unbounded = allowed(found, UNCLAIMED, found.namespace)
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=RIGHTS_BREADTH,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_BREADTH,
            title="The breadth of this identity could not be read",
            facts=[str(error).strip().splitlines()[0]],
        )

    if not unbounded:
        return Check(
            concern=CONCERN,
            check=RIGHTS_BREADTH,
            state=State.PASSED,
            seconds=time.monotonic() - started,
        )

    return Check(
        concern=CONCERN,
        check=RIGHTS_BREADTH,
        state=State.WARNING,
        seconds=time.monotonic() - started,
        code=CODE_BREADTH,
        title="This identity is permitted everything, so the rights below say nothing",
        facts=[
            "A resource that does not exist was reported as creatable",
            "The instance will be created, but a narrower identity may not be able to",
        ],
        help=[
            Help(
                text="Assess again as the identity that will operate the instance, to "
                "learn whether it holds what it needs"
            )
        ],
    )


def assess_workload_rights(target: Target) -> Check:
    return group(
        target,
        WORKLOAD_RIGHTS,
        WORKLOAD,
        "The identity cannot do everything an instance needs",
        "Creating an instance applies pods, services, configmaps and secrets, so "
        "each of these is needed in the namespace",
    )


def assess_teardown_rights(target: Target) -> Check:
    return group(
        target,
        TEARDOWN_RIGHTS,
        TEARDOWN,
        "The identity cannot delete the workloads of an instance in bulk",
        "deletecollection is a verb of its own, so holding delete does not imply "
        "it, and an instance that starts cleanly then refuses to stop",
    )


def assess_authoring_rights(target: Target) -> Check:
    return group(
        target,
        AUTHORING_RIGHTS,
        AUTHORING,
        "The identity cannot author the roles an instance installs",
        "An instance installs a service account, a role and a role binding for its "
        "own apiserver pod",
    )


def assess_escalation_rights(target: Target) -> Check:
    return group(
        target,
        ESCALATION_RIGHTS,
        GRANTED,
        "The identity cannot grant everything the apiserver role carries",
        "Kubernetes refuses to create a role granting a permission its author does "
        "not hold, so every right the instance passes to its own pod has to be held "
        "here first",
        code=CODE_ESCALATION,
    )


def assess_cluster_reads(target: Target) -> Check:
    return group(
        target,
        CLUSTER_READS,
        CLUSTER_WIDE,
        "The identity cannot make the cluster-wide reads create depends on",
        "Creating an instance reads the namespace and lists services across the "
        "cluster to find a port already claimed, and a refusal there stops it",
        namespaced=False,
    )
