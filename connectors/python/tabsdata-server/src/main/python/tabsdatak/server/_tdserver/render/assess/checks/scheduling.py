#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.checks.cluster import (
    ClusterUnavailable,
    hosted_only,
    reach,
    unreachable,
)
from tabsdatak.server._tdserver.render.assess.core.task import Target

CONCERN = "Scheduling"

PRIORITY_CLASSES = "Priority classes"
PRIORITY_ORDER = "Priority order"
SERVICE_NODES = "Service nodes"
FUNCTION_POOL = "Function pool"
POOL_TAINT = "Function taint"

CODE_PRIORITY = "assess::scheduling::priority"
CODE_ORDER = "assess::scheduling::order"
CODE_NODES = "assess::scheduling::nodes"
CODE_POOL = "assess::scheduling::pool"
CODE_TAINT = "assess::scheduling::taint"

CONFIG_FILE = "tabsdata.yaml"

KARPENTER_GROUP = "karpenter.sh"
KARPENTER_VERSION = "v1"
KARPENTER_PLURAL = "nodepools"


@dataclass(frozen=True)
class Toleration:
    key: str
    value: str
    effect: str

    @property
    def caption(self) -> str:
        return f"{self.key}={self.value}:{self.effect}"


@dataclass
class Contract:
    service_priority: str | None = None
    function_priority: str | None = None
    service_selectors: dict = field(default_factory=dict)
    function_selectors: dict = field(default_factory=dict)
    function_tolerations: list[Toleration] = field(default_factory=list)
    largest_cores: float = 0.0


def config_of(instance: str) -> Path:
    return (
        Path.home()
        / ".tabsdata"
        / "instances"
        / instance
        / "init"
        / "input"
        / "configs"
        / CONFIG_FILE
    )


def tolerations_of(placement: dict) -> list[Toleration]:
    return [
        Toleration(
            key=str(entry.get("key", "")),
            value=str(entry.get("value", "")),
            effect=str(entry.get("effect", "")),
        )
        for entry in placement.get("tolerations") or []
        if isinstance(entry, dict)
    ]


def contract_of(instance: str) -> Contract | None:
    path = config_of(instance)
    if not path.is_file():
        return None
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None

    pods = document.get("pods") or {}
    service = pods.get("service") or {}
    function = pods.get("function") or {}
    return Contract(
        largest_cores=largest_request(pods),
        service_priority=service.get("priority"),
        function_priority=function.get("priority"),
        service_selectors=(service.get("placement") or {}).get("selectors") or {},
        function_selectors=(function.get("placement") or {}).get("selectors") or {},
        function_tolerations=tolerations_of(function.get("placement") or {}),
    )


def largest_request(pods: dict) -> float:
    wanted = [0.0]
    for group in pods.values():
        if not isinstance(group, dict):
            continue
        for holder in (group, *(v for v in group.values() if isinstance(v, dict))):
            asked = (holder.get("resources") or {}).get("cpu")
            if asked is None:
                continue
            try:
                wanted.append(float(str(asked)))
            except ValueError:
                continue
    return max(wanted)


def unreadable(check: str, started: float, instance: str) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.SKIPPED,
        seconds=time.monotonic() - started,
        reason="the scheduling configuration of the instance could not be read",
        facts=[f"Expected file: {config_of(instance)}"],
    )


def passed(check: str, started: float) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.PASSED,
        seconds=time.monotonic() - started,
    )


def assess_priority_classes(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, PRIORITY_CLASSES, "a kminus instance pins its pods to one node"
    )
    if skip:
        return skip

    contract = contract_of(target.instance)
    if contract is None:
        return unreadable(PRIORITY_CLASSES, started, target.instance)

    wanted = [
        name for name in (contract.service_priority, contract.function_priority) if name
    ]
    if not wanted:
        return Check(
            concern=CONCERN,
            check=PRIORITY_CLASSES,
            state=State.WAIVED,
            reason="the instance names no priority class",
        )

    try:
        reach(target)
    except ClusterUnavailable as error:
        return unreachable(PRIORITY_CLASSES, started, error)

    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    scheduling = client.SchedulingV1Api()
    missing = []
    for name in wanted:
        try:
            scheduling.read_priority_class(name)
        except ApiException as error:
            if error.status == 404:
                missing.append(name)
            else:
                return Check(
                    concern=CONCERN,
                    check=PRIORITY_CLASSES,
                    state=State.WARNING,
                    seconds=time.monotonic() - started,
                    code=CODE_PRIORITY,
                    title="The priority classes could not be read",
                    facts=[str(error).strip().splitlines()[0]],
                )

    if not missing:
        return passed(PRIORITY_CLASSES, started)

    return Check(
        concern=CONCERN,
        check=PRIORITY_CLASSES,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_PRIORITY,
        title="A priority class the instance names does not exist in the cluster",
        facts=[f"{name}: absent" for name in missing],
        help=[
            Help(
                text="The API server rejects a pod naming a priority class that does "
                "not exist, so every pod of the instance fails at admission"
            ),
            Help(text="Ask whoever administers the cluster to create it"),
        ],
    )


def assess_priority_order(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, PRIORITY_ORDER, "a kminus instance pins its pods to one node"
    )
    if skip:
        return skip

    contract = contract_of(target.instance)
    if contract is None:
        return unreadable(PRIORITY_ORDER, started, target.instance)
    if not (contract.service_priority and contract.function_priority):
        return Check(
            concern=CONCERN,
            check=PRIORITY_ORDER,
            state=State.WAIVED,
            reason="the instance names fewer than two priority classes",
        )

    try:
        reach(target)
    except ClusterUnavailable as error:
        return unreachable(PRIORITY_ORDER, started, error)

    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    scheduling = client.SchedulingV1Api()
    try:
        service = scheduling.read_priority_class(contract.service_priority)
        function = scheduling.read_priority_class(contract.function_priority)
    except ApiException as error:
        return Check(
            concern=CONCERN,
            check=PRIORITY_ORDER,
            state=State.SKIPPED,
            seconds=time.monotonic() - started,
            reason="a priority class could not be read",
            facts=[str(error).strip().splitlines()[0]],
        )

    if service.value > function.value:
        return passed(PRIORITY_ORDER, started)

    return Check(
        concern=CONCERN,
        check=PRIORITY_ORDER,
        state=State.WARNING,
        seconds=time.monotonic() - started,
        code=CODE_ORDER,
        title="The pods of the instance do not outrank the function runs they trigger",
        facts=[
            f"{contract.service_priority}: {service.value}",
            f"{contract.function_priority}: {function.value}",
        ],
        help=[
            Help(
                text="A long running pod queues behind a batch of function runs when "
                "the cluster is full, which stalls the instance rather than the work"
            )
        ],
    )


def ready_nodes(found, selectors: dict) -> list:
    selector = ",".join(f"{key}={value}" for key, value in selectors.items())
    nodes = found.core.list_node(label_selector=selector).items if selector else []
    return [node for node in nodes if is_ready(node)]


def is_ready(node) -> bool:
    for condition in node.status.conditions or []:
        if condition.type == "Ready":
            return condition.status == "True"
    return False


def assess_service_nodes(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, SERVICE_NODES, "a kminus instance pins its pods to one node"
    )
    if skip:
        return skip

    contract = contract_of(target.instance)
    if contract is None:
        return unreadable(SERVICE_NODES, started, target.instance)
    if not contract.service_selectors:
        return Check(
            concern=CONCERN,
            check=SERVICE_NODES,
            state=State.WAIVED,
            reason="the instance requires no node label of its own",
        )

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(SERVICE_NODES, started, error)

    try:
        ready = ready_nodes(found, contract.service_selectors)
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=SERVICE_NODES,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_NODES,
            title="The nodes of the cluster could not be listed",
            facts=[str(error).strip().splitlines()[0]],
        )

    if ready:
        return passed(SERVICE_NODES, started)

    wanted = ", ".join(f"{k}={v}" for k, v in contract.service_selectors.items())
    return Check(
        concern=CONCERN,
        check=SERVICE_NODES,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_NODES,
        title="No ready node carries the labels the pods of the instance select",
        facts=[f"Selector: {wanted}"],
        help=[
            Help(
                text="The pods that make up an instance run for as long as it does, "
                "so they need a node already up rather than one provisioned on demand"
            ),
            Help(text="Ask whoever administers the cluster for a node so labelled"),
        ],
    )


def nodepools(found) -> list:
    return (
        found.custom.list_cluster_custom_object(
            group=KARPENTER_GROUP,
            version=KARPENTER_VERSION,
            plural=KARPENTER_PLURAL,
        ).get("items")
        or []
    )


def taints_of(pool: dict) -> list[Toleration]:
    spec = ((pool.get("spec") or {}).get("template") or {}).get("spec") or {}
    return [
        Toleration(
            key=str(entry.get("key", "")),
            value=str(entry.get("value", "")),
            effect=str(entry.get("effect", "")),
        )
        for entry in spec.get("taints") or []
        if isinstance(entry, dict)
    ]


def labels_of(pool: dict) -> dict:
    template = (pool.get("spec") or {}).get("template") or {}
    return (template.get("metadata") or {}).get("labels") or {}


def assess_function_pool(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, FUNCTION_POOL, "a kminus instance runs its functions locally"
    )
    if skip:
        return skip

    contract = contract_of(target.instance)
    if contract is None:
        return unreadable(FUNCTION_POOL, started, target.instance)
    if not contract.function_selectors:
        return Check(
            concern=CONCERN,
            check=FUNCTION_POOL,
            state=State.WAIVED,
            reason="the instance requires no node label for its function runs",
        )

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(FUNCTION_POOL, started, error)

    if ready_or_none(found, contract.function_selectors):
        return passed(FUNCTION_POOL, started)

    try:
        pools = nodepools(found)
    except Exception:
        pools = []

    matching = [
        pool
        for pool in pools
        if all(
            labels_of(pool).get(k) == v for k, v in contract.function_selectors.items()
        )
    ]
    if matching:
        return passed(FUNCTION_POOL, started)

    wanted = ", ".join(f"{k}={v}" for k, v in contract.function_selectors.items())
    return Check(
        concern=CONCERN,
        check=FUNCTION_POOL,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_POOL,
        title="Nothing in the cluster can provide a node for the function runs",
        facts=[
            f"Selector: {wanted}",
            "No ready node carries these labels, and no node pool stamps them",
        ],
        help=[
            Help(
                text="Function runs scale from zero, so a pool that stamps these "
                "labels is enough even when no node exists yet"
            ),
            Help(text="Ask whoever administers the cluster for such a pool"),
        ],
    )


def ready_or_none(found, selectors: dict) -> bool:
    try:
        return bool(ready_nodes(found, selectors))
    except Exception:
        return False


def assess_pool_taint(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, POOL_TAINT, "a kminus instance runs its functions locally"
    )
    if skip:
        return skip

    contract = contract_of(target.instance)
    if contract is None:
        return unreadable(POOL_TAINT, started, target.instance)
    if not contract.function_tolerations:
        return Check(
            concern=CONCERN,
            check=POOL_TAINT,
            state=State.WAIVED,
            reason="the function pods of the instance tolerate no taint",
        )

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(POOL_TAINT, started, error)

    try:
        pools = nodepools(found)
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=POOL_TAINT,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_TAINT,
            title="The node pools of the cluster could not be read",
            facts=[str(error).strip().splitlines()[0]],
        )

    if not pools:
        return Check(
            concern=CONCERN,
            check=POOL_TAINT,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_TAINT,
            title="The taints the function pods tolerate could not be confirmed",
            facts=["This cluster declares no node pool this command can read"],
        )

    tolerated = {entry.caption for entry in contract.function_tolerations}
    for pool in pools:
        taints = taints_of(pool)
        if taints and all(taint.caption in tolerated for taint in taints):
            return passed(POOL_TAINT, started)

    every = sorted({taint.caption for pool in pools for taint in taints_of(pool)})
    return Check(
        concern=CONCERN,
        check=POOL_TAINT,
        state=State.WARNING,
        seconds=time.monotonic() - started,
        code=CODE_TAINT,
        title="No node pool carries a taint the function pods tolerate",
        facts=[
            f"Tolerated: {', '.join(sorted(tolerated))}",
            f"Found: {', '.join(every) if every else 'no taint on any pool'}",
        ],
        help=[
            Help(
                text="A function pod carrying a toleration nothing matches still "
                "schedules, but it lands wherever it fits rather than on the pool "
                "meant for it"
            )
        ],
    )
