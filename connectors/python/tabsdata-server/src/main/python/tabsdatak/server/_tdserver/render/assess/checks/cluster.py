#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import yaml

from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.core.provider import Provider, provider_of
from tabsdatak.server._tdserver.render.assess.core.task import Target

CONCERN = "Cluster access"

CLUSTER_REACHABLE = "API server"
NAMESPACE_PRESENT = "Namespace presence"
NAMESPACE_FREE = "Namespace occupancy"
AUTH_PLUGIN = "Authentication helper"

CODE_UNREACHABLE = "assess::cluster::unreachable"
CODE_NAMESPACE = "assess::cluster::namespace"
CODE_OCCUPIED = "assess::cluster::occupied"
CODE_PLUGIN = "assess::cluster::plugin"

MANIFEST_CONFIGMAP = "td-sys-tabsdata-manifest-manifest.yaml"

DEFAULT_NAMESPACE = "tabsdata"


class ClusterUnavailable(Exception):
    pass


@dataclass(frozen=True)
class Reach:
    core: object
    authorization: object
    custom: object
    namespace: str


_reached: dict[tuple, Reach | ClusterUnavailable] = {}
_guard = Lock()


def namespace_of(target: Target) -> str:
    return target.namespace or DEFAULT_NAMESPACE


def reach(target: Target) -> Reach:
    key = (target.kubeconfig, target.context, target.current, namespace_of(target))
    with _guard:
        held = _reached.get(key)
        if held is None:
            try:
                held = connect(target)
            except ClusterUnavailable as error:
                held = error
            _reached[key] = held
    if isinstance(held, ClusterUnavailable):
        raise held
    return held


def connect(target: Target) -> Reach:
    from kubernetes import client, config

    try:
        config.load_kube_config(
            config_file=target.kubeconfig,
            context=None if target.current else target.context,
        )
    except Exception as error:
        raise ClusterUnavailable(
            f"the kubeconfig could not be loaded: {first_line(error)}"
        ) from error

    try:
        client.VersionApi().get_code()
    except Exception as error:
        raise ClusterUnavailable(
            reason_of(error, provider_of(target.provider))
        ) from error

    return Reach(
        core=client.CoreV1Api(),
        authorization=client.AuthorizationV1Api(),
        custom=client.CustomObjectsApi(),
        namespace=namespace_of(target),
    )


def first_line(error: Exception) -> str:
    text = str(error).strip()
    return text.splitlines()[0] if text else repr(error)


def reason_of(error: Exception, provider: Provider | None) -> str:
    status = getattr(error, "status", None)
    if status == 401:
        return "the cluster refused the credentials of this kubeconfig"
    if status == 403:
        return "the cluster accepted the credentials but refused the request"
    text = first_line(error)
    plugin = provider.auth_plugin if provider else None
    if plugin and plugin in text:
        return f"the authentication plugin {plugin} could not be run"
    return text


def sign_in_help(provider: Provider | None) -> list[Help]:
    if provider is None or not provider.sign_in:
        return []
    entries = [
        Help(
            text="The session of the command line tool may have lapsed, and the "
            "kubeconfig mints its token from it"
        )
    ]
    entries += [
        Help(text="Sign in again, with", command=step) for step in provider.sign_in
    ]
    return entries


def waived(check: str, reason: str) -> Check:
    return Check(concern=CONCERN, check=check, state=State.WAIVED, reason=reason)


def unreachable(check: str, started: float, error: Exception) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_UNREACHABLE,
        title="The cluster could not be reached",
        facts=[str(error)],
    )


def passed(check: str, started: float) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.PASSED,
        seconds=time.monotonic() - started,
    )


def hosted_only(target: Target, check: str, reason: str) -> Check | None:
    provider = provider_of(target.provider)
    if provider is not None and not provider.hosted:
        return waived(check, reason)
    return None


def assess_auth_plugin(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, AUTH_PLUGIN, "a kminus instance authenticates against no api server"
    )
    if skip:
        return skip

    wanted = invoked(target)
    if not wanted:
        return Check(
            concern=CONCERN,
            check=AUTH_PLUGIN,
            state=State.WAIVED,
            reason="this context mints its token without a helper",
        )

    found = shutil.which(wanted) if not Path(wanted).is_absolute() else wanted
    if found and Path(found).exists():
        return Check(
            concern=CONCERN,
            check=AUTH_PLUGIN,
            state=State.PASSED,
            seconds=time.monotonic() - started,
            facts=[str(found)],
        )

    provider = provider_of(target.provider)
    return Check(
        concern=CONCERN,
        check=AUTH_PLUGIN,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_PLUGIN,
        title=f"The kubeconfig runs '{wanted}' to authenticate, and it is not there",
        facts=["Named by the exec block of this context in the kubeconfig"],
        help=[
            Help(
                text="A missing helper surfaces as an authentication failure rather "
                "than as a missing program, which sends the reader the wrong way"
            ),
            *sign_in_help(provider),
        ],
    )


def kubeconfig_of(target: Target) -> Path:
    if target.kubeconfig:
        return Path(target.kubeconfig)
    named = os.environ.get("KUBECONFIG")
    if named:
        first = named.split(os.pathsep)[0]
        if first:
            return Path(first)
    return Path.home() / ".kube" / "config"


def invoked(target: Target) -> str | None:
    path = kubeconfig_of(target)
    if not path.is_file():
        return None
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None

    wanted = None if target.current else target.context
    wanted = wanted or document.get("current-context")
    user = None
    for entry in document.get("contexts") or []:
        if entry.get("name") == wanted:
            user = (entry.get("context") or {}).get("user")
            break
    if not user:
        return None

    for entry in document.get("users") or []:
        if entry.get("name") == user:
            return ((entry.get("user") or {}).get("exec") or {}).get("command")
    return None


def assess_cluster_reachable(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target,
        CLUSTER_REACHABLE,
        "a kminus instance brings up the cluster it needs itself",
    )
    if skip:
        return skip

    provider = provider_of(target.provider)
    try:
        reach(target)
    except ClusterUnavailable as error:
        return Check(
            concern=CONCERN,
            check=CLUSTER_REACHABLE,
            state=State.FAILED,
            seconds=time.monotonic() - started,
            code=CODE_UNREACHABLE,
            title="The cluster could not be reached",
            facts=[str(error)],
            help=sign_in_help(provider),
        )
    return passed(CLUSTER_REACHABLE, started)


def assess_namespace(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target,
        NAMESPACE_PRESENT,
        "a kminus instance runs in the default namespace of its own cluster",
    )
    if skip:
        return skip

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(NAMESPACE_PRESENT, started, error)

    from kubernetes.client.exceptions import ApiException

    try:
        found.core.read_namespace(found.namespace)
    except ApiException as error:
        if error.status == 404:
            return Check(
                concern=CONCERN,
                check=NAMESPACE_PRESENT,
                state=State.FAILED,
                seconds=time.monotonic() - started,
                code=CODE_NAMESPACE,
                title=f"The namespace '{found.namespace}' does not exist",
                facts=[
                    "tdkserver never creates a namespace, so one has to exist already"
                ],
                help=[
                    Help(
                        text="Ask whoever administers the cluster for one, or name a "
                        "namespace that exists",
                        command="tdkserver assess --namespace <NAMESPACE>",
                    )
                ],
            )
        return Check(
            concern=CONCERN,
            check=NAMESPACE_PRESENT,
            state=State.FAILED,
            seconds=time.monotonic() - started,
            code=CODE_NAMESPACE,
            title=f"The namespace '{found.namespace}' could not be read",
            facts=[reason_of(error, provider_of(target.provider))],
        )
    return passed(NAMESPACE_PRESENT, started)


def assess_namespace_free(target: Target) -> Check:
    started = time.monotonic()
    skip = hosted_only(
        target, NAMESPACE_FREE, "a kminus instance holds a cluster of its own"
    )
    if skip:
        return skip

    try:
        found = reach(target)
    except ClusterUnavailable as error:
        return unreachable(NAMESPACE_FREE, started, error)

    from kubernetes.client.exceptions import ApiException

    try:
        found.core.read_namespaced_config_map(MANIFEST_CONFIGMAP, found.namespace)
    except ApiException as error:
        if error.status == 404:
            return passed(NAMESPACE_FREE, started)
        return Check(
            concern=CONCERN,
            check=NAMESPACE_FREE,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_OCCUPIED,
            title="Whether the namespace already holds an instance could not be read",
            facts=[reason_of(error, provider_of(target.provider))],
        )
    return Check(
        concern=CONCERN,
        check=NAMESPACE_FREE,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_OCCUPIED,
        title=f"The namespace '{found.namespace}' already holds a tabsdata instance",
        facts=[f"The configmap '{MANIFEST_CONFIGMAP}' is present"],
        help=[
            Help(
                text="Delete that instance, or use another namespace",
                command="tdkserver trash",
            )
        ],
    )
