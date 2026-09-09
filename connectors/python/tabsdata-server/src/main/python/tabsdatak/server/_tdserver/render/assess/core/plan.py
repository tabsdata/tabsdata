#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdserver.render.assess.checks import (
    authorization,
    cluster,
    coherence,
    database,
    scheduling,
    storage,
)
from tabsdatak.server._tdserver.render.assess.core.provider import Provider
from tabsdatak.server._tdserver.render.assess.core.task import Task, kcheck
from tabsdatak.server._tdserver.render.assess.core.vantage import Tier

CONCERN_LOCAL_REGISTRY = "Local registry"
CONCERN_HOST_RESOURCES = "Host resources"
CONCERN_CLUSTER_ACCESS = cluster.CONCERN
CONCERN_OPERATOR_RIGHTS = authorization.CONCERN
CONCERN_SCHEDULING = scheduling.CONCERN
CONCERN_DATABASE = database.CONCERN
CONCERN_STORAGE = storage.CONCERN
CONCERN_COHERENCE = coherence.CONCERN

INSTANCE_STATE = "Instance state"
DEFINITION_MATCH = "Definition match"
MANDATORY_VALUES = "Mandatory values"
INSTANCE_VERSION = "Instance version"
INSTANCE_EDITION = "Instance edition"
INIT_FOLDER = "Init folder"
HOST_PORTS = "Host ports"
OPEN_FILES = "Open files"
RUNTIMES_LOCK = "Runtimes lock"

EVERY_PROVIDER = frozenset(Provider)


def verbs_of(wanted) -> str:
    resources = sorted({verb.resource for verb in wanted})
    doing = sorted({verb.verb for verb in wanted})
    return f"Needs {', '.join(doing)} on {', '.join(resources)}"


TASKS: tuple[Task, ...] = (
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=INSTANCE_STATE,
        asserts="The instance folder exists and is initialized",
        detail="State must be initialized, not created or registered",
        run=kcheck("instance-state"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
        gate=True,
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=DEFINITION_MATCH,
        asserts="The arguments given match what init persisted",
        detail="Provider, context, kubeconfig, namespace",
        run=kcheck("definition-match"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=MANDATORY_VALUES,
        asserts="No mandatory placeholder is still unfilled",
        detail="Every ${{literal:}} and ${{secret:}} without a ? suffix",
        run=kcheck("mandatory-values"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=INSTANCE_VERSION,
        asserts="The instance was initialized by this tabsdata",
        detail="The version recorded at init must equal the running one",
        run=kcheck("instance-version"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=INSTANCE_EDITION,
        asserts="The instance was initialized by this edition of tabsdata",
        detail="The two editions ship different pods and configs",
        run=kcheck("instance-edition"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_LOCAL_REGISTRY,
        check=INIT_FOLDER,
        asserts="The initialization folder matches the templates",
        detail="Only the config files may be edited, and only to fill placeholders",
        run=kcheck("init-folder"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_HOST_RESOURCES,
        check=HOST_PORTS,
        asserts="Nothing already holds the ports the instance publishes",
        detail="A kminus instance binds five instance ports plus seven cluster ports",
        run=kcheck("host-ports"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_HOST_RESOURCES,
        check=OPEN_FILES,
        asserts="This shell may open enough files for a kminus cluster",
        detail="Needs a soft limit of 65535",
        run=kcheck("open-files"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_HOST_RESOURCES,
        check=RUNTIMES_LOCK,
        asserts="No other process is building the python environments",
        detail="Tries the advisory lock rather than reading the file",
        run=kcheck("runtimes-lock"),
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_CLUSTER_ACCESS,
        check=cluster.AUTH_PLUGIN,
        asserts="The program the kubeconfig runs to authenticate is present",
        detail="Read from the exec block of the context in use",
        run=cluster.assess_auth_plugin,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_CLUSTER_ACCESS,
        check=cluster.CLUSTER_REACHABLE,
        asserts="The kubeconfig context answers",
        detail="The Kubernetes api server answers a version read",
        run=cluster.assess_cluster_reachable,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
        gate=True,
    ),
    Task(
        concern=CONCERN_CLUSTER_ACCESS,
        check=cluster.NAMESPACE_PRESENT,
        asserts="The namespace already exists",
        detail="The command never creates one",
        run=cluster.assess_namespace,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_CLUSTER_ACCESS,
        check=cluster.NAMESPACE_FREE,
        asserts="No instance already occupies it",
        detail=lambda: f"The configmap {cluster.MANIFEST_CONFIGMAP} must be absent",
        run=cluster.assess_namespace_free,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_CLUSTER_ACCESS}/{cluster.NAMESPACE_PRESENT}",),
    ),
    Task(
        concern=CONCERN_OPERATOR_RIGHTS,
        check=authorization.RIGHTS_BREADTH,
        asserts="Whether this identity is permitted everything",
        detail=lambda: (
            f"Asks to create {authorization.UNCLAIMED.resource}, which nothing declares"
        ),
        run=authorization.assess_rights_breadth,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_OPERATOR_RIGHTS,
        check=authorization.WORKLOAD_RIGHTS,
        asserts="The verbs an instance needs in its namespace",
        detail=lambda: verbs_of(authorization.WORKLOAD),
        run=authorization.assess_workload_rights,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_OPERATOR_RIGHTS,
        check=authorization.TEARDOWN_RIGHTS,
        asserts="The bulk delete a stop depends on",
        detail=lambda: verbs_of(authorization.TEARDOWN),
        run=authorization.assess_teardown_rights,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_OPERATOR_RIGHTS,
        check=authorization.AUTHORING_RIGHTS,
        asserts="The rbac objects an instance installs",
        detail=lambda: verbs_of(authorization.AUTHORING),
        run=authorization.assess_authoring_rights,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_OPERATOR_RIGHTS,
        check=authorization.ESCALATION_RIGHTS,
        asserts="Every right the apiserver role carries",
        detail=lambda: verbs_of(authorization.GRANTED),
        run=authorization.assess_escalation_rights,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_OPERATOR_RIGHTS,
        check=authorization.CLUSTER_READS,
        asserts="The cluster-wide reads create makes",
        detail=lambda: verbs_of(authorization.CLUSTER_WIDE),
        run=authorization.assess_cluster_reads,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_SCHEDULING,
        check=scheduling.PRIORITY_CLASSES,
        asserts="The priority classes the pods name exist",
        detail="A pod naming an absent class is rejected at admission",
        run=scheduling.assess_priority_classes,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_SCHEDULING,
        check=scheduling.PRIORITY_ORDER,
        asserts="Instance pods outrank the runs they trigger",
        detail="The service class must hold a higher value",
        run=scheduling.assess_priority_order,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_SCHEDULING,
        check=scheduling.SERVICE_NODES,
        asserts="A ready node carries the labels the pods select",
        detail="Read from the selectors of the instance",
        run=scheduling.assess_service_nodes,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_SCHEDULING,
        check=scheduling.FUNCTION_POOL,
        asserts="A node or pool can serve the function runs",
        detail="A pool stamping the labels is enough, since runs scale from zero",
        run=scheduling.assess_function_pool,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_SCHEDULING,
        check=scheduling.POOL_TAINT,
        asserts="A pool carries a taint the function pods tolerate",
        detail="Read from the tolerations of the instance",
        run=scheduling.assess_pool_taint,
        tier=Tier.CLUSTER,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.DATABASE_CERTIFICATE,
        asserts="The authority the pods verify the database against is in place",
        detail="Downloaded at startup, except on google where it travels inline",
        run=database.assess_certificate,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.DATABASE_REACH,
        asserts="The database answers and accepts the credentials",
        detail="Resolve, connect, then authenticate",
        run=database.assess_database_reach,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.DATABASE_TLS,
        asserts="The connection is encrypted",
        detail="The view pg_stat_ssl must report ssl for this session",
        run=database.assess_transport_security,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.DATABASE_VERSION,
        asserts="The server runs a version tabsdata supports",
        detail=lambda: (
            f"Supported on PostgreSQL {database.SUPPORTED_MAJOR}, tested on "
            f"{database.spelled(database.TESTED_VERSION)}"
        ),
        run=database.assess_database_version,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.LOCK_CAPACITY,
        asserts="The lock table is large enough",
        detail=lambda: (
            f"Needs max_locks_per_transaction of {database.MINIMUM_LOCKS} or more"
        ),
        run=database.assess_lock_capacity,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.ROLE_ATTRIBUTES,
        asserts="The role can provision what an instance needs",
        detail="Needs createrole and createdb, and must not bypass row-level security",
        run=database.assess_role_attributes,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.DATABASE_OWNERSHIP,
        asserts="The role owns its database",
        detail="Connecting is not owning",
        run=database.assess_database_ownership,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.PROJECT_MARKER,
        asserts="The cross-project marker is held and grantable",
        detail=lambda: (
            f"Holds {database.MARKER_ROLE}, with the option to grant it onward"
        ),
        run=database.assess_project_marker,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
    Task(
        concern=CONCERN_STORAGE,
        check=storage.STORAGE_SHAPE,
        asserts="The three mounts are declared and do not overlap",
        detail=lambda: f"Needs {', '.join(storage.WANTED_MOUNTS)}",
        run=storage.assess_storage_shape,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_STORAGE,
        check=storage.STORAGE_CREDENTIALS,
        asserts="Each mount carries the options its scheme takes",
        detail="Reports an option left over from another provider",
        run=storage.assess_storage_credentials,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_STORAGE}/{storage.STORAGE_SHAPE}",),
    ),
    Task(
        concern=CONCERN_STORAGE,
        check=storage.STORAGE_REACH,
        asserts="The bucket answers and accepts the credentials",
        detail="Reads the properties of each distinct bucket once",
        run=storage.assess_storage_reach,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_STORAGE}/{storage.STORAGE_CREDENTIALS}",),
    ),
    Task(
        concern=CONCERN_COHERENCE,
        check=coherence.PACKAGE_INDEX,
        asserts="The index the function environments resolve against answers",
        detail="Read from UV_INDEX in the scheduling config",
        run=coherence.assess_package_index,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_COHERENCE,
        check=coherence.SECRET_SHAPE,
        asserts="No secret is both supplied and generated",
        detail="A name declared twice leaves its value undecided",
        run=coherence.assess_secret_shape,
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_COHERENCE,
        check=coherence.AGENT_KEYS,
        asserts="The agent has a model it can call",
        detail="Reads the key from the config or from the environment",
        run=coherence.assess_agent_keys,
        tier=Tier.LOCAL,
        providers=EVERY_PROVIDER,
    ),
    Task(
        concern=CONCERN_DATABASE,
        check=database.SCHEMA_STATE,
        asserts="The database holds no schemas from an earlier instance",
        detail="An instance provisions a schema for each of its components",
        run=database.assess_schema_state,
        tier=Tier.REMOTE,
        providers=EVERY_PROVIDER,
        needs=(f"{CONCERN_DATABASE}/{database.DATABASE_REACH}",),
    ),
)
