#
# Copyright 2026 Tabsdata Inc.
#

"""API-level tests for the project-oriented authorization (Simple Security)
operations and their enforcement.

These exercise the ``td permission`` Simple-Security facade through the API
client (:meth:`TabsdataServer.grant_permission` / ``revoke_permission`` /
``list_permissions_by_user`` / ``list_permissions_by_project``) and assert both:

* the happy path (an ``admin`` holding ``manage_system`` can grant/revoke/list),
* the scope rules (project permissions require a project; system permissions
  forbid one), and
* enforcement -- a user holding only the auto-assigned ``default`` role (no
  ``manage_system`` / ``manage_project``) is rejected with ``APIServerError``,
  and a freshly-granted ``manage_system`` then *enables* the operation.

All operations go through API calls, not the CLI. Project-scoped grants depend
on the per-project roles minted at project-create time, which is Postgres-only
on the server; those cases create a project first and skip when that is not
available.
"""

import logging
import uuid

import pytest
from tabsdatax.itest.fixture.instances.tabsdata import (
    CONSUMER_TDTEST,
    Instance,
    tabsdata_instance,
)

from tabsdatak.sdk._apiserver import APIServerError
from tabsdatak.sdk.tabsdata_server import TabsdataServer

# Keep the whole module on one xdist worker: the tests mint/destroy shared
# users, roles and grants, which race under ``--dist=loadgroup``.
pytestmark = [
    pytest.mark.xdist_group("connector-permission_group_seq"),
    tabsdata_instance(consumer=CONSUMER_TDTEST),
]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_PASSWORD = "authz_password"


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _client_for(
    instance: Instance, name: str, password: str = _PASSWORD
) -> TabsdataServer:
    """An API client authenticated as the given user (a fresh token, so it
    reflects the user's current grants)."""
    server = instance.apiserver
    return TabsdataServer(f"{server.host}:{server.port}", name, password)


@pytest.fixture
def admin_client(instance_tdtest: Instance) -> TabsdataServer:
    """Admin API client against the registered ``tdtest`` instance."""
    superuser = instance_tdtest.apiserver.superuser
    return _client_for(instance_tdtest, superuser.user, superuser.password)


@pytest.fixture
def regular_user(instance_tdtest: Instance, admin_client):
    """A freshly-created, enabled user holding only the auto-assigned
    ``default`` role (no ``manage_system`` / ``manage_project``), with an API
    client logged in as that user.

    Yields ``(name, client)``.
    """
    name = _unique("authz_user")
    admin_client.delete_user(name, raise_for_status=False)
    admin_client.create_user(name, _PASSWORD)
    # A freshly-created user is flagged `password_must_change`, so its first
    # login is rejected until the password is changed. Clear the flag by
    # changing the password (to the same value) so the user can authenticate.
    admin_client.change_password(name, _PASSWORD, _PASSWORD)
    try:
        yield name, _client_for(instance_tdtest, name)
    finally:
        admin_client.delete_user(name, raise_for_status=False)


@pytest.fixture
def other_user(admin_client):
    """A second plain user, used as the target of grant/revoke operations."""
    name = _unique("authz_target")
    admin_client.delete_user(name, raise_for_status=False)
    admin_client.create_user(name, _PASSWORD)
    try:
        yield name
    finally:
        admin_client.delete_user(name, raise_for_status=False)


# --------------------------------------------------------------------------- #
# Happy path -- admin (manage_system) grants / lists / revokes
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_grant_list_revoke_system_permission(
    instance_tdtest, admin_client, other_user
):
    # `manage_system` is a system permission the `default` role does NOT hold,
    # so it is a clean grant/revoke target (unlike `create_projects`, which
    # every user already has via `default`).
    admin_client.grant_permission(other_user, "manage_system")
    perms = admin_client.list_permissions_by_user(other_user)
    assert any(
        p.permission == "manage_system" and p.project is None for p in perms
    ), f"manage_system not granted; got {perms}"

    admin_client.revoke_permission(other_user, "manage_system")
    perms = admin_client.list_permissions_by_user(other_user)
    assert not any(p.permission == "manage_system" for p in perms), (
        f"manage_system still present after revoke; got {perms}"
    )


# --------------------------------------------------------------------------- #
# Scope validation -- project perms require a project, system perms forbid one
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_system_permission_with_project_rejected(
    instance_tdtest, admin_client, other_user
):
    with pytest.raises(APIServerError):
        admin_client.grant_permission(
            other_user, "manage_system", project="any_project"
        )


@pytest.mark.integration
def test_project_permission_without_project_rejected(
    instance_tdtest, admin_client, other_user
):
    with pytest.raises(APIServerError):
        admin_client.grant_permission(other_user, "develop_project")


# --------------------------------------------------------------------------- #
# Enforcement -- a non-privileged user may not grant / revoke / list-by-project
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_grant_forbidden_for_non_privileged_user(
    instance_tdtest, regular_user, other_user
):
    _, client = regular_user
    # only `default` (no manage_system / manage_project) -> Forbidden
    with pytest.raises(APIServerError):
        client.grant_permission(other_user, "manage_system")


@pytest.mark.integration
def test_revoke_forbidden_for_non_privileged_user(
    instance_tdtest, admin_client, regular_user, other_user
):
    _, client = regular_user
    # admin first grants something so a revoke target exists
    admin_client.grant_permission(other_user, "manage_system")
    try:
        with pytest.raises(APIServerError):
            client.revoke_permission(other_user, "manage_system")
    finally:
        admin_client.revoke_permission(
            other_user, "manage_system", raise_for_status=False
        )


@pytest.mark.integration
def test_list_by_project_forbidden_for_non_privileged_user(
    instance_tdtest, regular_user
):
    _, client = regular_user
    # listing who-can-access a project requires manage on that project
    with pytest.raises(APIServerError):
        client.list_permissions_by_project("any_project")


# --------------------------------------------------------------------------- #
# Enforcement of a granted permission -- manage_system enables granting
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_manage_system_grant_enables_granting(
    instance_tdtest, admin_client, regular_user, other_user
):
    name, client = regular_user

    # Before: the plain user cannot grant.
    with pytest.raises(APIServerError):
        client.grant_permission(other_user, "manage_system")

    # Admin grants manage_system to the user (Simple facade -> sys_admin role).
    admin_client.grant_permission(name, "manage_system")
    try:
        # Re-login so the client carries a token issued after the grant.
        elevated = _client_for(instance_tdtest, name)
        # After: the user can now grant (effective permissions are the union
        # across the user's roles, evaluated live on each request).
        elevated.grant_permission(other_user, "create_projects")
        perms = admin_client.list_permissions_by_user(other_user)
        assert any(p.permission == "create_projects" for p in perms)
    finally:
        admin_client.revoke_permission(
            other_user, "create_projects", raise_for_status=False
        )
        admin_client.revoke_permission(
            name, "manage_system", raise_for_status=False
        )


# --------------------------------------------------------------------------- #
# Project-scoped grant + enforcement (needs a created project -> Postgres).
# --------------------------------------------------------------------------- #


@pytest.fixture
def created_project(admin_client):
    """A project created through the API so its per-project roles
    (``<name>_view`` / ``_dev`` / ``_admin``) are minted. Skips when project
    creation is unavailable (e.g. the server runs on SQLite)."""
    name = _unique("authz_proj")
    admin_client.delete_project(name, raise_for_status=False)
    try:
        admin_client.create_project(name, "authz test project")
    except APIServerError as err:
        pytest.skip(f"project creation unavailable on this backend: {err}")
    try:
        yield name
    finally:
        admin_client.delete_project(name, raise_for_status=False)


@pytest.mark.integration
def test_grant_project_permission_lists_under_project(
    instance_tdtest, admin_client, created_project, other_user
):
    admin_client.grant_permission(
        other_user, "develop_project", project=created_project
    )
    try:
        by_user = admin_client.list_permissions_by_user(other_user)
        assert any(
            p.permission == "develop_project" and p.project == created_project
            for p in by_user
        ), f"develop_project on {created_project} not in {by_user}"

        by_project = admin_client.list_permissions_by_project(created_project)
        assert any(
            a.user == other_user and a.permission == "develop_project"
            for a in by_project
        ), f"{other_user} not listed on {created_project}: {by_project}"
    finally:
        admin_client.revoke_permission(
            other_user, "develop_project", project=created_project, raise_for_status=False
        )


@pytest.mark.integration
def test_project_manager_can_grant_on_own_project_only(
    instance_tdtest, admin_client, created_project, regular_user, other_user
):
    name, client = regular_user
    # Make `name` a manager of the project (manage_project on it).
    admin_client.grant_permission(
        name, "manage_project", project=created_project
    )
    try:
        manager = _client_for(instance_tdtest, name)
        # Can grant a project permission on the project they manage.
        manager.grant_permission(
            other_user, "view_project", project=created_project
        )
        by_user = admin_client.list_permissions_by_user(other_user)
        assert any(
            p.permission == "view_project" and p.project == created_project
            for p in by_user
        )
        # But a project manager cannot grant a *system* permission.
        with pytest.raises(APIServerError):
            manager.grant_permission(other_user, "manage_system")
    finally:
        admin_client.revoke_permission(
            other_user, "view_project", project=created_project, raise_for_status=False
        )
        admin_client.revoke_permission(
            name, "manage_project", project=created_project, raise_for_status=False
        )
