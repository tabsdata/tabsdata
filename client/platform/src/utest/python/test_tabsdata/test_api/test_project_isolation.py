#
# Copyright 2025 Tabs Data Inc.
#

import logging
import uuid

import pytest

from test_tabsdata.conftest import scoped_project

from . import pytestmark as _package_pytestmark  # noqa: F401

# Combine the package `api` marker with a file-specific xdist_group so the
# tests in this file (which create and tear down real projects) land on a
# single worker and never race other suites over shared project state.
pytestmark = [
    _package_pytestmark,
    pytest.mark.xdist_group("platform-project_isolation_seq"),
]

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_listing_is_project_independent(tabsserver_connection):
    """The same collection name can live in two projects independently.

    Tenant resources are scoped per project, so a collection name is unique
    only within a project, and each project's listing shows only its own
    collections. This flips the active project on a single connection via
    `scoped_project` and asserts the two listings don't bleed into each other.
    """
    connection = tabsserver_connection.connection
    suffix = uuid.uuid4().hex[:8]
    project_a = f"iso_proj_a_{suffix}"
    project_b = f"iso_proj_b_{suffix}"
    shared = f"iso_coll_{suffix}"
    only_a = f"iso_coll_only_a_{suffix}"
    only_b = f"iso_coll_only_b_{suffix}"

    # Project create/delete are global endpoints -- no active project needed.
    tabsserver_connection.create_project(project_a, description="project A")
    tabsserver_connection.create_project(project_b, description="project B")
    try:
        with scoped_project(connection, project_a):
            tabsserver_connection.create_collection(shared, "shared name, in A")
            tabsserver_connection.create_collection(only_a, "only in A")
        with scoped_project(connection, project_b):
            tabsserver_connection.create_collection(shared, "shared name, in B")
            tabsserver_connection.create_collection(only_b, "only in B")

        with scoped_project(connection, project_a):
            names_a = {c.name for c in tabsserver_connection.collections}
        with scoped_project(connection, project_b):
            names_b = {c.name for c in tabsserver_connection.collections}

        # The shared name coexists in both projects: per-project uniqueness, not
        # global.
        assert shared in names_a
        assert shared in names_b

        # Each project's own collection is visible only in its own listing.
        assert only_a in names_a
        assert only_a not in names_b
        assert only_b in names_b
        assert only_b not in names_a
    finally:
        # Best-effort teardown: drop collections first, then the empty projects.
        with scoped_project(connection, project_a):
            tabsserver_connection.delete_collection(shared, raise_for_status=False)
            tabsserver_connection.delete_collection(only_a, raise_for_status=False)
        with scoped_project(connection, project_b):
            tabsserver_connection.delete_collection(shared, raise_for_status=False)
            tabsserver_connection.delete_collection(only_b, raise_for_status=False)
        tabsserver_connection.delete_project(project_a, raise_for_status=False)
        tabsserver_connection.delete_project(project_b, raise_for_status=False)
