#
# Copyright 2026 Tabsdata Inc.
#

"""Simple Security Mode permission-type constants.

Extracted from :mod:`tabsdatak.sdk.tabsdata_server` -- which is expensive to
import -- so the ``tdk permission`` command module can reference them in its
option help at registration time without loading the full server SDK.
``tabsdata_server`` re-exports them, so existing importers are unaffected.

Flat per-user permissions, the surface exposed by ``tdk permission``. The server
translates these into RBAC role-membership operations; there is no
``simple_permission`` table. See ``server/.../dxo/simple_permission.rs`` and
``server/.../types/basic/typed_enum.rs::SimplePermissionType``.
"""

# Project-scoped permissions: require `--proj`.
SIMPLE_PROJECT_PERMISSION_TYPES = [
    "view_project",
    "develop_project",
    "manage_project",
]

# System-scoped permissions: must not carry `--proj`. `manage_system` is full
# system admin (sys_admin): user/role administration plus authority over all
# projects.
SIMPLE_SYSTEM_PERMISSION_TYPES = [
    "create_projects",
    "manage_system",
]

VALID_SIMPLE_PERMISSION_TYPES = (
    SIMPLE_PROJECT_PERMISSION_TYPES + SIMPLE_SYSTEM_PERMISSION_TYPES
)

# Org-wide read/develop wildcards have no Simple-surface name; rejected
# client-side (and by the server) with a "use RBAC Mode View" message.
SIMPLE_RBAC_ONLY_WILDCARD_TYPES = [
    "view_all_projects",
    "develop_all_projects",
]

RBAC_ONLY_WILDCARD_MESSAGE = (
    "org-wide view/develop is RBAC Mode View only; grant per project or use RBAC Mode."
)
