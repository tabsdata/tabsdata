//
// Copyright 2025 Tabs Data Inc.
//

pub mod config;
pub mod password;

/// Predefined username for the system user.
pub const SYSTEM_USER: &str = "system";

/// Predefined username for the admin user.
pub const ADMIN_USER: &str = "admin";

/// Predefined role for the system admin role.
pub const SYS_ADMIN_ROLE: &str = "sys_admin";

/// Predefined role for the default role (auto-assigned to every user).
pub const DEFAULT_ROLE: &str = "default";

/// Default IDs:
///
/// - System: 00000000000000000000000000 (BASE32HEX_NOPAD: 00000000000000000000000000)
pub const ID_SYSTEM: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
pub const ENCODED_ID_SYSTEM: &str = "00000000000000000000000000";

///- Users:
///    - admin: 00000000000000000000000001 (BASE32HEX_NOPAD: 00000000000000000000000004)
pub const ID_USER_ADMIN: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
pub const ENCODED_ID_USER_ADMIN: &str = "00000000000000000000000004";

///- Roles:
///    - sys_admin: 00000000000000000000000002 (BASE32HEX_NOPAD: 00000000000000000000000008)
pub const ID_ROLE_SYS_ADMIN: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];
pub const ENCODED_ID_ROLE_SYS_ADMIN: &str = "00000000000000000000000008";
///    - default: 00000000000000000000000004 (BASE32HEX_NOPAD: 0000000000000000000000000G)
pub const ID_ROLE_DEFAULT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4];
pub const ENCODED_ID_ROLE_DEFAULT: &str = "0000000000000000000000000G";

///- Users Roles:
///    - admin/sys_admin: 00000000000000000000000005 (BASE32HEX_NOPAD: 0000000000000000000000000K)
pub const ID_USER_ROLE_ADMIN_SYS_ADMIN: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5];
pub const ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN: &str = "0000000000000000000000000K";
///    - admin/default: 00000000000000000000000006 (BASE32HEX_NOPAD: 0000000000000000000000000O)
pub const ID_USER_ROLE_ADMIN_DEFAULT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6];
pub const ENCODED_ID_USER_ROLE_ADMIN_DEFAULT: &str = "0000000000000000000000000O";

///- Groups:
///    - default: 00000000000000000000000022 (BASE32HEX_NOPAD: 00000000000000000000000208)
pub const ID_GROUP_DEFAULT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2];
pub const ENCODED_ID_GROUP_DEFAULT: &str = "00000000000000000000000208";
///    - source: 00000000000000000000000023 (BASE32HEX_NOPAD: 0000000000000000000000020C)
pub const ID_GROUP_SOURCE: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3];
pub const ENCODED_ID_GROUP_SOURCE: &str = "0000000000000000000000020C";
///    - destination: 00000000000000000000000024 (BASE32HEX_NOPAD: 0000000000000000000000020G)
pub const ID_GROUP_DESTINATION: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4];
pub const ENCODED_ID_GROUP_DESTINATION: &str = "0000000000000000000000020G";

/// Predefined group names.
pub const DEFAULT_GROUP: &str = "default";
pub const SOURCE_GROUP: &str = "source";
pub const DESTINATION_GROUP: &str = "destination";

///- Permissions (the five rows the bootstrap seeds):
///    - sys_admin / manage_access on system: (BASE32HEX_NOPAD: 00000000000000000000000010)
pub const ID_PERM_SYS_ADMIN_MANAGE_ACCESS: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8];
pub const ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS: &str = "00000000000000000000000010";
///    - sys_admin / create_projects on system: (BASE32HEX_NOPAD: 00000000000000000000000014)
pub const ID_PERM_SYS_ADMIN_CREATE_PROJECTS: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9];
pub const ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS: &str = "00000000000000000000000014";
///    - sys_admin / role_mgnt on system: (BASE32HEX_NOPAD: 00000000000000000000000100)
pub const ID_PERM_SYS_ADMIN_ROLE_MGNT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0];
pub const ENCODED_ID_PERM_SYS_ADMIN_ROLE_MGNT: &str = "00000000000000000000000100";
///    - sys_admin / manage_project on project:__all__: (BASE32HEX_NOPAD: 00000000000000000000000104)
pub const ID_PERM_SYS_ADMIN_MANAGE_PROJECT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1];
pub const ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT: &str = "00000000000000000000000104";
///    - default / create_projects on system: (BASE32HEX_NOPAD: 00000000000000000000000108)
pub const ID_PERM_DEFAULT_CREATE_PROJECTS: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2];
pub const ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS: &str = "00000000000000000000000108";
///    - reserved `__all__` entity id: 00000000000000000000000021 (BASE32HEX_NOPAD: 00000000000000000000000204)
pub const ID_ALL_ENTITIES: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1];
pub const ENCODED_ID_ALL_ENTITIES: &str = "00000000000000000000000204";

/// Id of the `sandbox` project — the one project the bootstrap seeds. Further
/// projects are created through the projects CRUD API. On PostgreSQL the
/// per-table `project_id` default reads the `app.project_id` session GUC
/// (`NULLIF(current_setting('app.project_id', true), '')`), which is NULL when
/// the GUC is unset and so fails the NOT NULL constraint — fail-closed, with NO
/// sandbox fallback. `sandbox` is an ordinary, deletable project: the system
/// stamps it explicitly only where it has no request GUC (e.g. bootstrap
/// seeds), and it is the single project the app operates in on SQLite/MySQL
/// (which have no row-level security).
///
/// Bytes 22..=24 are taken by the seeded groups (default/source/destination),
/// so this is byte 25.
pub const ID_PROJECT_SANDBOX: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 5];
pub const ENCODED_ID_PROJECT_SANDBOX: &str = "0000000000000000000000020K";

pub const DEFAULT_IDS: [[u8; 16]; 13] = [
    ID_SYSTEM,
    ID_USER_ADMIN,
    ID_ROLE_SYS_ADMIN,
    ID_ROLE_DEFAULT,
    ID_USER_ROLE_ADMIN_SYS_ADMIN,
    ID_USER_ROLE_ADMIN_DEFAULT,
    ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
    ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
    ID_PERM_SYS_ADMIN_ROLE_MGNT,
    ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
    ID_PERM_DEFAULT_CREATE_PROJECTS,
    ID_ALL_ENTITIES,
    ID_PROJECT_SANDBOX,
];

pub const DEFAULT_ENCODED_IDS: [&str; 13] = [
    ENCODED_ID_SYSTEM,
    ENCODED_ID_USER_ADMIN,
    ENCODED_ID_ROLE_SYS_ADMIN,
    ENCODED_ID_ROLE_DEFAULT,
    ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
    ENCODED_ID_USER_ROLE_ADMIN_DEFAULT,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
    ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
    ENCODED_ID_PERM_SYS_ADMIN_ROLE_MGNT,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
    ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS,
    ENCODED_ID_ALL_ENTITIES,
    ENCODED_ID_PROJECT_SANDBOX,
];
