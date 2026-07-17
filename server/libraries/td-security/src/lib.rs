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

/// Predefined role conferring exactly the `create_projects` system permission.
/// Membership target for the Simple-mode "grant create_projects to a user" facade.
pub const PROJECT_CREATOR_ROLE: &str = "project_creator";

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
///    - default: 00000000000000000000000004 (BASE32HEX_NOPAD: 0000000000000000000000000g)
pub const ID_ROLE_DEFAULT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4];
pub const ENCODED_ID_ROLE_DEFAULT: &str = "0000000000000000000000000g";

///- Users Roles:
///    - admin/sys_admin: 00000000000000000000000005 (BASE32HEX_NOPAD: 0000000000000000000000000k)
pub const ID_USER_ROLE_ADMIN_SYS_ADMIN: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5];
pub const ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN: &str = "0000000000000000000000000k";
///    - admin/default: 00000000000000000000000006 (BASE32HEX_NOPAD: 0000000000000000000000000o)
pub const ID_USER_ROLE_ADMIN_DEFAULT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6];
pub const ENCODED_ID_USER_ROLE_ADMIN_DEFAULT: &str = "0000000000000000000000000o";

///- Groups:
///    - default: 00000000000000000000000022 (BASE32HEX_NOPAD: 00000000000000000000000208)
pub const ID_GROUP_DEFAULT: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2];
pub const ENCODED_ID_GROUP_DEFAULT: &str = "00000000000000000000000208";
///    - source: 00000000000000000000000023 (BASE32HEX_NOPAD: 0000000000000000000000020c)
pub const ID_GROUP_SOURCE: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3];
pub const ENCODED_ID_GROUP_SOURCE: &str = "0000000000000000000000020c";
///    - destination: 00000000000000000000000024 (BASE32HEX_NOPAD: 0000000000000000000000020g)
pub const ID_GROUP_DESTINATION: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4];
pub const ENCODED_ID_GROUP_DESTINATION: &str = "0000000000000000000000020g";

/// Predefined group names.
pub const DEFAULT_GROUP: &str = "default";
pub const SOURCE_GROUP: &str = "sources";
pub const DESTINATION_GROUP: &str = "destinations";

/// The per-project system groups as `(encoded id, name, description)` — the
/// single source every seeder (schema bootstrap, project create, test utils)
/// derives from. Group ids are unique per project, so these fixed ids are
/// minted in every project.
pub const SYSTEM_GROUPS: [(&str, &str, &str); 3] = [
    (
        ENCODED_ID_GROUP_DEFAULT,
        DEFAULT_GROUP,
        "Default group — assigned to collections that do not specify a group.",
    ),
    (
        ENCODED_ID_GROUP_SOURCE,
        SOURCE_GROUP,
        "Sources group — assigned to ingestion collections.",
    ),
    (
        ENCODED_ID_GROUP_DESTINATION,
        DESTINATION_GROUP,
        "Destinations group — assigned to egestion collections.",
    ),
];

///- Permissions (the five rows the bootstrap seeds):
///    - sys_admin / manage_access on system: (BASE32HEX_NOPAD: 00000000000000000000000010)
pub const ID_PERM_SYS_ADMIN_MANAGE_ACCESS: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8];
pub const ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS: &str = "00000000000000000000000010";
///    - sys_admin / create_projects on system: (BASE32HEX_NOPAD: 00000000000000000000000014)
pub const ID_PERM_SYS_ADMIN_CREATE_PROJECTS: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9];
pub const ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS: &str = "00000000000000000000000014";
///    - sys_admin / manage_roles on system: (BASE32HEX_NOPAD: 00000000000000000000000100)
pub const ID_PERM_SYS_ADMIN_MANAGE_ROLES: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0];
pub const ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES: &str = "00000000000000000000000100";
///    - sys_admin / manage_project on project:__all__: (BASE32HEX_NOPAD: 00000000000000000000000104)
pub const ID_PERM_SYS_ADMIN_MANAGE_PROJECT: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1];
pub const ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT: &str = "00000000000000000000000104";
///    - default / create_projects on system: (BASE32HEX_NOPAD: 00000000000000000000000108)
pub const ID_PERM_DEFAULT_CREATE_PROJECTS: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2];
pub const ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS: &str = "00000000000000000000000108";
///    - reserved `__all__` entity id: 00000000000000000000000021 (BASE32HEX_NOPAD: 00000000000000000000000204)
pub const ID_ALL_ENTITIES: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1];
pub const ENCODED_ID_ALL_ENTITIES: &str = "00000000000000000000000204";

///- project_creator role: confers exactly `create_projects` on system.
///  Byte 0x26 (0x25 is unused; bytes 22..=24 are the seeded groups).
pub const ID_ROLE_PROJECT_CREATOR: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 6];
pub const ENCODED_ID_ROLE_PROJECT_CREATOR: &str = "0000000000000000000000020o";
///- project_creator / create_projects on system permission row: byte 0x27 = "...20s".
pub const ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 7];
pub const ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS: &str = "0000000000000000000000020s";

pub const DEFAULT_IDS: [[u8; 16]; 14] = [
    ID_SYSTEM,
    ID_USER_ADMIN,
    ID_ROLE_SYS_ADMIN,
    ID_ROLE_DEFAULT,
    ID_USER_ROLE_ADMIN_SYS_ADMIN,
    ID_USER_ROLE_ADMIN_DEFAULT,
    ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
    ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
    ID_PERM_SYS_ADMIN_MANAGE_ROLES,
    ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
    ID_PERM_DEFAULT_CREATE_PROJECTS,
    ID_ALL_ENTITIES,
    ID_ROLE_PROJECT_CREATOR,
    ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS,
];

pub const DEFAULT_ENCODED_IDS: [&str; 14] = [
    ENCODED_ID_SYSTEM,
    ENCODED_ID_USER_ADMIN,
    ENCODED_ID_ROLE_SYS_ADMIN,
    ENCODED_ID_ROLE_DEFAULT,
    ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
    ENCODED_ID_USER_ROLE_ADMIN_DEFAULT,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
    ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
    ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS,
    ENCODED_ID_ALL_ENTITIES,
    ENCODED_ID_ROLE_PROJECT_CREATOR,
    ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS,
];
