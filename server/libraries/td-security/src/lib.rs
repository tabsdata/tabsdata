//
// Copyright 2025 Tabs Data Inc.
//

pub mod config;
pub mod password;

/// Predefined username for the admin user.
pub const ADMIN_USER: &str = "admin";

/// Predefined role for the system admin role.
pub const SYS_ADMIN_ROLE: &str = "sys_admin";

/// Predefined role for the security admin role.
pub const SEC_ADMIN_ROLE: &str = "sec_admin";

/// Predefined role for the user role.
pub const USER_ROLE: &str = "user";

macro_rules! padded_bytes {
    ($last:expr) => {
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, $last]
    };
}

macro_rules! padded_string {
    ($last:expr) => {
        concat!("0000000000000000000000000", $last)
    };
}

/// Default IDs:
///
/// - System: 00000000000000000000000000 (BASE32HEX_NOPAD: 00000000000000000000000000)
pub const ID_SYSTEM: [u8; 16] = padded_bytes!(0);
pub const ENCODED_ID_SYSTEM: &str = padded_string!("0");
///- Users:
///    - admin: 00000000000000000000000001 (BASE32HEX_NOPAD: 00000000000000000000000004)
pub const ID_USER_ADMIN: [u8; 16] = padded_bytes!(1);
pub const ENCODED_ID_USER_ADMIN: &str = padded_string!("4");
///- Roles:
///    - sys_admin: 00000000000000000000000002 (BASE32HEX_NOPAD: 00000000000000000000000008)
pub const ID_ROLE_SYS_ADMIN: [u8; 16] = padded_bytes!(2);
pub const ENCODED_ID_ROLE_SYS_ADMIN: &str = padded_string!("8");
///    - sec_admin: 00000000000000000000000003 (BASE32HEX_NOPAD: 0000000000000000000000000C)
pub const ID_ROLE_SEC_ADMIN: [u8; 16] = padded_bytes!(3);
pub const ENCODED_ID_ROLE_SEC_ADMIN: &str = padded_string!("C");
///    - user: 00000000000000000000000004 (BASE32HEX_NOPAD: 0000000000000000000000000G)
pub const ID_ROLE_USER: [u8; 16] = padded_bytes!(4);
pub const ENCODED_ID_ROLE_USER: &str = padded_string!("G");
///- Users Roles:
///    - admin/sys_admin: 00000000000000000000000005 (BASE32HEX_NOPAD: 0000000000000000000000000K)
pub const ID_USER_ROLE_ADMIN_SYS_ADMIN: [u8; 16] = padded_bytes!(5);
pub const ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN: &str = padded_string!("K");
///    - admin/sec_admin: 00000000000000000000000006 (BASE32HEX_NOPAD: 0000000000000000000000000O)
pub const ID_USER_ROLE_ADMIN_SEC_ADMIN: [u8; 16] = padded_bytes!(6);
pub const ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN: &str = padded_string!("O");
///    - admin/user: 00000000000000000000000007 (BASE32HEX_NOPAD: 0000000000000000000000000S)
pub const ID_USER_ROLE_ADMIN_USER: [u8; 16] = padded_bytes!(7);
pub const ENCODED_ID_USER_ROLE_ADMIN_USER: &str = padded_string!("S");

pub const DEFAULT_IDS: [[u8; 16]; 8] = [
    ID_SYSTEM,
    ID_USER_ADMIN,
    ID_ROLE_SYS_ADMIN,
    ID_ROLE_SEC_ADMIN,
    ID_ROLE_USER,
    ID_USER_ROLE_ADMIN_SYS_ADMIN,
    ID_USER_ROLE_ADMIN_SEC_ADMIN,
    ID_USER_ROLE_ADMIN_USER,
];

pub const DEFAULT_ENCODED_IDS: [&str; 8] = [
    ENCODED_ID_SYSTEM,
    ENCODED_ID_USER_ADMIN,
    ENCODED_ID_ROLE_SYS_ADMIN,
    ENCODED_ID_ROLE_SEC_ADMIN,
    ENCODED_ID_ROLE_USER,
    ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
    ENCODED_ID_USER_ROLE_ADMIN_SEC_ADMIN,
    ENCODED_ID_USER_ROLE_ADMIN_USER,
];
