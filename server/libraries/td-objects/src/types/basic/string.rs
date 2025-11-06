//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::table::TableDBRead;
use crate::parse::{
    DATA_LOCATION_REGEX, parse_collection, parse_email, parse_entity, parse_execution,
    parse_function, parse_role, parse_table, parse_user,
};
use std::fmt::Debug;
use td_security::{ADMIN_USER, SEC_ADMIN_ROLE, SYS_ADMIN_ROLE, USER_ROLE};

#[td_type::typed(string)]
pub struct AccessToken;

#[td_type::typed(string(default = "<unavailable>"))]
pub struct BuildManifest;

#[td_type::typed(string)]
pub struct BundleHash;

#[td_type::typed(string(parser = parse_collection), try_from = ToCollectionName)]
pub struct CollectionName;

#[td_type::typed(string)]
pub struct Connector;

#[td_type::typed(string(regex = DATA_LOCATION_REGEX, default = "/"))]
pub struct DataLocation;

#[td_type::typed(string(min_len = 0, max_len = 200, default = ""))]
pub struct Description;

#[td_type::typed(string)]
pub struct Dot;

#[td_type::typed(string(parser = parse_email))]
pub struct Email;

#[td_type::typed(string(parser = parse_entity))]
pub struct EntityName;

#[td_type::typed(string)]
pub struct ExecutionError;

#[td_type::typed(string(parser = parse_execution))]
pub struct ExecutionName;

#[td_type::typed(string)]
pub struct FullName;

#[td_type::typed(string(parser = parse_function))]
pub struct FunctionName;

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[td_type::typed(string(max_len = 4096, default = "{}"))]
pub struct FunctionRuntimeValues;

#[td_type::typed(string)]
pub struct LikeFilter;

const MIN_PASSWORD_LEN: usize = 8;
const MAX_PASSWORD_LEN: usize = 64;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct NewPassword;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct OldPassword;

#[td_type::typed(string(min_len = 1, max_len = 1024))]
pub struct Partition;

#[td_type::typed(string)]
pub struct PartitionName;

#[td_type::typed(string)]
pub struct PartitionFileName;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct Password;

#[td_type::typed(string)]
pub struct PasswordHash;

#[td_type::typed(string)]
pub struct PythonVersion;

#[td_type::typed(string)]
pub struct RefreshToken;

#[td_type::typed(string(parser = parse_role))]
pub struct RoleName;

impl RoleName {
    pub fn sys_admin() -> Self {
        Self(SYS_ADMIN_ROLE.to_string())
    }

    pub fn sec_admin() -> Self {
        Self(SEC_ADMIN_ROLE.to_string())
    }

    pub fn user() -> Self {
        Self(USER_ROLE.to_string())
    }
}

#[td_type::typed(string)]
pub struct SchemaFieldName;

#[td_type::typed(string)]
pub struct SchemaFieldType;

#[td_type::typed(string)]
pub struct SchemaHash;

#[td_type::typed(string(min_len = 0, max_len = 4096))]
pub struct Snippet;

#[td_type::typed(string)]
pub struct Sql;

#[td_type::typed(string(min_len = 1, max_len = 10, default = "V1"))]
pub struct StorageVersion;

#[td_type::typed(string, try_from = TableNameDto)]
pub struct TableName;

impl TableName {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

impl TryFrom<&TableDBRead> for TableName {
    type Error = td_error::TdError;

    fn try_from(v: &TableDBRead) -> Result<Self, Self::Error> {
        let table = v.name.clone();
        Ok(table)
    }
}

#[td_type::typed(string(parser = parse_table))]
pub struct TableNameDto;

impl TableNameDto {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

#[td_type::typed(string)]
pub struct TabsdataVersion;

#[td_type::typed(string(parser = parse_collection))]
pub struct ToCollectionName;

#[td_type::typed(string)]
pub struct TokenType;

#[td_type::typed(string(default = "F"))]
pub struct TransactionByStr;

#[td_type::typed(string)]
pub struct TransactionKey;

#[td_type::typed(string(parser = parse_user))]
pub struct UserName;

impl UserName {
    pub fn admin() -> Self {
        Self(ADMIN_USER.to_string())
    }
}
