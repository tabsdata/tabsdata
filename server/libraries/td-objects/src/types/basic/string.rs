//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::table::TableDBWithNames;
use crate::parse::{
    DATA_LOCATION_REGEX, parse_collection, parse_email, parse_entity, parse_execution,
    parse_function, parse_role, parse_table, parse_user,
};
use std::fmt::Debug;
use td_error::{ApiError, api_error};
use td_security::{ADMIN_USER, SEC_ADMIN_ROLE, SYS_ADMIN_ROLE, USER_ROLE};

#[ta_objects::typed(string(secret))]
pub struct AccessToken;

#[ta_objects::typed(string(default = "<unavailable>"))]
pub struct BuildManifest;

#[ta_objects::typed(string)]
pub struct BundleHash;

#[ta_objects::typed(string(parser = parse_collection), try_from = ToCollectionName)]
#[derive(Eq, PartialEq, Hash)]
pub struct CollectionName;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq)]
pub struct Connector;

#[ta_objects::typed(string(parser = CronMask::parse_cron))]
pub struct CronMask;

impl CronMask {
    fn create_cron(s: String) -> Result<croner::Cron, td_error::TdError> {
        if s.contains("@") {
            return Err(api_error!(
                ApiError::InputError,
                "Invalid cron expression: '@' aliases not supported"
            ));
        }
        // We do not allow seconds precision in the parsed cron masks.
        let parser = croner::parser::CronParser::builder()
            .alternative_weekdays(false)
            .seconds(croner::parser::Seconds::Disallowed)
            .build();
        let cron = parser
            .parse(&s)
            .map_err(|e| api_error!(ApiError::InputError, "Invalid cron expression: {e}"))?;
        Ok(cron)
    }

    fn parse_cron(s: String) -> Result<String, td_error::TdError> {
        let cron = Self::create_cron(s)?;
        Ok(cron.to_string())
    }

    pub fn into_cron(self) -> Result<croner::Cron, td_error::TdError> {
        let cron = Self::create_cron(self.0)?;
        Ok(cron)
    }
}

#[ta_objects::typed(string(regex = DATA_LOCATION_REGEX, default = "/"))]
#[derive(Eq, PartialEq, Hash)]
pub struct DataLocation;

#[ta_objects::typed(string(min_len = 0, max_len = 200, default = ""))]
#[derive(Eq, PartialEq)]
pub struct Description;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq)]
pub struct Dot;

#[ta_objects::typed(string(parser = parse_email))]
#[derive(Eq, PartialEq)]
pub struct Email;

#[ta_objects::typed(string(parser = parse_entity))]
pub struct EntityName;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq, Hash)]
pub struct EnvPrefix;

#[ta_objects::typed(string)]
pub struct ExecutionError;

#[ta_objects::typed(string(parser = parse_execution))]
#[derive(Eq, PartialEq)]
pub struct ExecutionName;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq)]
pub struct FullName;

#[ta_objects::typed(string(parser = parse_function))]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FunctionName;

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[ta_objects::typed(string(secret, max_len = 4096, default = "{}"))]
#[derive(Eq, PartialEq)]
pub struct FunctionRuntimeValues;

#[ta_objects::typed(string)]
pub struct LikeFilter;

const MIN_PASSWORD_LEN: usize = 8;
const MAX_PASSWORD_LEN: usize = 64;

#[ta_objects::typed(string(secret, min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct NewPassword;

#[ta_objects::typed(string(secret, min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct OldPassword;

#[ta_objects::typed(string(min_len = 1, max_len = 1024))]
pub struct Partition;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq, Hash)]
pub struct PartitionName;

#[ta_objects::typed(string)]
pub struct PartitionFileName;

#[ta_objects::typed(string(secret, min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct Password;

#[ta_objects::typed(string(secret))]
#[derive(Eq, PartialEq)]
pub struct PasswordHash;

#[ta_objects::typed(string)]
pub struct PythonVersion;

#[ta_objects::typed(string(secret))]
pub struct RefreshToken;

#[ta_objects::typed(string(parser = parse_role))]
#[derive(Eq, PartialEq)]
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

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq)]
pub struct SchemaFieldName;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq)]
pub struct SchemaFieldType;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq, Hash)]
pub struct SchemaHash;

#[ta_objects::typed(string(min_len = 0, max_len = 4096))]
#[derive(Eq, PartialEq)]
pub struct Snippet;

#[ta_objects::typed(string)]
pub struct Sql;

#[ta_objects::typed(string, try_from = TableNameDto)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TableName;

impl TableName {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

impl TryFrom<&TableDBWithNames> for TableName {
    type Error = td_error::TdError;

    fn try_from(v: &TableDBWithNames) -> Result<Self, Self::Error> {
        let table = v.name.clone();
        Ok(table)
    }
}

#[ta_objects::typed(string(parser = parse_table))]
#[derive(Eq, PartialEq, Hash)]
pub struct TableNameDto;

impl TableNameDto {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

#[ta_objects::typed(string)]
pub struct TabsdataVersion;

#[ta_objects::typed(string(parser = parse_collection))]
pub struct ToCollectionName;

#[ta_objects::typed(string)]
pub struct TokenType;

#[ta_objects::typed(string(default = "F"))]
pub struct TransactionByStr;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TransactionKey;

#[ta_objects::typed(string(parser = parse_user))]
#[derive(Eq, PartialEq, Hash)]
pub struct UserName;

impl UserName {
    pub fn admin() -> Self {
        Self(ADMIN_USER.to_string())
    }
}

// JSON blob with config for the workers.
#[ta_objects::typed(string(default = "{}", parser = WorkerConfig::parse_json))]
#[derive(Eq, PartialEq)]
pub struct WorkerConfig;

impl WorkerConfig {
    fn parse_json(value: String) -> Result<String, td_error::TdError> {
        // validate value is a valid json string
        serde_json::from_str::<serde_json::Value>(&value).map_err(|e| {
            api_error!(
                ApiError::InternalError,
                "Could not parse worker config as json. Value: {value}. Error: {e}"
            )
        })?;
        Ok(value)
    }

    pub fn serialize<T>(value: T) -> Result<Self, td_error::TdError>
    where
        T: serde::Serialize,
    {
        Ok(Self(serde_json::to_string(&value).map_err(|e| {
            api_error!(
                ApiError::InternalError,
                "Could not serialize worker config as json: {e}"
            )
        })?))
    }
}

#[ta_objects::typed(string(min_len = 1, max_len = 10, default = "V1"))]
pub struct WorkerVersion;
