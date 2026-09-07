//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::table::TableDBWithNames;
use crate::parse::{DATA_LOCATION_REGEX, NAME_PATTERN, UNDERSCORE_NAME_PATTERN, parse_email};
use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use td_error::{ApiError, api_error};
use td_security::{
    ADMIN_USER, DEFAULT_GROUP, DEFAULT_ROLE, DESTINATION_GROUP, PROJECT_CREATOR_ROLE, SOURCE_GROUP,
    SYS_ADMIN_ROLE, SYSTEM_USER,
};

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(secret)]
pub struct AccessToken(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = "<unavailable>")]
pub struct BuildManifest(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct BundleHash(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters", try_from = ToCollectionName)]
pub struct CollectionName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct ProjectName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct Connector(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(parser = CronMask::parse_cron)]
pub struct CronMask(String);

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

/// Logical storage prefix for an entity's artifacts. The physical backend
/// (s3/az/gcs/local) is selected by mounts matching this prefix. Mandatory on
/// collections (default `/`), an optional override on function versions, and
/// stamped (frozen) on physical table data versions and bundles at creation.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(regex = DATA_LOCATION_REGEX, default = "/")]
pub struct BasePath(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min_len = 0, max_len = 200, default = "")]
pub struct Description(String);

/// Fully-qualified Conn class of a connection (`<module>:<qualname>`, e.g.
/// `tabsdatak.conn.mysql:MySqlConn`). Stored on the connection row so the
/// connector type cannot change across upserts, and matched against a
/// publisher/subscriber function's own Conn class at register/update.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(min_len = 1, max_len = 200)]
pub struct ConnectionKind(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct Dot(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(parser = parse_email)]
pub struct Email(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct EntityName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct EnvPrefix(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct ExecutionError(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(max_len = 100)]
pub struct ExecutionName(String);

/// Caller-supplied idempotency key for a trigger. When set, an execution is
/// deduplicated per function on `(function_id, external_uid)`.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(min_len = 1, max_len = 64)]
pub struct ExternalUid(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct FullName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct FunctionName(String);

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(secret, max_len = 4096, default = "{}")]
pub struct FunctionRuntimeValues(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct LikeFilter(String);

const MIN_PASSWORD_LEN: usize = 8;
const MAX_PASSWORD_LEN: usize = 64;

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(secret, min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN)]
pub struct NewPassword(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(secret, min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN)]
pub struct OldPassword(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(min_len = 1, max_len = 1024)]
pub struct Partition(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct PartitionName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct PartitionFileName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(secret, min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN)]
pub struct Password(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(secret)]
pub struct PasswordHash(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct PythonVersion(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(secret)]
pub struct RefreshToken(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct RoleName(String);

impl RoleName {
    pub fn sys_admin() -> Self {
        Self(SYS_ADMIN_ROLE.to_string())
    }

    pub fn default_role() -> Self {
        Self(DEFAULT_ROLE.to_string())
    }

    pub fn project_creator() -> Self {
        Self(PROJECT_CREATOR_ROLE.to_string())
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct GroupName(String);

impl GroupName {
    pub fn default_group() -> Self {
        Self(DEFAULT_GROUP.to_string())
    }

    pub fn source() -> Self {
        Self(SOURCE_GROUP.to_string())
    }

    pub fn destination() -> Self {
        Self(DESTINATION_GROUP.to_string())
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct SchemaFieldName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct SchemaFieldType(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct SchemaHash(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct Sql(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(try_from = TableNameDto)]
pub struct TableName(String);

impl TableName {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

impl TryFrom<&TableDBWithNames> for TableName {
    type Error = td_error::TdError;

    fn try_from(v: &TableDBWithNames) -> Result<Self, Self::Error> {
        let table = v.base.name.clone();
        Ok(table)
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(regex = UNDERSCORE_NAME_PATTERN, expected = "a [_A-Za-z0-9] word of up to 100 characters")]
pub struct TableNameDto(String);

impl TableNameDto {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TabsdataVersion(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct ToCollectionName(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
pub struct TokenType(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(default = "F")]
pub struct TransactionByStr(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TransactionKey(String);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(regex = NAME_PATTERN, expected = "a [A-Za-z0-9] word of up to 100 characters")]
pub struct UserName(String);

impl UserName {
    pub fn admin() -> Self {
        Self(ADMIN_USER.to_string())
    }

    pub fn system() -> Self {
        Self(SYSTEM_USER.to_string())
    }
}

// JSON blob with config for the workers.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(default = "{}", parser = WorkerConfig::parse_json)]
pub struct WorkerConfig(String);

impl WorkerConfig {
    fn parse_json(value: String) -> Result<String, td_error::TdError> {
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

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
#[typed(min_len = 1, max_len = 10, default = "V1")]
pub struct WorkerVersion(String);

#[cfg(test)]
mod tests {
    use super::*;

    // BasePath is validated by DATA_LOCATION_REGEX:
    //   ^(/|(/[a-zA-Z][a-zA-Z0-9_]{0,99})*)$
    // i.e. a lone "/" or one-or-more "/<identifier>" segments, where each
    // identifier starts with a letter and continues with letters/digits/'_'.

    #[test]
    fn test_base_path_accepts_valid() {
        for valid in [
            "",         // empty: matches the zero-repetition branch of (/...)*
            "/",        // root
            "/abc",     // single segment
            "/abc/def", // nested path
            "/A1_b/c2", // letters, digits and underscores
        ] {
            assert!(
                BasePath::try_from(valid).is_ok(),
                "expected '{valid}' to be a valid BasePath"
            );
        }
    }

    #[test]
    fn test_base_path_rejects_invalid() {
        for invalid in [
            "abc",   // missing leading slash
            "/abc/", // trailing slash
            "/1abc", // segment starting with a digit
            "/ab-c", // illegal character in segment
            "//abc", // empty segment
        ] {
            assert!(
                BasePath::try_from(invalid).is_err(),
                "expected '{invalid}' to be an invalid BasePath"
            );
        }
    }

    #[test]
    fn test_base_path_default_is_root() {
        assert_eq!(BasePath::default().0, "/");
    }
}
