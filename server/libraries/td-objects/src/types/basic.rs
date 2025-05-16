//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::dependency::DependencyVersionDBWithNames;
use crate::types::parse::{
    parse_collection, parse_email, parse_entity, parse_execution, parse_function, parse_role,
    parse_table, parse_user, DATA_LOCATION_REGEX,
};
use crate::types::table::TableVersionDBWithNames;
use crate::types::table_ref::{TableRef, VersionedTableRef, Versions};
use crate::types::trigger::TriggerVersionDBWithNames;
use td_common::id::Id;
use td_error::TdError;
use td_security::{
    ADMIN_USER, ID_ROLE_SEC_ADMIN, ID_ROLE_SYS_ADMIN, ID_ROLE_USER, ID_USER_ADMIN, SEC_ADMIN_ROLE,
    SYS_ADMIN_ROLE, USER_ROLE,
};

#[td_type::typed(string)]
pub struct AccessToken;

#[td_type::typed(i64)]
pub struct AccessTokenExpiration;

#[td_type::typed(id)]
pub struct AccessTokenId;

#[td_type::typed_enum(skip_serde)]
pub enum AtMulti {
    Time(AtTime),
    Trx(TransactionId),
    Exec(ExecutionId),
    Data(DataVersionId),
}

impl AtMulti {
    pub fn time(&self) -> Option<AtTime> {
        match self {
            AtMulti::Time(time) => Some(time.clone()),
            _ => None,
        }
    }
    pub fn trx(&self) -> Option<TransactionId> {
        match self {
            AtMulti::Trx(trx) => Some(*trx),
            _ => None,
        }
    }
    pub fn exec(&self) -> Option<ExecutionId> {
        match self {
            AtMulti::Exec(exec) => Some(*exec),
            _ => None,
        }
    }
    pub fn data(&self) -> Option<DataVersionId> {
        match self {
            AtMulti::Data(data) => Some(*data),
            _ => None,
        }
    }
}

impl Default for AtMulti {
    fn default() -> Self {
        AtMulti::Time(Default::default())
    }
}

static AT_MULTI_TIME: &str = "time:";
static AT_MULTI_TRX: &str = "trx:";
static AT_MULTI_EXEC: &str = "exec:";
static AT_MULTI_DATA: &str = "data:";

impl<'de> serde::Deserialize<'de> for AtMulti {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if let Some(stripped) = s.strip_prefix(AT_MULTI_TIME) {
            return Ok(AtMulti::Time(
                AtTime::try_from(stripped).map_err(serde::de::Error::custom)?,
            ));
        }
        if let Some(stripped) = s.strip_prefix(AT_MULTI_TRX) {
            return Ok(AtMulti::Trx(
                TransactionId::try_from(stripped).map_err(serde::de::Error::custom)?,
            ));
        }
        if let Some(stripped) = s.strip_prefix(AT_MULTI_EXEC) {
            return Ok(AtMulti::Exec(
                ExecutionId::try_from(stripped).map_err(serde::de::Error::custom)?,
            ));
        }
        if let Some(stripped) = s.strip_prefix(AT_MULTI_DATA) {
            return Ok(AtMulti::Data(
                DataVersionId::try_from(stripped).map_err(serde::de::Error::custom)?,
            ));
        }
        Err(serde::de::Error::custom(format!(
            "Value '{}' with unknown prefix",
            s
        )))
    }
}

impl serde::Serialize for AtMulti {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            AtMulti::Time(ref val) => format!("time:{}", val.as_str_millis()).serialize(serializer),
            AtMulti::Trx(ref val) => format!("trx:{}", val).serialize(serializer),
            AtMulti::Exec(ref val) => format!("exec:{}", val).serialize(serializer),
            AtMulti::Data(ref val) => format!("data:{}", val).serialize(serializer),
        }
    }
}

#[td_type::typed(timestamp)]
pub struct AtTime;

#[td_type::typed(string)]
pub struct BundleHash;

#[td_type::typed(id)]
pub struct BundleId;

#[td_type::typed(id, try_from = EntityId, try_from = FromCollectionId, try_from = ToCollectionId)]
pub struct CollectionId;

#[td_type::typed(id_name(id = CollectionId, name = CollectionName))]
pub struct CollectionIdName;

#[td_type::typed(string(parser = parse_collection), try_from = ToCollectionName)]
pub struct CollectionName;

#[td_type::typed(bool)]
pub struct DataChanged;

#[td_type::typed(string(regex = DATA_LOCATION_REGEX, default = "/"))]
pub struct DataLocation;

#[td_type::typed(id)]
pub struct DataVersionId;

#[td_type::typed_enum]
pub enum Decorator {
    #[strum(to_string = "P")]
    Publisher,
    #[strum(to_string = "T")]
    Transformer,
    #[strum(to_string = "S")]
    Subscriber,
}

#[td_type::typed(id)]
pub struct DependencyId;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct DependencyPos;

#[td_type::typed_enum]
pub enum DependencyStatus {
    #[strum(to_string = "A")]
    Active,
    #[strum(to_string = "D")]
    Deleted,
}

#[td_type::typed(id)]
pub struct DependencyVersionId;

#[td_type::typed(string(min_len = 0, max_len = 200, default = ""))]
pub struct Description;

#[td_type::typed(string)]
pub struct Dot;

#[td_type::typed(string(parser = parse_email))]
pub struct Email;

#[td_type::typed(id, try_from = CollectionId)]
pub struct EntityId;

#[td_type::typed(string(parser = parse_entity))]
pub struct EntityName;

#[td_type::typed(string)]
pub struct ExecutionError;

#[td_type::typed(id)]
pub struct ExecutionId;

// ExecutionName is not UNIQUE nor NOT NULL. We cannot use it as a primary key to lookup.
#[td_type::typed(id_name(id = ExecutionId))]
pub struct ExecutionIdName;

#[td_type::typed(i16)]
pub struct ExecutionLimit;

#[td_type::typed(string(parser = parse_execution))]
pub struct ExecutionName;

#[td_type::typed(i16)]
pub struct ExecutionTry;

#[td_type::typed(bool(default = false))]
pub struct Fixed;

#[td_type::typed(bool)]
pub struct FixedRole;

#[td_type::typed(id, try_from = CollectionId)]
pub struct FromCollectionId;

#[td_type::typed(bool(default = false))]
pub struct Frozen;

#[td_type::typed(string)]
pub struct FullName;

#[td_type::typed(id)]
pub struct FunctionId;

#[td_type::typed(id_name(id = FunctionId, name = FunctionName))]
pub struct FunctionIdName;

#[td_type::typed(string(parser = parse_function))]
pub struct FunctionName;

#[td_type::typed(id)]
pub struct FunctionRunId;

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[td_type::typed(string(max_len = 4096, default = "{}"))]
pub struct FunctionRuntimeValues;

#[td_type::typed_enum]
pub enum FunctionStatus {
    #[strum(to_string = "A")]
    Active,
    #[strum(to_string = "F")]
    Frozen,
    #[strum(to_string = "D")]
    Deleted,
}

#[td_type::typed(id)]
pub struct FunctionVersionId;

#[td_type::typed(id_name(id = FunctionVersionId, name = FunctionName))]
pub struct FunctionVersionIdName;

#[td_type::typed_enum]
pub enum GrantType {
    #[strum(to_string = "refresh_token")]
    RefreshToken,
}

#[td_type::typed(bool(default = false))]
pub struct HasData;

#[td_type::typed(id)]
pub struct InterCollectionPermissionId;

#[td_type::typed(id_name(id = InterCollectionPermissionId))]
pub struct InterCollectionPermissionIdName;

const MIN_PASSWORD_LEN: usize = 8;
const MAX_PASSWORD_LEN: usize = 64;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct NewPassword;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct OldPassword;

#[td_type::typed(string(min_len = 1, max_len = 1024))]
pub struct Partition;

#[td_type::typed(bool)]
pub struct Partitioned;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct Password;

#[td_type::typed(timestamp, try_from = AtTime)]
pub struct PasswordChangeTime;

#[td_type::typed(string)]
pub struct PasswordHash;

#[td_type::typed(bool(default = false))]
pub struct PasswordMustChange;

#[td_type::typed_enum]
pub enum PermissionEntityType {
    #[strum(to_string = "s")]
    System,
    #[strum(to_string = "c")]
    Collection,
}

#[td_type::typed(id)]
pub struct PermissionId;

#[td_type::typed(id_name(id = PermissionId))]
pub struct PermissionIdName;

#[td_type::typed_enum]
pub enum PermissionType {
    #[strum(to_string = "sa")]
    SysAdmin,
    #[strum(to_string = "ss")]
    SecAdmin,
    #[strum(to_string = "ca")]
    CollectionAdmin,
    #[strum(to_string = "cd")]
    CollectionDev,
    #[strum(to_string = "cx")]
    CollectionExec,
    #[strum(to_string = "cr")]
    CollectionRead,
    #[strum(to_string = "cR")]
    CollectionReadAll,
}

impl PermissionType {
    pub fn on_entity_type(&self) -> PermissionEntityType {
        if matches!(self, &Self::SysAdmin | &Self::SecAdmin) {
            PermissionEntityType::System
        } else {
            PermissionEntityType::Collection
        }
    }
}

#[td_type::typed(bool)]
pub struct Private;

#[td_type::typed(timestamp)]
pub struct PublishedOn;

#[td_type::typed(string)]
pub struct RefreshToken;

#[td_type::typed(id)]
pub struct RefreshTokenId;

#[td_type::typed(id)]
pub struct RequirementId;

#[td_type::typed(bool)]
pub struct ReuseFrozen;

#[td_type::typed(id)]
pub struct RoleId;

impl RoleId {
    pub fn sys_admin() -> Self {
        Self(Id::_new(ID_ROLE_SYS_ADMIN))
    }

    pub fn sec_admin() -> Self {
        Self(Id::_new(ID_ROLE_SEC_ADMIN))
    }

    pub fn user() -> Self {
        Self(Id::_new(ID_ROLE_USER))
    }
}

#[td_type::typed(id_name(id = RoleId, name = RoleName))]
pub struct RoleIdName;

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

#[td_type::typed(i64(min = 0, default = 0))]
pub struct SampleOffset;

#[td_type::typed(i64(min = 0, max = 1000, default = 100))]
pub struct SampleLen;

#[td_type::typed(string)]
pub struct SchemaFieldName;

#[td_type::typed(string)]
pub struct SchemaFieldType;

#[td_type::typed(bool)]
pub struct SelfDependency;

#[td_type::typed(id)]
pub struct SessionId;

#[td_type::typed_enum]
pub enum SessionStatus {
    #[strum(to_string = "a")]
    Active,
    #[strum(to_string = "i_pc")]
    InvalidPasswordChange,
    #[strum(to_string = "i_nt")]
    InvalidNewToken,
    #[strum(to_string = "i_rc")]
    InvalidRoleChange,
    #[strum(to_string = "i_l")]
    InvalidLogout,
    #[strum(to_string = "i_ud")]
    InvalidUserDisabled,
}

#[td_type::typed(string(min_len = 0, max_len = 4096))]
pub struct Snippet;

#[td_type::typed(string(min_len = 1, max_len = 10, default = "V1"))]
pub struct StorageVersion;

#[td_type::typed(id)]
pub struct TableDataId;

#[td_type::typed(id)]
pub struct TableDataVersionId;

#[td_type::typed(composed(inner = VersionedTableRef))]
pub struct TableDependency;

impl TryFrom<&DependencyVersionDBWithNames> for TableDependency {
    type Error = TdError;

    fn try_from(v: &DependencyVersionDBWithNames) -> Result<Self, Self::Error> {
        let versions = &**v.table_versions();
        let table_dep = TableDependency::new(VersionedTableRef::new(
            Some(v.collection().clone()),
            v.table_name().clone(),
            versions.clone(),
        ));
        Ok(table_dep)
    }
}

#[td_type::typed(i32)]
pub struct TableFunctionParamPos;

#[td_type::typed(id)]
pub struct TableId;

#[td_type::typed(id_name(id = TableId, name = TableName))]
pub struct TableIdName;

#[td_type::typed(string(parser = parse_table))]
pub struct TableName;

impl TryFrom<&TableVersionDBWithNames> for TableName {
    type Error = TdError;

    fn try_from(v: &TableVersionDBWithNames) -> Result<Self, Self::Error> {
        let table = v.name().clone();
        Ok(table)
    }
}

#[td_type::typed_enum]
pub enum TableStatus {
    #[strum(to_string = "A")]
    Active,
    #[strum(to_string = "F")]
    Frozen,
    #[strum(to_string = "D")]
    Deleted,
}

#[td_type::typed(composed(inner = TableRef))]
pub struct TableTrigger;

impl TryFrom<&TriggerVersionDBWithNames> for TableTrigger {
    type Error = TdError;

    fn try_from(v: &TriggerVersionDBWithNames) -> Result<Self, Self::Error> {
        let table = TableTrigger::new(TableRef::new(
            Some(v.collection().clone()),
            v.trigger_by_table_name().clone(),
        ));
        Ok(table)
    }
}

#[td_type::typed(id)]
pub struct TableVersionId;

#[td_type::typed(composed(inner = Versions))]
pub struct TableVersions;

#[td_type::typed(id, try_from = CollectionId)]
pub struct ToCollectionId;

#[td_type::typed(string(parser = parse_collection))]
pub struct ToCollectionName;

#[td_type::typed(string)]
pub struct TokenType;

#[td_type::typed(string(default = "F"))]
pub struct TransactionByStr;

#[td_type::typed(id)]
pub struct TransactionId;

#[td_type::typed(id_name(id = TransactionId))]
pub struct TransactionIdName;

#[td_type::typed(string)]
pub struct TransactionKey;

#[td_type::typed_enum]
pub enum Trigger {
    #[strum(to_string = "M")]
    Manual,
    #[strum(to_string = "D")]
    Dependency,
}

#[td_type::typed(timestamp, try_from = AtTime)]
pub struct TriggeredOn;

#[td_type::typed(id)]
pub struct TriggerId;

#[td_type::typed_enum]
#[derive(Default)]
pub enum TriggerStatus {
    #[default]
    #[strum(to_string = "A")]
    Active,
    #[strum(to_string = "D")]
    Deleted,
}

#[td_type::typed(id)]
pub struct TriggerVersionId;

#[td_type::typed(bool(default = true))]
pub struct UserEnabled;

#[td_type::typed(id)]
pub struct UserId;

impl UserId {
    pub fn admin() -> Self {
        Self(Id::_new(ID_USER_ADMIN))
    }
}

#[td_type::typed(id_name(id = UserId, name = UserName))]
pub struct UserIdName;

#[td_type::typed(string(parser = parse_user))]
pub struct UserName;

impl UserName {
    pub fn admin() -> Self {
        Self(ADMIN_USER.to_string())
    }
}

#[td_type::typed(id)]
pub struct UserRoleId;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct VersionIdx;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct VersionPos;

#[td_type::typed(id)]
pub struct WorkerMessageId;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_at_multi_methods() {
        let at_multi = AtMulti::Time(AtTime::default());
        assert!(at_multi.time().is_some());
        assert!(at_multi.trx().is_none());
        assert!(at_multi.exec().is_none());
        assert!(at_multi.data().is_none());

        let at_multi_trx = AtMulti::Trx(TransactionId::default());
        assert!(at_multi_trx.time().is_none());
        assert!(at_multi_trx.trx().is_some());
        assert!(at_multi_trx.exec().is_none());
        assert!(at_multi_trx.data().is_none());

        let at_multi_exec = AtMulti::Exec(ExecutionId::default());
        assert!(at_multi_exec.time().is_none());
        assert!(at_multi_exec.trx().is_none());
        assert!(at_multi_exec.exec().is_some());
        assert!(at_multi_exec.data().is_none());

        let at_multi_data = AtMulti::Data(DataVersionId::default());
        assert!(at_multi_data.time().is_none());
        assert!(at_multi_data.trx().is_none());
        assert!(at_multi_data.exec().is_none());
        assert!(at_multi_data.data().is_some());
    }

    #[test]
    fn test_at_multi_serde() {
        let at_multi = AtMulti::Time(AtTime::default());
        let serialized = serde_json::to_string(&at_multi).unwrap();
        assert!(serialized.starts_with("\"time:"));

        let deserialized: AtMulti = serde_json::from_str(&serialized).unwrap();
        assert_eq!(at_multi, deserialized);

        let at_multi_trx = AtMulti::Trx(TransactionId::default());
        let serialized_trx = serde_json::to_string(&at_multi_trx).unwrap();
        assert!(serialized_trx.starts_with("\"trx:"));
        let deserialized_trx: AtMulti = serde_json::from_str(&serialized_trx).unwrap();
        assert_eq!(at_multi_trx, deserialized_trx);

        let at_multi_exec = AtMulti::Exec(ExecutionId::default());
        let serialized_exec = serde_json::to_string(&at_multi_exec).unwrap();
        assert!(serialized_exec.starts_with("\"exec:"));
        let deserialized_exec: AtMulti = serde_json::from_str(&serialized_exec).unwrap();
        assert_eq!(at_multi_exec, deserialized_exec);

        let at_multi_data = AtMulti::Data(DataVersionId::default());
        let serialized_data = serde_json::to_string(&at_multi_data).unwrap();
        assert!(serialized_data.starts_with("\"data:"));
        let deserialized_data: AtMulti = serde_json::from_str(&serialized_data).unwrap();
        assert_eq!(at_multi_data, deserialized_data);

        let invalid_str = "\"invalid_prefix:12345\"";
        serde_json::from_str::<AtMulti>(invalid_str).unwrap_err();
    }
}
