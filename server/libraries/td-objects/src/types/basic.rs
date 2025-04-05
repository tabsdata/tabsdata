//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::dependency::DependencyVersionDBWithNames;
use crate::types::parse::{
    parse_collection, parse_email, parse_entity, parse_function, parse_role, parse_table,
    parse_user, DATA_LOCATION_REGEX,
};
use crate::types::table::TableVersionDBWithNames;
use crate::types::table_ref::{TableRef, VersionedTableRef, Versions};
use crate::types::trigger::TriggerVersionDBWithNames;
use constcat::concat;
use td_common::id::Id;
use td_error::TdError;
use td_security::{ID_ROLE_SEC_ADMIN, ID_ROLE_SYS_ADMIN, ID_ROLE_USER, ID_USER_ADMIN, USER_ROLE};

#[td_type::typed(string)]
pub struct AccessToken;

#[td_type::typed(i64)]
pub struct AccessTokenExpiration;

#[td_type::typed(id)]
pub struct AccessTokenId;

#[td_type::typed(timestamp)]
pub struct AtTime;

#[td_type::typed(id)]
pub struct BundleId;

#[td_type::typed(id, try_from=EntityId)]
pub struct CollectionId;

#[td_type::typed(string(parser = parse_collection))]
pub struct CollectionName;

#[td_type::typed(id_name(id = CollectionId, name = CollectionName))]
pub struct CollectionIdName;

#[td_type::typed(string(regex = DATA_LOCATION_REGEX))]
pub struct DataLocation;

#[td_type::typed(string(min_len = 0, max_len = 200))]
pub struct Description;

#[td_type::typed(id)]
pub struct DependencyId;

#[td_type::typed(i16(min = 0, max = 200))]
pub struct DependencyPos;

#[td_type::typed(string(regex = DependencyStatus::REGEX))]
pub struct DependencyStatus;

impl DependencyStatus {
    const REGEX: &'static str = "^[AD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(id)]
pub struct DependencyVersionId;

#[td_type::typed(string(parser = parse_email))]
pub struct Email;

#[td_type::typed(id)]
pub struct EntityId;

#[td_type::typed(string(parser = parse_entity))]
pub struct EntityName;

#[td_type::typed(id)]
pub struct ExecutionPlanId;

#[td_type::typed(bool(default = false))]
pub struct Fixed;

#[td_type::typed(bool)]
pub struct FixedRole;

#[td_type::typed(bool(default = false))]
pub struct Frozen;

#[td_type::typed(bool)]
pub struct ReuseFrozen;

#[td_type::typed(bool)]
pub struct Private;

#[td_type::typed(id)]
pub struct FunctionId;

#[td_type::typed(string(parser = parse_function))]
pub struct FunctionName;

#[td_type::typed(id_name(id = FunctionId, name = FunctionName))]
pub struct FunctionIdName;

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[td_type::typed(string(max_len = 4096, default = "{}"))]
pub struct FunctionRuntimeValues;

#[td_type::typed(string(regex = FunctionStatus::REGEX, default = "A"))]
pub struct FunctionStatus;

impl FunctionStatus {
    const REGEX: &'static str = "^[AFD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn frozen() -> Self {
        Self("F".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(id)]
pub struct FunctionVersionId;

#[td_type::typed(id_name(id = FunctionVersionId, name = FunctionName))]
pub struct FunctionVersionIdName;

#[td_type::typed(string(regex = GRANT_TYPE_REGEX))]
pub struct GrantType;

const GRANT_TYPE_REGEX: &str = "^refresh_token$";

const MIN_PASSWORD_LEN: usize = 8;
const MAX_PASSWORD_LEN: usize = 64;
#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct NewPassword;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct OldPassword;

#[td_type::typed(string(min_len = 1, max_len = 1024))]
pub struct Partition;

#[td_type::typed(string(min_len = MIN_PASSWORD_LEN, max_len = MAX_PASSWORD_LEN))]
pub struct Password;

#[td_type::typed(timestamp, try_from = AtTime)]
pub struct PasswordChangeTime;

#[td_type::typed(string)]
pub struct PasswordHash;

#[td_type::typed(bool(default = false))]
pub struct PasswordMustChange;

#[td_type::typed(id)]
pub struct PermissionId;

#[td_type::typed(id_name(id = PermissionId))]
pub struct PermissionIdName;

#[td_type::typed(string(regex = PERMISSION_ENTITY_TYPE_REGEX))]
pub struct PermissionEntityType;

const PERMISSION_ENTITY_TYPE_REGEX: &str = concat!(
    "^(",
    PermissionEntityType::SYS,
    "|",
    PermissionEntityType::COLL,
    ")$"
);

impl PermissionEntityType {
    pub const SYS: &'static str = "s";
    pub const COLL: &'static str = "c";

    pub fn system() -> Self {
        Self(Self::SYS.to_string())
    }

    pub fn collection() -> Self {
        Self(Self::COLL.to_string())
    }
}

#[td_type::typed(string(regex = PERMISSION_TYPE_REGEX))]
pub struct PermissionType;

const PERMISSION_TYPE_REGEX: &str = concat!(
    "^(",
    PermissionType::SA,
    "|",
    PermissionType::SS,
    "|",
    PermissionType::CA,
    "|",
    PermissionType::CD,
    "|",
    PermissionType::CX,
    "|",
    PermissionType::CR,
    "|",
    PermissionType::CR_ALL,
    ")$"
);

impl PermissionType {
    pub const SA: &'static str = concat!(PermissionEntityType::SYS, "a");
    pub const SS: &'static str = concat!(PermissionEntityType::SYS, "s");
    pub const CA: &'static str = concat!(PermissionEntityType::COLL, "a");
    pub const CD: &'static str = concat!(PermissionEntityType::COLL, "d");
    pub const CX: &'static str = concat!(PermissionEntityType::COLL, "x");
    pub const CR: &'static str = concat!(PermissionEntityType::COLL, "r");
    pub const CR_ALL: &'static str = concat!(PermissionEntityType::COLL, "R");

    pub fn sys_admin() -> Self {
        Self(Self::SA.to_string())
    }

    pub fn sec_admin() -> Self {
        Self(Self::SS.to_string())
    }

    pub fn collection_admin() -> Self {
        Self(Self::CA.to_string())
    }

    pub fn collection_dev() -> Self {
        Self(Self::CD.to_string())
    }

    pub fn collection_exec() -> Self {
        Self(Self::CX.to_string())
    }

    pub fn collection_read() -> Self {
        Self(Self::CR.to_string())
    }

    pub fn collection_read_all() -> Self {
        Self(Self::CR_ALL.to_string())
    }

    pub fn on_entity_type(&self) -> PermissionEntityType {
        if self.0.starts_with("s") {
            PermissionEntityType::system()
        } else {
            PermissionEntityType::collection()
        }
    }
}

#[td_type::typed(timestamp)]
pub struct PublishedOn;

#[td_type::typed(string)]
pub struct RefreshToken;

#[td_type::typed(id)]
pub struct RefreshTokenId;

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
#[td_type::typed(string(parser = parse_role))]
pub struct RoleName;

impl RoleName {
    pub fn user() -> Self {
        Self(USER_ROLE.to_string())
    }
}

#[td_type::typed(id_name(id = RoleId, name = RoleName))]
pub struct RoleIdName;

#[td_type::typed(id)]
pub struct SessionId;

#[td_type::typed(string(regex = SESSION_INVALIDATION_REASON_REGEX))]
pub struct SessionStatus;

const SESSION_INVALIDATION_REASON_REGEX: &str = constcat::concat!(
    "^(",
    SessionStatus::ACTIVE,
    "|",
    SessionStatus::PASSW_RESET,
    "|",
    SessionStatus::INVALID_NEW_TOKEN,
    "|",
    SessionStatus::INVALID_ROLE_CHANGE,
    "|",
    SessionStatus::INVALID_LOGOUT,
    ")$"
);

impl SessionStatus {
    pub const ACTIVE: &'static str = "a";
    pub const PASSW_RESET: &'static str = "p";
    pub const INVALID_NEW_TOKEN: &'static str = "it";
    pub const INVALID_ROLE_CHANGE: &'static str = "ir";
    pub const INVALID_LOGOUT: &'static str = "il";

    pub fn active() -> Self {
        Self(Self::ACTIVE.to_string())
    }

    pub fn password_reset() -> Self {
        Self(Self::PASSW_RESET.to_string())
    }

    pub fn invalid_new_token() -> Self {
        Self(Self::INVALID_NEW_TOKEN.to_string())
    }

    pub fn invalid_role_change() -> Self {
        Self(Self::INVALID_ROLE_CHANGE.to_string())
    }

    pub fn invalid_logout() -> Self {
        Self(Self::INVALID_LOGOUT.to_string())
    }
}

#[td_type::typed(string(min_len = 0, max_len = 4096))]
pub struct Snippet;

#[td_type::typed(string(min_len = 1, max_len = 10))]
pub struct StorageVersion;

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

#[td_type::typed(id)]
pub struct TableDataId;

#[td_type::typed(string(regex = TableDataVersionStatus::REGEX))]
pub struct TableDataVersionStatus;

impl TableDataVersionStatus {
    const REGEX: &'static str = "^[SRDEFHCP]$";

    pub fn scheduled() -> Self {
        Self("S".to_string())
    }

    pub fn running() -> Self {
        Self("R".to_string())
    }

    pub fn done() -> Self {
        Self("D".to_string())
    }

    pub fn error() -> Self {
        Self("E".to_string())
    }

    pub fn failed() -> Self {
        Self("F".to_string())
    }

    pub fn hold() -> Self {
        Self("H".to_string())
    }

    pub fn canceled() -> Self {
        Self("C".to_string())
    }

    pub fn publish() -> Self {
        Self("P".to_string())
    }
}

#[td_type::typed(id)]
pub struct TableDataVersionId;

#[td_type::typed(id)]
pub struct TableId;

#[td_type::typed(string(parser = parse_table))]
pub struct TableName;

impl TryFrom<&TableVersionDBWithNames> for TableName {
    type Error = TdError;

    fn try_from(v: &TableVersionDBWithNames) -> Result<Self, Self::Error> {
        let table = v.name().clone();
        Ok(table)
    }
}

#[td_type::typed(id_name(id = TableId, name = TableName))]
pub struct TableIdName;

#[td_type::typed(i16)]
pub struct TableFunctionParamPos;

#[td_type::typed(string(regex = TableStatus::REGEX))]
pub struct TableStatus;

impl TableStatus {
    const REGEX: &'static str = "^[AFD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn frozen() -> Self {
        Self("F".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
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

#[td_type::typed(string)]
pub struct TokenType;

#[td_type::typed(id)]
pub struct TransactionId;

#[td_type::typed(timestamp)]
pub struct TriggeredOn;

#[td_type::typed(id)]
pub struct TriggerId;

#[td_type::typed(string(regex = TriggerStatus::REGEX))]
pub struct TriggerStatus;

impl TriggerStatus {
    const REGEX: &'static str = "^[AD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(id)]
pub struct TriggerVersionId;

#[td_type::typed(bool)]
pub struct UserEnabled;

#[td_type::typed(id)]
pub struct UserId;

impl UserId {
    pub fn admin() -> Self {
        Self(Id::_new(ID_USER_ADMIN))
    }
}

#[td_type::typed(string(parser = parse_user))]
pub struct UserName;

#[td_type::typed(id_name(id = UserId, name = UserName))]
pub struct UserIdName;

#[td_type::typed(id)]
pub struct UserRoleId;
