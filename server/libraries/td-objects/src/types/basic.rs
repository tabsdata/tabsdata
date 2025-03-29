//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::parse::{
    parse_collection, parse_entity, parse_function, parse_role, parse_table, parse_user,
    DATA_LOCATION_REGEX,
};
use crate::types::table::TableVersionDBWithNames;
use crate::types::table_ref::{TableRef, VersionedTableRef, Versions};

#[td_type::typed(timestamp)]
pub struct AtTime;

#[td_type::typed(id)]
pub struct BundleId;

#[td_type::typed(id)]
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

#[td_type::typed(bool)]
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

#[td_type::typed(string(regex = FunctionStatus::REGEX))]
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

#[td_type::typed(string(min_len = 1, max_len = 1024))]
pub struct Partition;

#[td_type::typed(id)]
pub struct PermissionId;

#[td_type::typed(id_name(id = PermissionId))]
pub struct PermissionIdName;

#[td_type::typed(string(regex = PermissionType::REGEX))]
pub struct PermissionType;

impl PermissionType {
    const REGEX: &'static str = "^(sa|ss|ca|cd|cx|cr|cR)$";

    pub fn sys_admin() -> Self {
        Self("sa".to_string())
    }

    pub fn sec_admin() -> Self {
        Self("ss".to_string())
    }

    pub fn collection_admin() -> Self {
        Self("ca".to_string())
    }

    pub fn collection_dev() -> Self {
        Self("cd".to_string())
    }

    pub fn collection_exec() -> Self {
        Self("cx".to_string())
    }

    pub fn collection_read() -> Self {
        Self("cr".to_string())
    }

    pub fn collection_read_all() -> Self {
        Self("cR".to_string())
    }

    pub fn on_entity_type(&self) -> PermissionEntityType {
        if self.0.starts_with("s") {
            PermissionEntityType::system()
        } else {
            PermissionEntityType::collection()
        }
    }
}

#[td_type::typed(string(regex = PermissionEntityType::REGEX))]
pub struct PermissionEntityType;

impl PermissionEntityType {
    const REGEX: &'static str = "^(s|c)$";

    pub fn system() -> Self {
        Self("s".to_string())
    }

    pub fn collection() -> Self {
        Self("c".to_string())
    }
}

#[td_type::typed(timestamp)]
pub struct PublishedOn;

#[td_type::typed(id)]
pub struct RoleId;

#[td_type::typed(string(parser = parse_role))]
pub struct RoleName;

#[td_type::typed(id_name(id = RoleId, name = RoleName))]
pub struct RoleIdName;

#[td_type::typed(string(min_len = 0, max_len = 4096))]
pub struct Snippet;

#[td_type::typed(string(min_len = 1, max_len = 10))]
pub struct StorageVersion;

#[td_type::typed(composed(inner = VersionedTableRef))]
pub struct TableDependency;

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

#[td_type::typed(id)]
pub struct TableVersionId;

#[td_type::typed(composed(inner = Versions))]
pub struct TableVersions;

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

#[td_type::typed(id)]
pub struct UserId;

#[td_type::typed(string(parser = parse_user))]
pub struct UserName;

#[td_type::typed(id_name(id = UserId, name = UserName))]
pub struct UserIdName;

#[td_type::typed(id)]
pub struct UserRoleId;
