//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{ListFilterGenerator, QueryError};
use crate::types::parse::{
    DATA_LOCATION_REGEX, parse_collection, parse_email, parse_entity, parse_execution,
    parse_function, parse_role, parse_table, parse_user,
};
use crate::types::table::{TableDBRead, TableDBWithNames};
use crate::types::table_ref::{TableRef, VersionedTableRef, Versions};
use crate::types::{ComposedString, DataAccessObject};
use async_trait::async_trait;
use sqlx::{QueryBuilder, Sqlite};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use strum::IntoEnumIterator;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::id::{ID_LENGTH, Id};
use td_error::TdError;
use td_security::{
    ADMIN_USER, ID_ALL_ENTITIES, ID_ROLE_SEC_ADMIN, ID_ROLE_SYS_ADMIN, ID_ROLE_USER, ID_USER_ADMIN,
    SEC_ADMIN_ROLE, SYS_ADMIN_ROLE, USER_ROLE,
};

#[td_type::typed(string)]
pub struct AccessToken;

#[td_type::typed(i64)]
pub struct AccessTokenExpiration;

#[td_type::typed(id)]
pub struct AccessTokenId;

impl AccessTokenId {
    pub fn log(&self) -> String {
        // half the token as trace
        let s = self.to_string();
        let mid = ID_LENGTH / 2;
        s[mid..].to_string()
    }
}

#[td_type::typed(timestamp, try_from = TriggeredOn)]
pub struct AtTime;

#[td_type::typed(string(default = "<unavailable>"))]
pub struct BuildManifest;

#[td_type::typed(string)]
pub struct BundleHash;

#[td_type::typed(id)]
pub struct BundleId;

#[td_type::typed(id, try_from = EntityId, try_from = FromCollectionId, try_from = ToCollectionId)]
pub struct CollectionId;

impl CollectionId {
    pub fn all_collections() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_collections(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

#[td_type::typed(id_name(id = CollectionId, name = CollectionName))]
pub struct CollectionIdName;

#[td_type::typed(string(parser = parse_collection), try_from = ToCollectionName)]
pub struct CollectionName;

#[td_type::typed(i64)]
pub struct ColumnCount;

#[td_type::typed(bool)]
pub struct DataChanged;

impl From<Option<HasData>> for DataChanged {
    fn from(has_data: Option<HasData>) -> Self {
        if let Some(has_data) = has_data {
            DataChanged(*has_data)
        } else {
            DataChanged(false)
        }
    }
}

#[td_type::typed(string(regex = DATA_LOCATION_REGEX, default = "/"))]
pub struct DataLocation;

#[td_type::typed_enum]
pub enum Decorator {
    #[typed_enum(rename = "P")]
    Publisher,
    #[typed_enum(rename = "T")]
    Transformer,
    #[typed_enum(rename = "S")]
    Subscriber,
}

#[td_type::typed(id)]
pub struct DependencyId;

#[td_type::typed(i32(default = 0))]
pub struct DependencyPos;

#[td_type::typed_enum]
pub enum DependencyStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "D")]
    Deleted,
}

impl DependencyStatus {
    pub async fn active() -> Result<Vec<DependencyStatus>, TdError> {
        Ok(vec![DependencyStatus::Active])
    }
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

impl EntityId {
    pub fn all_entities() -> Self {
        Self(Id::_new(ID_ALL_ENTITIES))
    }

    pub fn is_all_entities(&self) -> bool {
        *self.0 == ID_ALL_ENTITIES
    }
}

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

/// Represents the status of an execution.
/// It is a summary of the statuses of all function runs within the execution.
#[td_type::typed_enum]
pub enum ExecutionStatus {
    /// All function runs are scheduled.
    #[typed_enum(rename = "S")]
    Scheduled,
    /// At least one function run is still running (or able to do so).
    #[typed_enum(rename = "R")]
    Running,
    /// All functions are in a finished state, but at least one is Failed or OnHold.
    #[typed_enum(rename = "L")]
    Stalled,
    /// All function runs are finished (either successfully or with issues).
    #[typed_enum(rename = "F")]
    Finished,
    /// Unexpected status used as fallback.
    #[typed_enum(rename = "U")]
    Unexpected,
}

#[td_type::typed(i16)]
pub struct ExecutionTry;

#[td_type::typed(bool(default = false))]
pub struct Fixed;

#[td_type::typed(bool)]
pub struct FixedRole;

#[td_type::typed(id, try_from = CollectionId)]
pub struct FromCollectionId;

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

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of a function run.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Canceled --> [*]
///     Yanked --> [*]
///
///     Scheduled --> RunRequested
///     RunRequested --> Scheduled
///     RunRequested --> Running
///     Scheduled --> OnHold
///     Scheduled --> Canceled
///     Running --> Done
///     Running --> Error
///     Running --> Failed
///     Running --> Canceled
///     OnHold --> Canceled
///     OnHold --> Scheduled
///     Error --> Running
///     Error --> Canceled
///     Failed --> ReScheduled
///     Failed --> Canceled
///     Done --> Canceled
///     Done --> Committed
///     Committed --> Yanked
/// ```
#[td_type::typed_enum]
pub enum FunctionRunStatus {
    #[typed_enum(rename = "S")]
    Scheduled,
    #[typed_enum(rename = "RR")]
    RunRequested,
    #[typed_enum(rename = "RS")]
    ReScheduled,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "D")]
    Done,
    #[typed_enum(rename = "E")]
    Error,
    #[typed_enum(rename = "F")]
    Failed,
    #[typed_enum(rename = "H")]
    OnHold,
    #[typed_enum(rename = "C")]
    Committed,
    #[typed_enum(rename = "X")]
    Canceled,
    #[typed_enum(rename = "Y")]
    Yanked,
}

impl FunctionRunStatus {
    pub async fn committed() -> Result<Vec<Self>, TdError> {
        Ok(vec![FunctionRunStatus::Committed])
    }
}

impl From<WorkerCallbackStatus> for FunctionRunStatus {
    fn from(value: WorkerCallbackStatus) -> Self {
        match value {
            WorkerCallbackStatus::Running => FunctionRunStatus::Running,
            WorkerCallbackStatus::Done => FunctionRunStatus::Done,
            WorkerCallbackStatus::Error => FunctionRunStatus::Error,
            WorkerCallbackStatus::Failed => FunctionRunStatus::Failed,
        }
    }
}

#[td_apiforge::apiserver_schema]
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FunctionRunStatusCount(HashMap<FunctionRunStatus, StatusCount>);

impl FunctionRunStatusCount {
    fn new(map: HashMap<FunctionRunStatus, StatusCount>) -> Self {
        let mut map = map;
        // Enforce all statuses to be present in the map with count 0.
        for status in FunctionRunStatus::iter() {
            map.entry(status)
                .or_insert(StatusCount::try_from(0).unwrap());
        }
        Self(map)
    }
}

impl From<sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>> for FunctionRunStatusCount {
    fn from(value: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>) -> Self {
        FunctionRunStatusCount::new(value.0)
    }
}

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[td_type::typed(string(max_len = 4096, default = "{}"))]
pub struct FunctionRuntimeValues;

#[td_type::typed_enum]
pub enum FunctionStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

impl FunctionStatus {
    pub async fn active() -> Result<Vec<FunctionStatus>, TdError> {
        Ok(vec![FunctionStatus::Active])
    }

    pub async fn active_or_frozen() -> Result<Vec<FunctionStatus>, TdError> {
        Ok(vec![FunctionStatus::Active, FunctionStatus::Frozen])
    }

    pub async fn none() -> Result<Vec<FunctionStatus>, TdError> {
        Ok(vec![])
    }
}

#[td_type::typed(id)]
pub struct FunctionVersionId;

#[td_type::typed_enum]
pub enum GrantType {
    #[typed_enum(rename = "refresh_token")]
    RefreshToken,
}

#[td_type::typed_enum]
pub enum GlobalStatus {
    #[typed_enum(rename = "S")]
    Scheduled,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "L")]
    Stalled,
    #[typed_enum(rename = "F")]
    Finished,
    #[typed_enum(rename = "U")]
    Unknown,
}

#[td_type::typed(bool(default = false))]
pub struct HasData;

#[td_type::typed(id)]
pub struct InterCollectionPermissionId;

#[td_type::typed(id_name(id = InterCollectionPermissionId))]
pub struct InterCollectionPermissionIdName;

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
    #[typed_enum(rename = "s")]
    System,
    #[typed_enum(rename = "c")]
    Collection,
}

#[td_type::typed(id)]
pub struct PermissionId;

#[td_type::typed(id_name(id = PermissionId))]
pub struct PermissionIdName;

#[td_type::typed_enum]
pub enum PermissionType {
    #[typed_enum(rename = "sa")]
    SysAdmin,
    #[typed_enum(rename = "ss")]
    SecAdmin,
    #[typed_enum(rename = "ca")]
    CollectionAdmin,
    #[typed_enum(rename = "cd")]
    CollectionDev,
    #[typed_enum(rename = "cx")]
    CollectionExec,
    #[typed_enum(rename = "cr")]
    CollectionRead,
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
pub struct PythonVersion;

#[td_type::typed(string)]
pub struct RefreshToken;

#[td_type::typed(id)]
pub struct RefreshTokenId;

#[td_type::typed(id)]
pub struct RequirementId;

#[td_type::typed(i16(min = 1))]
pub struct LogsCastNumber;

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

#[td_type::typed(i64)]
pub struct RowCount;

#[td_type::typed(i64(min = 0, default = 0))]
pub struct SampleOffset;

#[td_type::typed(i64(min = 0, max = SampleLen::MAX, default = 100))]
pub struct SampleLen;

impl SampleLen {
    pub const MAX: i64 = 1000;
}
#[td_type::typed(string)]
pub struct SchemaFieldName;

#[td_type::typed(string)]
pub struct SchemaFieldType;

#[td_type::typed(string)]
pub struct SchemaHash;

#[td_type::typed(bool)]
pub struct SelfDependency;

#[td_type::typed(id)]
pub struct SessionId;

#[td_type::typed_enum]
pub enum SessionStatus {
    #[typed_enum(rename = "a")]
    Active,
    #[typed_enum(rename = "i_pc")]
    InvalidPasswordChange,
    #[typed_enum(rename = "i_nt")]
    InvalidNewToken,
    #[typed_enum(rename = "i_rc")]
    InvalidRoleChange,
    #[typed_enum(rename = "i_l")]
    InvalidLogout,
    #[typed_enum(rename = "i_ud")]
    InvalidUserDisabled,
}

#[td_type::typed(string(min_len = 0, max_len = 4096))]
pub struct Snippet;

#[td_type::typed(string)]
pub struct Sql;

#[td_type::typed(i32)]
pub struct StatusCount;

#[td_type::typed(string(min_len = 1, max_len = 10, default = "V1"))]
pub struct StorageVersion;

#[td_type::typed(bool)]
pub struct System;

#[td_type::typed(id)]
pub struct TableDataId;

#[td_type::typed(id)]
pub struct TableDataVersionId;

#[td_type::typed(composed(inner = "VersionedTableRef::<TableName>"), try_from = TableDependencyDto)]
pub struct TableDependency;

#[td_type::typed(composed(inner = "VersionedTableRef::<TableNameDto>"))]
pub struct TableDependencyDto;

#[td_type::typed(i32)]
pub struct TableFunctionParamPos;

#[td_type::typed(id)]
pub struct TableId;

#[td_type::typed(id_name(id = TableId, name = TableName))]
pub struct TableIdName;

#[td_type::typed(string, try_from = TableNameDto)]
pub struct TableName;

impl TableName {
    pub fn is_private(&self) -> bool {
        self.0.starts_with('_')
    }
}

impl TryFrom<&TableDBRead> for TableName {
    type Error = TdError;

    fn try_from(v: &TableDBRead) -> Result<Self, Self::Error> {
        let table = v.name().clone();
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

#[td_type::typed_enum]
pub enum TableStatus {
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

impl TableStatus {
    pub async fn active() -> Result<Vec<TableStatus>, TdError> {
        Ok(vec![TableStatus::Active])
    }

    pub async fn frozen() -> Result<Vec<TableStatus>, TdError> {
        Ok(vec![TableStatus::Frozen])
    }

    pub async fn active_or_frozen() -> Result<Vec<TableStatus>, TdError> {
        Ok(vec![TableStatus::Active, TableStatus::Frozen])
    }
}

#[td_type::typed(composed(inner = "TableRef::<TableName>"), try_from = TableTriggerDto)]
pub struct TableTrigger;

impl TryFrom<&TableDependencyDto> for TableTrigger {
    type Error = TdError;

    fn try_from(v: &TableDependencyDto) -> Result<Self, Self::Error> {
        let table = TableTrigger::new(TableRef::new(v.collection().clone(), v.table().try_into()?));
        Ok(table)
    }
}

impl TryFrom<&TableDBWithNames> for TableTrigger {
    type Error = TdError;

    fn try_from(v: &TableDBWithNames) -> Result<Self, Self::Error> {
        let table = TableTrigger::new(TableRef::new(
            Some(v.collection().clone()),
            v.name().clone(),
        ));
        Ok(table)
    }
}

#[td_type::typed(composed(inner = "TableRef::<TableNameDto>"))]
pub struct TableTriggerDto;

#[td_type::typed(id)]
pub struct TableVersionId;

#[td_type::typed(composed(inner = "Versions"))]
pub struct TableVersions;

#[td_type::typed(string)]
pub struct TabsdataVersion;

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

/// Represents the status of a transaction. Note transactions are atomic status wise.
/// So final status (e.g., Committed, Canceled, Yanked) means all function runs within the transaction
/// do have the same status.
/// It is a summary of the statuses of all function runs within the transaction.
#[td_type::typed_enum]
pub enum TransactionStatus {
    /// All function runs are scheduled.
    #[typed_enum(rename = "S")]
    Scheduled,
    /// At least one function run is still running (or able to do so).
    #[typed_enum(rename = "R")]
    Running,
    /// All functions are in a finished state, but at least one is Failed or OnHold.
    #[typed_enum(rename = "L")]
    Stalled,
    /// All function runs are Committed.
    #[typed_enum(rename = "C")]
    Committed,
    /// All function runs are Canceled.
    #[typed_enum(rename = "X")]
    Canceled,
    /// All function runs are Yanked.
    #[typed_enum(rename = "Y")]
    Yanked,
    /// Unexpected status used as fallback.
    #[typed_enum(rename = "U")]
    Unexpected,
}

#[td_type::typed_enum]
pub enum Trigger {
    #[typed_enum(rename = "M")]
    Manual,
    #[typed_enum(rename = "D")]
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
    #[typed_enum(rename = "A")]
    Active,
    #[typed_enum(rename = "F")]
    Frozen,
    #[typed_enum(rename = "D")]
    Deleted,
}

impl TriggerStatus {
    pub async fn active() -> Result<Vec<TriggerStatus>, TdError> {
        Ok(vec![TriggerStatus::Active])
    }

    pub async fn active_or_frozen() -> Result<Vec<TriggerStatus>, TdError> {
        Ok(vec![TriggerStatus::Active, TriggerStatus::Frozen])
    }
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
pub struct InputIdx;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct VersionPos;

#[derive(Debug, Clone)]
pub struct VisibleCollections(HashSet<CollectionId>, HashSet<CollectionId>);

impl VisibleCollections {
    pub fn new(visible: HashSet<CollectionId>, indirect: HashSet<CollectionId>) -> Self {
        Self(visible, indirect)
    }

    pub fn direct(&self) -> &HashSet<CollectionId> {
        &self.0
    }

    pub fn indirect(&self) -> &HashSet<CollectionId> {
        &self.1
    }
}

fn collections_where<'a>(
    query_builder: &mut QueryBuilder<'a, Sqlite>,
    field: &str,
    collections: &'a HashSet<CollectionId>,
) -> Result<(), QueryError> {
    query_builder.push("(");
    if collections.is_empty() {
        query_builder.push("1 = 0"); // if no collections, we need to ensure the condition is false
    } else if collections.contains(&CollectionId::all_collections()) {
        query_builder.push("1 = 1"); // if all collections, we need to ensure the condition is true
    } else {
        query_builder.push(format!("{field} IN ("));
        let mut separated = query_builder.separated(", ");
        for collection_id in collections {
            separated.push_bind(collection_id);
        }
        query_builder.push(")");
    }
    query_builder.push(")");
    Ok(())
}

#[async_trait]
impl ListFilterGenerator for VisibleCollections {
    async fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        first: bool,
        query_builder: &mut QueryBuilder<'a, Sqlite>,
    ) -> Result<bool, QueryError> {
        let mut first = first;
        if first {
            query_builder.push(" WHERE ");
        } else {
            query_builder.push(" AND ");
        }
        first = false;

        let field = D::sql_field_for_type(std::any::type_name::<CollectionId>()).ok_or(
            QueryError::TypeNotFound(
                std::any::type_name::<CollectionId>().to_string(),
                D::sql_table().to_string(),
            ),
        )?;

        query_builder.push("(");
        collections_where(query_builder, field, self.direct())?;
        query_builder.push(" OR ");
        collections_where(query_builder, field, self.indirect())?;
        query_builder.push(")");

        Ok(first)
    }
}

#[derive(Debug, Clone)]
pub struct VisibleTablesCollections(VisibleCollections);

impl Deref for VisibleTablesCollections {
    type Target = VisibleCollections;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&VisibleCollections> for VisibleTablesCollections {
    type Error = TdError;

    fn try_from(visible: &VisibleCollections) -> Result<Self, TdError> {
        Ok(Self(visible.clone()))
    }
}

// TODO this is only allows to tables, that have CollectionId and Private. We should make this
// more generic and resilient.
#[async_trait]
impl ListFilterGenerator for VisibleTablesCollections {
    async fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        first: bool,
        query_builder: &mut QueryBuilder<'a, Sqlite>,
    ) -> Result<bool, QueryError> {
        let mut first = first;
        if first {
            query_builder.push(" WHERE ");
        } else {
            query_builder.push(" AND ");
        }
        first = false;

        let field = D::sql_field_for_type(std::any::type_name::<CollectionId>()).ok_or(
            QueryError::TypeNotFound(
                std::any::type_name::<CollectionId>().to_string(),
                D::sql_table().to_string(),
            ),
        )?;

        query_builder.push("(");
        collections_where(query_builder, field, self.direct())?;
        query_builder.push(" OR ");

        let private = D::sql_field_for_type(std::any::type_name::<Private>()).ok_or(
            QueryError::TypeNotFound(
                std::any::type_name::<Private>().to_string(),
                D::sql_table().to_string(),
            ),
        )?;

        query_builder.push("(");
        if self.indirect().is_empty() {
            query_builder.push("1 = 0"); // if no collections, we need to ensure the condition is false
        } else if self.indirect().contains(&CollectionId::all_collections()) {
            query_builder.push("1 = 1"); // if all collections, we need to ensure the condition is true
        } else {
            query_builder.push(format!("{field} IN ("));
            let mut separated = query_builder.separated(", ");
            for collection_id in self.indirect() {
                separated.push_bind(collection_id);
                separated.push_bind(format!(" AND {private} = false")); // only non-private tables
            }
            query_builder.push(")");
        }
        query_builder.push(")");

        query_builder.push(")");

        Ok(first)
    }
}

#[derive(Debug, Clone)]
pub struct VisibleFunctionsCollections(VisibleCollections);

impl Deref for VisibleFunctionsCollections {
    type Target = VisibleCollections;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&VisibleCollections> for VisibleFunctionsCollections {
    type Error = TdError;

    fn try_from(visible: &VisibleCollections) -> Result<Self, TdError> {
        Ok(Self(visible.clone()))
    }
}

// TODO this is only allows functions of direct collections. We should make this
// more generic and resilient.
#[async_trait]
impl ListFilterGenerator for VisibleFunctionsCollections {
    async fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        first: bool,
        query_builder: &mut QueryBuilder<'a, Sqlite>,
    ) -> Result<bool, QueryError> {
        let mut first = first;
        if first {
            query_builder.push(" WHERE ");
        } else {
            query_builder.push(" AND ");
        }
        first = false;

        let field = D::sql_field_for_type(std::any::type_name::<CollectionId>()).ok_or(
            QueryError::TypeNotFound(
                std::any::type_name::<CollectionId>().to_string(),
                D::sql_table().to_string(),
            ),
        )?;

        query_builder.push("(");
        collections_where(query_builder, field, self.direct())?;
        query_builder.push(")");

        Ok(first)
    }
}

#[td_type::typed(id)]
pub struct WorkerId;

#[td_type::typed(id_name(id = WorkerId))]
pub struct WorkerIdName;

#[td_type::typed_enum]
pub enum WorkerStatus {
    #[typed_enum(rename = "RR")]
    RunRequested,
    #[typed_enum(rename = "R")]
    Running,
    #[typed_enum(rename = "D")]
    Done,
    #[typed_enum(rename = "E")]
    Error,
    #[typed_enum(rename = "F")]
    Failed,
    #[typed_enum(rename = "X")]
    Canceled,
}

impl From<WorkerCallbackStatus> for WorkerStatus {
    fn from(value: WorkerCallbackStatus) -> Self {
        match value {
            WorkerCallbackStatus::Running => WorkerStatus::Running,
            WorkerCallbackStatus::Done => WorkerStatus::Done,
            WorkerCallbackStatus::Error => WorkerStatus::Error,
            WorkerCallbackStatus::Failed => WorkerStatus::Failed,
        }
    }
}
