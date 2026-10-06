//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::Scd2ValidFrom;
use crate::types::basic::{
    AtTime, BasePath, BundleId, CollectionId, CollectionIdName, CollectionName, ConnectionKind,
    Connector, Decorator, Description, Enabled, FunctionId, FunctionIdName, FunctionName,
    FunctionRuntimeValues, FunctionStatus, FunctionType, FunctionVersionId, GroupId, Parent,
    ProjectId, RawSnippet, ResetStage, ResetState, ReuseFrozen, Snippet, StageId, StorageVersion,
    TableName, TableNameDto, TableType, UserId, UserLabel, WorkerConfig, WorkerId, WorkerTypeDto,
};
use crate::types::composed::{TableDependency, TableDependencyDto, TableTrigger, TableTriggerDto};
use std::collections::HashMap;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{FromDao, and};
use td_error::TdError;

#[ta_objects::Dto]
pub struct FunctionRegister {
    pub name: FunctionName,
    pub description: Description,
    pub bundle_id: BundleId,
    pub snippet: RawSnippet,
    pub decorator: Decorator,
    pub connector: Option<Connector>,
    pub connection: Option<ConnectionKind>,
    #[serde(default)]
    pub base_path: Option<BasePath>,
    pub dependencies: Option<Vec<TableDependencyDto>>,
    pub triggers: Option<Vec<TableTriggerDto>>,
    pub tables: Option<Vec<TableNameDto>>,
    #[serde(default)]
    pub table_types: Option<HashMap<TableNameDto, TableType>>,
    #[serde(default)]
    pub runtime_values: FunctionRuntimeValues,
    pub reuse_frozen_tables: ReuseFrozen,
    #[serde(default)]
    pub enabled: Option<Enabled>,
    #[serde(default)]
    pub workers: Option<HashMap<WorkerTypeDto, WorkerConfig>>,
    #[serde(default)]
    pub child_functions: Option<Vec<ChildFunction>>,
}

#[ta_objects::Dto]
pub struct ChildFunction {
    pub name: FunctionName,
    #[serde(default)]
    pub function_type: FunctionType,
    pub bundle_id: BundleId,
    pub snippet: RawSnippet,
    #[serde(default)]
    pub dependencies: Option<Vec<TableDependencyDto>>,
    #[serde(default)]
    pub tables: Option<Vec<TableNameDto>>,
    #[serde(default)]
    pub table_types: Option<HashMap<TableNameDto, TableType>>,
    pub parent_table: TableNameDto,
}

impl ChildFunction {
    pub fn parent_tables(&self) -> HashMap<TableNameDto, TableNameDto> {
        self.tables
            .iter()
            .flatten()
            .map(|table| (table.clone(), self.parent_table.clone()))
            .collect()
    }

    pub fn to_register(
        &self,
        reuse_frozen_tables: ReuseFrozen,
        enabled: Enabled,
    ) -> Result<FunctionRegister, TdError> {
        Ok(FunctionRegister {
            name: self.name.clone(),
            description: Description::try_from(self.name.to_string())?,
            bundle_id: self.bundle_id,
            snippet: self.snippet.clone(),
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: self.dependencies.clone(),
            triggers: None,
            tables: self.tables.clone(),
            table_types: self.table_types.clone(),
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables,
            enabled: Some(enabled),
            workers: None,
            child_functions: None,
        })
    }

    pub fn to_update(
        &self,
        reuse_frozen_tables: ReuseFrozen,
        enabled: Enabled,
    ) -> Result<FunctionUpdate, TdError> {
        Ok(FunctionUpdate {
            name: Some(self.name.clone()),
            description: Some(Description::try_from(self.name.to_string())?),
            bundle_id: Some(self.bundle_id),
            snippet: Some(self.snippet.clone()),
            decorator: Some(Decorator::Transformer),
            dependencies: self.dependencies.clone(),
            triggers: None,
            tables: self.tables.clone(),
            table_types: self.table_types.clone(),
            runtime_values: Some(FunctionRuntimeValues::default()),
            reuse_frozen_tables,
            enabled: Some(enabled),
            child_functions: None,
            ..Default::default()
        })
    }
}

impl FunctionRegister {
    pub fn to_dao(
        &self,
        ctx: &RequestContext,
        collection_id: CollectionId,
        stage_base_path: BasePath,
    ) -> Result<FunctionInsertDB, TdError> {
        Ok(FunctionInsertDB {
            id: FunctionVersionId::new(),
            parent_function_id: None,
            collection_id,
            name: self.name.clone(),
            description: self.description.clone(),
            decorator: self.decorator.clone(),
            function_type: FunctionType::default(),
            connector: self.connector.clone(),
            connection: self.connection.clone(),
            runtime_values: self.runtime_values.clone(),
            function_id: FunctionId::default(),
            base_path: self.base_path.clone(),
            stage_id: StageId::default(),
            stage_base_path,
            storage_version: StorageVersion::default(),
            bundle_id: self.bundle_id,
            snippet: Snippet::try_from(self.snippet.clone())?,
            defined_by_id: ctx.user_id,
            status: FunctionStatus::Active,
            enabled: self.enabled.unwrap_or_default(),
        })
    }
}

#[ta_objects::Dto]
#[derive(Default)]
#[serde(default)]
pub struct FunctionUpdate {
    pub name: Option<FunctionName>,
    pub description: Option<Description>,
    pub bundle_id: Option<BundleId>,
    pub snippet: Option<RawSnippet>,
    pub decorator: Option<Decorator>,
    pub connector: Option<Option<Connector>>,
    pub connection: Option<Option<ConnectionKind>>,
    pub dependencies: Option<Vec<TableDependencyDto>>,
    pub triggers: Option<Vec<TableTriggerDto>>,
    pub tables: Option<Vec<TableNameDto>>,
    pub table_types: Option<HashMap<TableNameDto, TableType>>,
    pub runtime_values: Option<FunctionRuntimeValues>,
    pub reuse_frozen_tables: ReuseFrozen,
    pub enabled: Option<Enabled>,
    /// Storage-prefix override. Some sets the override for the new version;
    /// None carries forward the previous version's value.
    pub base_path: Option<BasePath>,
    pub workers: Option<HashMap<WorkerTypeDto, WorkerConfig>>,
    /// When true, regenerate the function's state table id instead of
    /// carrying it forward, so publishers re-read initial config values.
    pub reset_state: ResetState,
    /// When true, regenerate the function's stage id (a fresh stage/checkpoint
    /// dir) instead of carrying it forward, discarding accumulated stage state.
    pub reset_stage: ResetStage,
    /// None keeps the current children; an empty list removes them all.
    pub child_functions: Option<Vec<ChildFunction>>,
}

impl FunctionUpdate {
    pub fn to_dao(
        &self,
        previous: &FunctionDB,
        ctx: &RequestContext,
    ) -> Result<FunctionInsertDB, TdError> {
        let mut function = FunctionInsertDB {
            id: FunctionVersionId::new(),
            status: FunctionStatus::Active,
            defined_by_id: ctx.user_id,
            storage_version: StorageVersion::default(),
            ..previous.base.clone()
        };
        if let Some(name) = &self.name {
            function.name = name.clone();
        }
        if let Some(description) = &self.description {
            function.description = description.clone();
        }
        if let Some(bundle_id) = self.bundle_id {
            function.bundle_id = bundle_id;
        }
        if let Some(snippet) = &self.snippet {
            function.snippet = Snippet::try_from(snippet)?;
        }
        if let Some(decorator) = &self.decorator {
            function.decorator = decorator.clone();
        }
        if let Some(connector) = &self.connector {
            function.connector = connector.clone();
        }
        if let Some(connection) = &self.connection {
            function.connection = connection.clone();
        }
        if let Some(runtime_values) = &self.runtime_values {
            function.runtime_values = runtime_values.clone();
        }
        if let Some(enabled) = self.enabled {
            function.enabled = enabled;
        }
        if let Some(base_path) = &self.base_path {
            function.base_path = Some(base_path.clone());
        }
        // stage_base_path is carried forward, so the fresh stage lands at the same
        // stamped prefix — not relocated by a later collection base_path change.
        if *self.reset_stage {
            function.stage_id = StageId::default();
        }
        Ok(function)
    }
}

#[ta_objects::Dto]
pub struct FunctionEnable {
    pub enabled: Enabled,
}

/// A patch is a code-only override of a specific `FunctionVersionId`
/// (`patch_of`): it shares the parent's topology — no tables, dependencies,
/// triggers, workers — and is reachable only by explicit fvid (replay).
/// Fields not set fall back to parent values.
#[ta_objects::Dto]
pub struct FunctionPatch {
    pub patch_of: FunctionVersionId,
    pub bundle_id: BundleId,
    pub snippet: RawSnippet,
    #[serde(default)]
    pub runtime_values: Option<FunctionRuntimeValues>,
    #[serde(default)]
    pub description: Option<Description>,
}

impl FunctionPatch {
    pub fn to_dao(
        self,
        parent: FunctionDB,
        ctx: &RequestContext,
    ) -> Result<FunctionPatchInsertDB, TdError> {
        Ok(FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: parent.base.id.into(),
            function_id: parent.base.function_id,
            collection_id: parent.base.collection_id,
            snippet: Snippet::try_from(&self.snippet)?,
            bundle_id: self.bundle_id,
            runtime_values: self.runtime_values.unwrap_or(parent.base.runtime_values),
            description: self.description.unwrap_or(parent.base.description),
            defined_by_id: ctx.user_id,
            status: FunctionStatus::Active,
        })
    }
}

#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(table = "functions", ops(insert), scd2, project_scoped)]
pub struct FunctionInsertDB {
    #[dao(pk)]
    pub id: FunctionVersionId,
    pub parent_function_id: Option<Parent<FunctionId>>,
    pub collection_id: CollectionId,
    pub name: FunctionName,
    pub description: Description,
    pub decorator: Decorator,
    pub function_type: FunctionType,
    pub connector: Option<Connector>,
    pub connection: Option<ConnectionKind>,
    pub runtime_values: FunctionRuntimeValues,
    #[dao(partition_by)]
    pub function_id: FunctionId,
    // Storage-prefix override for output table data. NULL → inherit the collection
    // base_path at mint; carried forward on update unless FunctionUpdate::base_path.
    pub base_path: Option<BasePath>,
    // Stage/checkpoint identity: carried forward on update, regenerated when
    // FunctionUpdate::reset_stage.
    pub stage_id: StageId,
    // Stamped from the collection base_path at register and carried forward, so
    // stage state does not relocate when the collection base_path later changes.
    pub stage_base_path: BasePath,
    pub storage_version: StorageVersion,
    pub bundle_id: BundleId,
    pub snippet: Snippet,
    pub defined_by_id: UserId,
    pub status: FunctionStatus,
    pub enabled: Enabled,
}

#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(table = "functions", ops(select), scd2, project_scoped)]
pub struct FunctionDB {
    #[dao(flatten)]
    pub base: FunctionInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl FunctionDB {
    pub fn active() -> impl SqlCondition<Self> {
        FunctionStatus::Active
    }

    pub fn children_of(parent: &FunctionId) -> impl SqlCondition<Self> {
        Some(Parent::from(*parent))
    }

    pub fn frozen_version(&self, ctx: &RequestContext) -> FunctionInsertDB {
        FunctionInsertDB {
            id: FunctionVersionId::new(),
            status: FunctionStatus::Frozen,
            defined_by_id: ctx.user_id,
            ..self.base.clone()
        }
    }

    pub fn re_registered(&self, ctx: &RequestContext, enabled: Enabled) -> FunctionInsertDB {
        FunctionInsertDB {
            id: FunctionVersionId::new(),
            defined_by_id: ctx.user_id,
            enabled,
            ..self.base.clone()
        }
    }

    pub fn executable() -> impl SqlCondition<Self> {
        and!(FunctionStatus::Active, Enabled::TRUE)
    }
}

/// Lives in its own `function_patches` table so every "find latest version"
/// query against `functions` is patch-blind by construction.
#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(table = "function_patches", ops(insert), scd2, project_scoped)]
pub struct FunctionPatchInsertDB {
    #[dao(pk)]
    pub id: FunctionVersionId,
    #[dao(partition_by)]
    pub parent_function_version_id: Parent<FunctionVersionId>,
    pub function_id: FunctionId,
    pub collection_id: CollectionId,
    pub snippet: Snippet,
    pub bundle_id: BundleId,
    pub runtime_values: FunctionRuntimeValues,
    pub description: Description,
    pub defined_by_id: UserId,
    pub status: FunctionStatus,
}

/// The row as the database holds it: `project_id` is the `project_isolation` policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(table = "function_patches", ops(select), scd2, project_scoped)]
pub struct FunctionPatchDB {
    #[dao(flatten)]
    pub base: FunctionPatchInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten, order_by)]
    pub interval: Scd2Interval,
}

impl FunctionPatchDB {
    pub fn active() -> impl SqlCondition<Self> {
        FunctionStatus::Active
    }
}

/// The patch row with its author's name resolved, the list surface's source.
#[ta_objects::Dao]
#[dao(view = "function_patches__with_names", scd2, project_scoped)]
pub struct FunctionPatchDBWithNames {
    #[dao(flatten)]
    pub base: FunctionPatchDB,
    pub defined_by: UserLabel,
}

#[ta_objects::Dto]
#[dto(list(on = FunctionPatchDBWithNames))]
pub struct FunctionPatchRecord {
    #[dto(list(pagination_by = "-"))]
    pub id: FunctionVersionId,
    #[dto(list(filter, filter_like, order_by))]
    pub parent_function_version_id: Parent<FunctionVersionId>,
    #[dto(list(filter, filter_like, order_by))]
    pub function_id: FunctionId,
    #[dto(list(filter, filter_like, order_by))]
    pub collection_id: CollectionId,
    pub snippet: Snippet,
    pub bundle_id: BundleId,
    pub runtime_values: FunctionRuntimeValues,
    #[dto(list(filter, filter_like))]
    pub description: Description,
    #[dto(list(filter, filter_like, order_by, col = Scd2ValidFrom))]
    pub defined_on: AtTime,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: FunctionStatus,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserLabel,
}

impl FromDao for FunctionPatchRecord {
    type Dao = FunctionPatchDBWithNames;

    fn from_dao(dao: FunctionPatchDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            parent_function_version_id: dao.base.base.parent_function_version_id,
            function_id: dao.base.base.function_id,
            collection_id: dao.base.base.collection_id,
            snippet: dao.base.base.snippet,
            bundle_id: dao.base.base.bundle_id,
            runtime_values: dao.base.base.runtime_values,
            description: dao.base.base.description,
            defined_on: dao.base.interval.valid_from.into(),
            defined_by_id: dao.base.base.defined_by_id,
            status: dao.base.base.status,
            defined_by: dao.defined_by,
        })
    }
}

#[ta_objects::Dao]
#[dao(
    view = "functions__with_names",
    scd2,
    project_scoped(
        unique_at(CollectionIdName, FunctionIdName),
        unique_at(CollectionId, FunctionIdName)
    )
)]
pub struct FunctionDBWithNames {
    #[dao(flatten)]
    pub base: FunctionDB,
    pub collection: CollectionName,
    /// Group snapshot of the function's collection at view-query time.
    /// Threaded into LFR/LTDV stamping via FunctionNode at plan-build.
    /// No default: the group is project-scoped and must be set explicitly.
    pub group_id: GroupId,
    pub defined_by: UserLabel,
}

impl FunctionDBWithNames {
    pub fn executable() -> impl SqlCondition<Self> {
        and!(FunctionStatus::Active, Enabled::TRUE)
    }
}

/// Head versions and patches unified — the only patch-aware query surface;
/// every other function DAO stays patch-blind. Patch rows borrow the identity
/// columns they don't own (name, decorator, storage fields, ...) from their
/// parent head version and carry `patch_of`; head rows have it NULL.
#[ta_objects::Dao]
#[dao(view = "function_versions__with_names", timeline, project_scoped)]
pub struct FunctionVersionDBWithNames {
    #[dao(pk)]
    pub id: FunctionVersionId,
    pub collection_id: CollectionId,
    pub name: FunctionName,
    pub description: Description,
    pub decorator: Decorator,
    pub connector: Option<Connector>,
    pub connection: Option<ConnectionKind>,
    pub runtime_values: FunctionRuntimeValues,
    #[dao(partition_by)]
    pub function_id: FunctionId,
    pub base_path: Option<BasePath>,
    pub stage_id: StageId,
    pub stage_base_path: BasePath,
    pub storage_version: StorageVersion,
    pub bundle_id: BundleId,
    pub snippet: Snippet,
    #[dao(order_by, timeline(version_by))]
    pub valid_from: Scd2ValidFrom,
    pub defined_by_id: UserId,
    pub status: FunctionStatus,
    pub enabled: Enabled,
    pub function_type: FunctionType,
    pub parent_function_id: Option<Parent<FunctionId>>,
    pub collection: CollectionName,
    pub defined_by: UserLabel,
    pub patch_of: Option<FunctionVersionId>,
}

impl FunctionVersionDBWithNames {}

/// One version-history row; `patch_of` is the parent head fvid on patch
/// rows and NULL on head versions.
#[ta_objects::Dto]
#[dto(list(on = FunctionVersionDBWithNames))]
pub struct FunctionVersion {
    #[dto(list(pagination_by = "+"))]
    pub id: FunctionVersionId,
    pub collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    #[dto(list(filter, order_by))]
    pub decorator: Decorator,
    #[dto(list(filter, order_by))]
    pub function_type: FunctionType,
    #[dto(list(filter, order_by))]
    pub parent_function_id: Option<Parent<FunctionId>>,
    #[dto(list(filter, filter_like, order_by))]
    pub connector: Option<Connector>,
    #[dto(list(filter, filter_like, order_by))]
    pub connection: Option<ConnectionKind>,
    pub runtime_values: FunctionRuntimeValues,
    pub function_id: FunctionId,
    pub base_path: Option<BasePath>,
    pub stage_id: StageId,
    pub stage_base_path: BasePath,
    pub storage_version: StorageVersion,
    pub bundle_id: BundleId,
    pub snippet: Snippet,
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: FunctionStatus,
    pub enabled: Enabled,

    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserLabel,
    #[dto(list(filter, order_by))]
    pub patch_of: Option<FunctionVersionId>,
}

impl FromDao for FunctionVersion {
    type Dao = FunctionVersionDBWithNames;

    fn from_dao(dao: FunctionVersionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.id,
            collection_id: dao.collection_id,
            name: dao.name,
            description: dao.description,
            decorator: dao.decorator,
            function_type: dao.function_type,
            parent_function_id: dao.parent_function_id,
            connector: dao.connector,
            connection: dao.connection,
            runtime_values: dao.runtime_values,
            function_id: dao.function_id,
            base_path: dao.base_path,
            stage_id: dao.stage_id,
            stage_base_path: dao.stage_base_path,
            storage_version: dao.storage_version,
            bundle_id: dao.bundle_id,
            snippet: dao.snippet,
            defined_on: dao.valid_from.into(),
            defined_by_id: dao.defined_by_id,
            status: dao.status,
            enabled: dao.enabled,
            collection: dao.collection,
            defined_by: dao.defined_by,
            patch_of: dao.patch_of,
        })
    }
}

/// Not a Dao — a logical-layer projection: a single `functions ⋈ bundles`
/// query SELECTs all FunctionDB columns plus
/// `COALESCE(bundles.base_path, functions.stage_base_path) AS bundle_base_path`,
/// so the stage-trigger builder avoids a second per-bundle fetch.
#[ta_objects::Dlo]
#[derive(sqlx::FromRow)]
pub struct FunctionDBWithBundleBasePath {
    #[sqlx(flatten)]
    pub base: FunctionDB,
    pub bundle_base_path: BasePath,
}

#[ta_objects::Dto]
#[dto(list(on = FunctionDBWithNames))]
pub struct Function {
    #[dto(list(pagination_by = "+"))]
    pub id: FunctionVersionId,
    pub collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    #[dto(list(filter, order_by))]
    pub decorator: Decorator,
    #[dto(list(filter, order_by))]
    pub function_type: FunctionType,
    #[dto(list(filter, order_by))]
    pub parent_function_id: Option<Parent<FunctionId>>,
    #[dto(list(filter, filter_like, order_by))]
    pub connector: Option<Connector>,
    #[dto(list(filter, filter_like, order_by))]
    pub connection: Option<ConnectionKind>,
    pub runtime_values: FunctionRuntimeValues,
    pub function_id: FunctionId,
    pub base_path: Option<BasePath>,
    pub stage_id: StageId,
    pub stage_base_path: BasePath,
    pub storage_version: StorageVersion,
    pub bundle_id: BundleId,
    pub snippet: Snippet,
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: FunctionStatus,
    pub enabled: Enabled,

    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    pub group_id: GroupId,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserLabel,
}

impl FromDao for Function {
    type Dao = FunctionDBWithNames;

    fn from_dao(dao: FunctionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            collection_id: dao.base.base.collection_id,
            name: dao.base.base.name,
            description: dao.base.base.description,
            decorator: dao.base.base.decorator,
            function_type: dao.base.base.function_type,
            parent_function_id: dao.base.base.parent_function_id,
            connector: dao.base.base.connector,
            connection: dao.base.base.connection,
            runtime_values: dao.base.base.runtime_values,
            function_id: dao.base.base.function_id,
            base_path: dao.base.base.base_path,
            stage_id: dao.base.base.stage_id,
            stage_base_path: dao.base.base.stage_base_path,
            storage_version: dao.base.base.storage_version,
            bundle_id: dao.base.base.bundle_id,
            snippet: dao.base.base.snippet,
            defined_on: dao.base.interval.valid_from.into(),
            defined_by_id: dao.base.base.defined_by_id,
            status: dao.base.base.status,
            enabled: dao.base.base.enabled,
            collection: dao.collection,
            group_id: dao.group_id,
            defined_by: dao.defined_by,
        })
    }
}

#[ta_objects::Dto]
pub struct FunctionWithTables {
    #[serde(flatten)]
    pub function: Function,
    pub dependencies: Vec<TableDependency>,
    pub triggers: Vec<TableTrigger>,
    pub workers: HashMap<WorkerTypeDto, WorkerId>,
    pub tables: Vec<TableName>,
}

#[ta_objects::UrlParam]
pub struct FunctionParam {
    pub function: FunctionIdName,
}

#[ta_objects::UrlParam]
pub struct FunctionVersionParam {
    pub function_version: FunctionVersionId,
}

#[cfg(test)]
mod child_function_tests {
    use super::ChildFunction;
    use crate::types::basic::FunctionType;

    #[test]
    fn test_a_child_function_type_defaults_to_regular() {
        let json = r#"{"name":"c","bundle_id":"00000000000000000000000000","snippet":"s","tables":["t"],"parent_table":"p"}"#;
        let child: ChildFunction = serde_json::from_str(json).unwrap();
        assert!(matches!(child.function_type, FunctionType::Regular));
    }
}
