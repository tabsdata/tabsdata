//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BasePath, BundleId, CollectionId, CollectionName, ConnectionKind, Connector, Decorator,
    Description, Enabled, FunctionId, FunctionIdName, FunctionName, FunctionRuntimeValues,
    FunctionStatus, FunctionVersionId, GroupId, RawSnippet, ResetStage, ResetState, ReuseFrozen,
    Snippet, StageId, StorageVersion, TableName, TableNameDto, TableType, UserId, UserName,
    WorkerConfig, WorkerId, WorkerTypeDto,
};
use crate::types::composed::{TableDependency, TableDependencyDto, TableTrigger, TableTriggerDto};
use std::collections::HashMap;
use ta_objects::{FromDao, StateFilter, and, or};
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
    // Optional storage-prefix override for this function's output table data.
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
}

impl FunctionRegister {
    pub fn to_dao(
        &self,
        ctx: &RequestContext,
        collection_id: CollectionId,
        stage_base_path: BasePath,
    ) -> Result<FunctionDB, TdError> {
        Ok(FunctionDB {
            id: FunctionVersionId::new(),
            collection_id,
            name: self.name.clone(),
            description: self.description.clone(),
            decorator: self.decorator.clone(),
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
            defined_on: ctx.time,
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
}

impl FunctionUpdate {
    /// The new function version row for an update: the previous version's
    /// fields with a fresh version id, Active status, a fresh storage
    /// version, and the update's changes applied on top.
    pub fn to_dao(
        &self,
        previous: &FunctionDB,
        ctx: &RequestContext,
    ) -> Result<FunctionDB, TdError> {
        let mut function = FunctionDB {
            id: FunctionVersionId::new(),
            status: FunctionStatus::Active,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            storage_version: StorageVersion::default(),
            ..previous.clone()
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
        // reset-stage: fresh stage id (new stage/checkpoint dir). The stage_base_path is
        // carried forward from the previous version, so the fresh stage lands at the same
        // stamped prefix — it is not relocated by an unrelated collection base_path change.
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

/// Body for `POST /api/v1/collections/{coll}/functions/{name}/patch`.
///
/// A patch is a code-only override of a specific `FunctionVersionId`
/// (`patch_of`). It shares the parent's topology — no `tables`,
/// `dependencies`, `triggers`, `workers` — and is reachable only by
/// explicit fvid (replay). Fields not set fall back to parent values.
#[ta_objects::Dto]
pub struct FunctionPatch {
    /// The function version this patches. Required.
    pub patch_of: FunctionVersionId,
    pub bundle_id: BundleId,
    pub snippet: RawSnippet,
    #[serde(default)]
    pub runtime_values: Option<FunctionRuntimeValues>,
    #[serde(default)]
    pub description: Option<Description>,
}

impl FunctionPatch {
    /// The new patch row: fields not set in the body fall back to the
    /// parent function version's values.
    pub fn to_dao(
        self,
        parent: FunctionDB,
        ctx: &RequestContext,
    ) -> Result<FunctionPatchDB, TdError> {
        Ok(FunctionPatchDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: parent.id,
            function_id: parent.function_id,
            collection_id: parent.collection_id,
            snippet: Snippet::try_from(&self.snippet)?,
            bundle_id: self.bundle_id,
            runtime_values: self.runtime_values.unwrap_or(parent.runtime_values),
            description: self.description.unwrap_or(parent.description),
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            status: FunctionStatus::Active,
        })
    }
}

#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(
    sql_table = "functions",
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "function_id",
        current = "functions_current",
    )
)]
pub struct FunctionDB {
    pub id: FunctionVersionId,
    pub collection_id: CollectionId,
    pub name: FunctionName,
    pub description: Description,
    pub decorator: Decorator,
    pub connector: Option<Connector>,
    pub connection: Option<ConnectionKind>,
    pub runtime_values: FunctionRuntimeValues,
    pub function_id: FunctionId,
    // Optional storage-prefix override for this version's output table data.
    // NULL → inherit the collection base_path at mint. Mapped from
    // `FunctionRegister::base_path`; carried forward from the previous version on
    // update unless `FunctionUpdate::base_path` overrides it.
    pub base_path: Option<BasePath>,
    // Stage/checkpoint identity. Fresh at register, carried forward on update
    // (via try_from FunctionDB), regenerated when FunctionUpdate::reset_stage.
    pub stage_id: StageId,
    // Storage prefix for the stage/checkpoint dirs, stamped at register from the
    // collection base_path and carried forward, so a function's state does not
    // silently relocate when the collection base_path later changes.
    pub stage_base_path: BasePath,
    pub storage_version: StorageVersion,
    pub bundle_id: BundleId,
    pub snippet: Snippet,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub status: FunctionStatus,
    pub enabled: Enabled,
}

impl FunctionDB {
    /// Non-deleted functions with active dependencies.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(FunctionStatus::Active)
    }

    /// A new Deleted version of this function.
    pub fn deleted(&self, ctx: &RequestContext) -> Self {
        Self {
            id: FunctionVersionId::new(),
            status: FunctionStatus::Deleted,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            ..self.clone()
        }
    }

    /// A new Frozen version of this function.
    pub fn frozen(&self, ctx: &RequestContext) -> Self {
        Self {
            id: FunctionVersionId::new(),
            status: FunctionStatus::Frozen,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            ..self.clone()
        }
    }

    /// A new version of this function with `enabled` flipped.
    pub fn re_registered(&self, ctx: &RequestContext, enabled: Enabled) -> Self {
        Self {
            id: FunctionVersionId::new(),
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            enabled,
            ..self.clone()
        }
    }

    /// Every non-deleted, readable function (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(FunctionStatus::Active, FunctionStatus::Frozen))
    }

    /// Runnable functions: active (not frozen or deleted) and enabled.
    pub fn executable() -> StateFilter<Self> {
        StateFilter::of(and!(FunctionStatus::Active, Enabled::TRUE))
    }
}

/// Patch DAO — code-only override of a parent function version.
/// Lives in its own `function_patches` table so every "find latest
/// version" query against `functions` is patch-blind by
/// construction. Reachable only by explicit fvid (replay) or via
/// the patches list endpoint.
#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(
    sql_table = "function_patches",
    order_by(col = "defined_on", dir = "desc"),
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "parent_function_version_id",
        current = "function_patches_current",
    )
)]
pub struct FunctionPatchDB {
    pub id: FunctionVersionId,
    pub parent_function_version_id: FunctionVersionId,
    pub function_id: FunctionId,
    pub collection_id: CollectionId,
    pub snippet: Snippet,
    pub bundle_id: BundleId,
    pub runtime_values: FunctionRuntimeValues,
    pub description: Description,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub status: FunctionStatus,
}

impl FunctionPatchDB {
    /// Non-deleted patches.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(FunctionStatus::Active)
    }
}

/// Public DTO for a patch — the response shape for patch create
/// and list-patches endpoints. Mirrors `FunctionPatchDB` 1:1;
/// clients resolve parent details separately via the parent fvid.
#[ta_objects::Dto]
#[dto(list(on = FunctionPatchDB))]
pub struct FunctionPatchRecord {
    #[dto(list(pagination_by = "-"))]
    pub id: FunctionVersionId,
    #[dto(list(filter, filter_like, order_by))]
    pub parent_function_version_id: FunctionVersionId,
    #[dto(list(filter, filter_like, order_by))]
    pub function_id: FunctionId,
    #[dto(list(filter, filter_like, order_by))]
    pub collection_id: CollectionId,
    pub snippet: Snippet,
    pub bundle_id: BundleId,
    pub runtime_values: FunctionRuntimeValues,
    #[dto(list(filter, filter_like))]
    pub description: Description,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_on: AtTime,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: FunctionStatus,
}

impl FromDao for FunctionPatchRecord {
    type Dao = FunctionPatchDB;

    fn from_dao(dao: FunctionPatchDB) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.id,
            parent_function_version_id: dao.parent_function_version_id,
            function_id: dao.function_id,
            collection_id: dao.collection_id,
            snippet: dao.snippet,
            bundle_id: dao.bundle_id,
            runtime_values: dao.runtime_values,
            description: dao.description,
            defined_on: dao.defined_on,
            defined_by_id: dao.defined_by_id,
            status: dao.status,
        })
    }
}

#[ta_objects::Dao]
#[dao(
    sql_table = "functions__with_names",
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "function_id",
        current = "functions_current__with_names"
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
    pub defined_by: UserName,
}

impl FunctionDBWithNames {
    /// Every non-deleted, readable function (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(FunctionStatus::Active, FunctionStatus::Frozen))
    }

    /// Runnable functions: active (not frozen or deleted) and enabled.
    pub fn executable() -> StateFilter<Self> {
        StateFilter::of(and!(FunctionStatus::Active, Enabled::TRUE))
    }
}

/// Head versions and patches unified, for version history and
/// fvid-addressed reads — the only patch-aware query surface; every
/// other function DAO stays patch-blind by construction. Patch rows
/// borrow the identity columns they don't own (name, decorator,
/// storage fields, ...) from their parent head version and carry
/// `patch_of`; head rows have it NULL.
#[ta_objects::Dao]
#[dao(
    sql_table = "function_versions__with_names",
    order_by(col = "defined_on", dir = "desc"),
    versioned(pk = "id", version_by = "defined_on", partition_by = "function_id")
)]
pub struct FunctionVersionDBWithNames {
    pub id: FunctionVersionId,
    pub collection_id: CollectionId,
    pub name: FunctionName,
    pub description: Description,
    pub decorator: Decorator,
    pub connector: Option<Connector>,
    pub connection: Option<ConnectionKind>,
    pub runtime_values: FunctionRuntimeValues,
    pub function_id: FunctionId,
    pub base_path: Option<BasePath>,
    pub stage_id: StageId,
    pub stage_base_path: BasePath,
    pub storage_version: StorageVersion,
    pub bundle_id: BundleId,
    pub snippet: Snippet,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub status: FunctionStatus,
    pub enabled: Enabled,
    pub collection: CollectionName,
    pub defined_by: UserName,
    pub patch_of: Option<FunctionVersionId>,
}

impl FunctionVersionDBWithNames {
    /// Every non-deleted, readable function version (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(FunctionStatus::Active, FunctionStatus::Frozen))
    }
}

/// One row of a function's version history: head versions and
/// patches interleaved by `defined_on`. `patch_of` is the parent
/// head fvid on patch rows and NULL on head versions.
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
    #[dto(list(filter, order_by))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: FunctionStatus,
    pub enabled: Enabled,

    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserName,
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
            defined_on: dao.defined_on,
            defined_by_id: dao.defined_by_id,
            status: dao.status,
            enabled: dao.enabled,
            collection: dao.collection,
            defined_by: dao.defined_by,
            patch_of: dao.patch_of,
        })
    }
}

/// FunctionDB enriched with the effective bundle's stamped
/// `base_path`, populated by a single `functions ⋈ bundles` query so
/// the stage-trigger builder reads the bundle prefix off the same row
/// rather than issuing a second per-bundle fetch. Not a Dao — a
/// logical-layer projection (like PhysicalFunctionRunToExecute): the
/// query SELECTs all FunctionDB columns plus
/// `COALESCE(bundles.base_path, functions.stage_base_path) AS bundle_base_path`.
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
    #[dto(list(filter, order_by))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: FunctionStatus,
    pub enabled: Enabled,

    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    pub group_id: GroupId,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserName,
}

impl FromDao for Function {
    type Dao = FunctionDBWithNames;

    fn from_dao(dao: FunctionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            collection_id: dao.base.collection_id,
            name: dao.base.name,
            description: dao.base.description,
            decorator: dao.base.decorator,
            connector: dao.base.connector,
            connection: dao.base.connection,
            runtime_values: dao.base.runtime_values,
            function_id: dao.base.function_id,
            base_path: dao.base.base_path,
            stage_id: dao.base.stage_id,
            stage_base_path: dao.base.stage_base_path,
            storage_version: dao.base.storage_version,
            bundle_id: dao.base.bundle_id,
            snippet: dao.base.snippet,
            defined_on: dao.base.defined_on,
            defined_by_id: dao.base.defined_by_id,
            status: dao.base.status,
            enabled: dao.base.enabled,
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
pub struct FunctionVersionIdParam {
    pub function_version_id: FunctionVersionId,
}
