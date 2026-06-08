//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, BundleId, CollectionId, CollectionName, Connector, DataLocation, Decorator,
        Description, Enabled, FunctionId, FunctionName, FunctionRuntimeValues, FunctionStatus,
        FunctionVersionId, GroupId, ProjectId, ResetState, ReuseFrozen, Snippet, StorageVersion,
        TableName,
        TableNameDto, TableType, UserId, UserName, WorkerConfig, WorkerId, WorkerTypeDto,
    };
    use crate::types::composed::{
        TableDependency, TableDependencyDto, TableTrigger, TableTriggerDto,
    };
    use std::collections::HashMap;
    use ta_objects::{and, or};

    #[ta_objects::Dto]
    pub struct FunctionRegister {
        #[dxo(extractor)]
        pub name: FunctionName,
        pub description: Description,
        pub bundle_id: BundleId,
        pub snippet: Snippet,
        pub decorator: Decorator,
        #[builder(default)]
        pub connector: Option<Connector>,
        #[dxo(extractor)]
        pub dependencies: Option<Vec<TableDependencyDto>>,
        #[dxo(extractor)]
        pub triggers: Option<Vec<TableTriggerDto>>,
        #[dxo(extractor)]
        pub tables: Option<Vec<TableNameDto>>,
        #[serde(default)]
        #[builder(default)]
        #[dxo(extractor)]
        pub table_types: Option<HashMap<TableNameDto, TableType>>, // added in v4
        #[serde(default)]
        pub runtime_values: FunctionRuntimeValues,
        #[dxo(extractor)]
        pub reuse_frozen_tables: ReuseFrozen,
        #[serde(default)]
        #[builder(default)]
        #[dxo(extractor)]
        pub enabled: Option<Enabled>, // added in v4
        #[serde(default)]
        #[builder(default)]
        #[dxo(extractor)]
        pub workers: Option<HashMap<WorkerTypeDto, WorkerConfig>>, // added in v4
    }

    #[ta_objects::Dto]
    #[derive(Default)]
    #[serde(default)]
    #[builder(default)]
    pub struct FunctionUpdate {
        pub name: Option<FunctionName>,
        pub description: Option<Description>,
        pub bundle_id: Option<BundleId>,
        pub snippet: Option<Snippet>,
        pub decorator: Option<Decorator>,
        pub connector: Option<Option<Connector>>,
        #[dxo(extractor)]
        pub dependencies: Option<Vec<TableDependencyDto>>,
        #[dxo(extractor)]
        pub triggers: Option<Vec<TableTriggerDto>>,
        #[dxo(extractor)]
        pub tables: Option<Vec<TableNameDto>>,
        #[dxo(extractor)]
        pub table_types: Option<HashMap<TableNameDto, TableType>>, // added in v4
        pub runtime_values: Option<FunctionRuntimeValues>,
        #[dxo(extractor)]
        pub reuse_frozen_tables: ReuseFrozen,
        pub enabled: Option<Enabled>, // added in v4
        #[dxo(extractor)]
        pub workers: Option<HashMap<WorkerTypeDto, WorkerConfig>>, // added in v4
        /// When true, regenerate the function's state table id instead of
        /// carrying it forward, so publishers re-read initial config values.
        #[dxo(extractor)]
        pub reset_state: ResetState,
    }

    #[ta_objects::Dto]
    pub struct FunctionEnable {
        pub enabled: Enabled, // added in v4
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
        #[dxo(extractor)]
        pub patch_of: FunctionVersionId,
        pub bundle_id: BundleId,
        pub snippet: Snippet,
        #[serde(default)]
        #[builder(default)]
        pub runtime_values: Option<FunctionRuntimeValues>,
        #[serde(default)]
        #[builder(default)]
        pub description: Option<Description>,
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
        ),
        states(
            // Active functions are functions which are not deleted that have dependencies Active.
            Active = FunctionStatus::Active,
            // Available functions are functions that are not deleted and can be read.
            Available = or!(FunctionStatus::Active, FunctionStatus::Frozen),
            // To execute a function, we need it to be Active (not Frozen or Deleted) and Enabled.
            // This state is required upfront for recursive queries to generate proper views.
            Executable = and!(FunctionStatus::Active, Enabled::TRUE),
        )
    )]
    #[dxo(
        builder(try_from = FunctionDB),
        builder(try_from = FunctionRegister, skip_all),
        updater(try_from = FunctionEnable, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct FunctionDB {
        #[builder(default = "FunctionVersionId::new()")]
        #[dxo(extractor)]
        pub id: FunctionVersionId,
        /// The project this function belongs to. Inherited from the parent
        /// collection at function registration; required at construction.
        #[dxo(extractor, setter)]
        pub project_id: ProjectId,
        #[dxo(setter)]
        pub collection_id: CollectionId,
        #[dxo(builder(include))]
        pub name: FunctionName,
        #[dxo(builder(include))]
        pub description: Description,
        #[dxo(builder(include))]
        pub decorator: Decorator,
        #[dxo(builder(include))]
        pub connector: Option<Connector>,
        #[dxo(builder(include))]
        pub runtime_values: FunctionRuntimeValues,
        #[builder(default)]
        #[dxo(setter, extractor)]
        pub function_id: FunctionId,
        #[dxo(setter)]
        pub data_location: DataLocation,
        #[dxo(setter)]
        pub storage_version: StorageVersion,
        #[dxo(extractor)]
        #[dxo(builder(include))]
        pub bundle_id: BundleId,
        #[dxo(builder(include))]
        pub snippet: Snippet,
        #[dxo(updater(try_from = RequestContext, field = "time"))]
        pub defined_on: AtTime,
        #[dxo(updater(try_from = RequestContext, field = "user_id"))]
        pub defined_by_id: UserId,
        #[builder(default = FunctionStatus::Active)]
        pub status: FunctionStatus,
        #[dxo(setter, updater(include, try_from = FunctionEnable))]
        pub enabled: Enabled, // added in v4
    }

    /// Patch DAO — code-only override of a parent function version.
    /// Lives in its own `function_patches` table so every "find latest
    /// version" query against `functions` is patch-blind by
    /// construction. Reachable only by explicit fvid (replay) or via
    /// the patches list endpoint.
    ///
    /// Versioned by `parent_function_version_id`: at any point in time
    /// each head fvid has at most one current patch (latest
    /// `defined_on`, `status = Active`). Mint reads through the current
    /// view via `conn.versioned(at).fetch_*` so a replay scheduled at
    /// T1 sees the patch active at T1 — not whatever lands between
    /// request and dispatch. List-patches reads historic instead.
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
        ),
        states(
            Active = FunctionStatus::Active,
        )
    )]
    pub struct FunctionPatchDB {
        #[builder(default = "FunctionVersionId::new()")]
        #[dxo(extractor)]
        pub id: FunctionVersionId,
        pub parent_function_version_id: FunctionVersionId,
        #[dxo(extractor)]
        pub function_id: FunctionId,
        #[dxo(extractor)]
        pub collection_id: CollectionId,
        pub snippet: Snippet,
        #[dxo(extractor)]
        pub bundle_id: BundleId,
        pub runtime_values: FunctionRuntimeValues,
        pub description: Description,
        pub defined_on: AtTime,
        pub defined_by_id: UserId,
        #[builder(default = FunctionStatus::Active)]
        pub status: FunctionStatus,
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

    #[ta_objects::Dao]
    #[dao(
        sql_table = "functions__with_names",
        versioned(current = "functions_current__with_names")
    )]
    #[inherits(FunctionDB)]
    pub struct FunctionDBWithNames {
        #[dxo(extractor)]
        pub id: FunctionVersionId,
        #[dxo(extractor)]
        pub collection_id: CollectionId,
        #[dxo(extractor)]
        pub function_id: FunctionId,
        #[dxo(extractor)]
        pub enabled: Enabled, //  added in v4

        pub collection: CollectionName,
        /// Group snapshot of the function's collection at view-query time.
        /// Threaded into LFR/LTDV stamping via FunctionNode at plan-build.
        /// Test builders default to `Default`.
        #[builder(default = "GroupId::default_group()")]
        pub group_id: GroupId,
        pub defined_by: UserName,
    }

    #[ta_objects::Dto]
    #[dto(list(on = FunctionDBWithNames))]
    #[inherits(FunctionDBWithNames)]
    pub struct Function {
        #[dto(list(pagination_by = "+"))]
        pub id: FunctionVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: FunctionName,
        #[dto(list(filter, filter_like, order_by))]
        pub description: Description,
        #[dto(list(filter, order_by))]
        pub decorator: Decorator,
        #[dto(list(filter, filter_like, order_by))]
        pub connector: Option<Connector>,
        #[dto(list(filter, order_by))]
        pub defined_on: AtTime,
        #[dto(list(filter, order_by))]
        pub status: FunctionStatus,

        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like, order_by))]
        pub defined_by: UserName,
    }

    #[ta_objects::Dto]
    #[dxo(builder(try_from = Function))]
    #[inherits(Function)]
    pub struct FunctionWithTables {
        #[dxo(builder(skip), setter)]
        pub dependencies: Vec<TableDependency>,
        #[dxo(builder(skip), setter)]
        pub triggers: Vec<TableTrigger>,
        #[dxo(builder(skip), setter)]
        pub workers: HashMap<WorkerTypeDto, WorkerId>,
        #[dxo(builder(skip), setter)]
        pub tables: Vec<TableName>,
    }
}
