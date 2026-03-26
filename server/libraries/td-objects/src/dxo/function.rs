//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, BundleId, CollectionId, CollectionName, Connector, DataLocation, Decorator,
        Description, Enabled, FunctionId, FunctionName, FunctionRuntimeValues, FunctionStatus,
        FunctionVersionId, ReuseFrozen, Snippet, StorageVersion, TableName, TableNameDto,
        TableType, UserId, UserName, WorkerConfig, WorkerTypeDto,
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
    }

    #[ta_objects::Dto]
    pub struct FunctionEnable {
        pub enabled: Enabled, // added in v4
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
        #[dto(list(filter, filter_like, order_by))]
        pub decorator: Decorator,
        #[dto(list(filter, filter_like, order_by))]
        pub connector: Option<Connector>,
        #[dto(list(filter, filter_like, order_by))]
        pub defined_on: AtTime,
        #[dto(list(filter, filter_like, order_by))]
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
        pub tables: Vec<TableName>,
    }

    #[ta_objects::Dto]
    #[dxo(builder(try_from = FunctionWithTables))]
    #[inherits(FunctionWithTables)]
    pub struct FunctionWithAllVersions {
        #[dxo(builder(skip), setter)]
        pub all: Vec<Function>,
    }
}
