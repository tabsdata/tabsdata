//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::types::bool::ReuseFrozen;
    use crate::types::composed::{
        TableDependency, TableDependencyDto, TableTrigger, TableTriggerDto,
    };
    use crate::types::id::{BundleId, CollectionId, FunctionId, FunctionVersionId, UserId};
    use crate::types::string::{
        CollectionName, Connector, DataLocation, Description, FunctionName, FunctionRuntimeValues,
        Snippet, StorageVersion, TableName, TableNameDto, UserName,
    };
    use crate::types::timestamp::AtTime;
    use crate::types::typed_enum::{Decorator, FunctionStatus};

    #[td_type::Dto]
    pub struct FunctionRegister {
        #[td_type(extractor)]
        pub name: FunctionName,
        pub description: Description,
        pub bundle_id: BundleId,
        pub snippet: Snippet,
        pub decorator: Decorator,
        #[builder(default)]
        pub connector: Option<Connector>,
        #[td_type(extractor)]
        pub dependencies: Option<Vec<TableDependencyDto>>,
        #[td_type(extractor)]
        pub triggers: Option<Vec<TableTriggerDto>>,
        #[td_type(extractor)]
        pub tables: Option<Vec<TableNameDto>>,
        #[serde(default)]
        pub runtime_values: FunctionRuntimeValues,
        #[td_type(extractor)]
        pub reuse_frozen_tables: ReuseFrozen,
    }

    pub type FunctionUpdate = FunctionRegister;

    #[td_type::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "functions",
        versioned(
            order_by = "defined_on",
            partition_by = "function_id",
        ),
        states(
            Active = &[&FunctionStatus::Active],
            Available = &[&FunctionStatus::Active, &FunctionStatus::Frozen],
            DownstreamTrigger = &[&FunctionStatus::Active],
            Readable = &[&FunctionStatus::Active, &FunctionStatus::Frozen],
        )
    )]
    #[td_type(
        builder(try_from = FunctionDB),
        builder(try_from = FunctionRegister, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct FunctionDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: FunctionVersionId,
        #[td_type(setter)]
        pub collection_id: CollectionId,
        #[td_type(builder(include))]
        pub name: FunctionName,
        #[td_type(builder(include))]
        pub description: Description,
        #[td_type(builder(include))]
        pub decorator: Decorator,
        #[td_type(builder(include))]
        pub connector: Option<Connector>,
        #[td_type(builder(include))]
        pub runtime_values: FunctionRuntimeValues,
        #[builder(default)]
        #[td_type(setter, extractor)]
        pub function_id: FunctionId,
        #[td_type(setter)]
        pub data_location: DataLocation,
        #[td_type(setter)]
        pub storage_version: StorageVersion,
        #[td_type(builder(include), extractor)]
        pub bundle_id: BundleId,
        #[td_type(builder(include))]
        pub snippet: Snippet,
        #[td_type(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        #[builder(default = FunctionStatus::Active)]
        pub status: FunctionStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "functions__with_names")]
    #[inherits(FunctionDB)]
    pub struct FunctionDBWithNames {
        #[td_type(extractor)]
        pub id: FunctionVersionId,
        #[td_type(extractor)]
        pub collection_id: CollectionId,
        #[td_type(extractor)]
        pub function_id: FunctionId,

        pub collection: CollectionName,
        pub defined_by: UserName,
    }

    #[td_type::Dto]
    #[dto(list(on = FunctionDBWithNames))]
    #[td_type(builder(try_from = FunctionDBWithNames))]
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

    #[td_type::Dto]
    #[td_type(builder(try_from = Function))]
    #[inherits(Function)]
    pub struct FunctionWithTables {
        #[td_type(builder(skip), setter)]
        pub dependencies: Vec<TableDependency>,
        #[td_type(builder(skip), setter)]
        pub triggers: Vec<TableTrigger>,
        #[td_type(builder(skip), setter)]
        pub tables: Vec<TableName>,
    }

    #[td_type::Dto]
    #[td_type(builder(try_from = FunctionWithTables))]
    #[inherits(FunctionWithTables)]
    pub struct FunctionWithAllVersions {
        #[td_type(builder(skip), setter)]
        pub all: Vec<Function>,
    }
}
