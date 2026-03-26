//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::FunctionDB;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
        DependencyVersionId, FunctionId, System, TableId, UserId,
    };
    use crate::types::composed::TableVersions;
    use ta_objects::and;

    #[ta_objects::Dao]
    #[dao(
        sql_table = "dependencies",
        order_by(col = "dep_pos", dir = "desc"),
        versioned(pk = "id", version_by = "defined_on", partition_by = "dependency_id", current = "dependencies_current"),
        recursive(up = "table_function_id", down = "function_id"),
        states(
            // Active dependencies are dependencies that can be used in executions.
            Active = DependencyStatus::Active,
            // Available dependencies are active user dependencies.
            User = and!(DependencyStatus::Active, System::FALSE),
        )
    )]
    #[dxo(
        builder(try_from = FunctionDB, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct DependencyDB {
        #[builder(default)]
        pub id: DependencyVersionId,
        #[dxo(builder(include))]
        pub collection_id: CollectionId,
        #[builder(default)]
        #[dxo(extractor)]
        pub dependency_id: DependencyId,
        #[dxo(builder(include, field = "function_id"))]
        #[dxo(extractor)]
        pub function_id: FunctionId,
        pub table_collection_id: CollectionId,
        pub table_function_id: FunctionId,
        pub table_id: TableId,
        pub table_versions: TableVersions,
        pub dep_pos: DependencyPos,
        pub status: DependencyStatus,
        #[dxo(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        pub system: System,
    }

    #[ta_objects::Dao]
    #[dao(
        sql_table = "dependencies__with_names",
        versioned(current = "dependencies_current__with_names")
    )]
    #[inherits(DependencyDB)]
    pub struct DependencyDBWithNames {
        #[dxo(extractor)]
        pub table_id: TableId,

        pub collection: CollectionName,
        pub table_collection: CollectionName,
    }
}
