//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::defs::FunctionDB;
    use crate::types::bool::System;
    use crate::types::composed::TableVersions;
    use crate::types::i32::DependencyPos;
    use crate::types::id::{
        CollectionId, DependencyId, DependencyVersionId, FunctionId, TableId, UserId,
    };
    use crate::types::string::CollectionName;
    use crate::types::timestamp::AtTime;
    use crate::types::typed_enum::DependencyStatus;

    #[td_type::Dao]
    #[dao(
        sql_table = "dependencies",
        order_by = "dep_pos",
        versioned(order_by = "defined_on", partition_by = "dependency_id"),
        recursive(up = "table_function_id", down = "function_id"),
        states(
            Active = &[&DependencyStatus::Active],
        )
    )]
    #[td_type(
        builder(try_from = FunctionDB, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct DependencyDB {
        #[builder(default)]
        pub id: DependencyVersionId,
        #[td_type(builder(include))]
        pub collection_id: CollectionId,
        #[builder(default)]
        #[td_type(extractor)]
        pub dependency_id: DependencyId,
        #[td_type(builder(include, field = "function_id"))]
        #[td_type(extractor)]
        pub function_id: FunctionId,
        pub table_collection_id: CollectionId,
        pub table_function_id: FunctionId,
        pub table_id: TableId,
        pub table_versions: TableVersions,
        pub dep_pos: DependencyPos,
        pub status: DependencyStatus,
        #[td_type(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        pub system: System,
    }

    #[td_type::Dao]
    #[dao(sql_table = "dependencies__with_names")]
    #[inherits(DependencyDB)]
    pub struct DependencyDBWithNames {
        pub collection: CollectionName,
        pub trigger_by_collection: CollectionName,
        pub table_collection: CollectionName,
    }

    #[td_type::Dao]
    #[dao(sql_table = "dependencies__read")]
    #[inherits(DependencyDBWithNames)]
    pub struct DependencyDBRead {
        #[td_type(extractor)]
        pub table_id: TableId,
    }
}
