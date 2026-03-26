//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::execution::{FunctionNodeResponse, TableNodeResponse};
    use crate::dxo::function::FunctionDB;
    use crate::dxo::graph::GraphEdge;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, FunctionId, FunctionName, FunctionVersionId,
        Partitioned, Private, System, TableFunctionParamPos, TableId, TableName, TableStatus,
        TableType, TableVersionId, UserId, UserName,
    };
    use crate::types::composed::TableVersions;
    use std::collections::{HashMap, HashSet};
    use ta_objects::{and, or};

    #[ta_objects::Dao]
    #[dao(
        sql_table = "tables",
        versioned(
            pk = "id",
            version_by = "defined_on",
            partition_by = "table_id",
            current = "tables_current",
        ),
        states(
            // Active tables are tables that can be generated and consumed.
            Active = TableStatus::Active,
            // Frozen tables are tables that cannot be generated, but can be consumed.
            Frozen = TableStatus::Frozen,
            // Available tables are all the tables that are not deleted.
            Available = or!(TableStatus::Active, TableStatus::Frozen),
        )
    )]
    #[dxo(
        builder(try_from = TableDB),
        builder(try_from = FunctionDB, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct TableDB {
        #[builder(default)]
        #[dxo(extractor)]
        pub id: TableVersionId,
        #[dxo(extractor, builder(include))]
        pub collection_id: CollectionId,
        #[dxo(extractor)]
        pub table_id: TableId,
        #[dxo(extractor)]
        pub name: TableName,
        #[dxo(setter, builder(try_from = FunctionDB, field = "function_id"))]
        pub function_id: FunctionId,
        #[dxo(setter, builder(try_from = FunctionDB, field = "id"))]
        pub function_version_id: FunctionVersionId,
        pub function_param_pos: Option<TableFunctionParamPos>,
        #[builder(default = "Private::FALSE")]
        pub private: Private,
        #[builder(default = "Partitioned::FALSE")]
        pub partitioned: Partitioned,
        #[dxo(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        pub status: TableStatus,
        #[builder(default)]
        pub table_type: TableType, // added in v4
    }

    #[ta_objects::Dao]
    #[dao(
        sql_table = "tables__with_names",
        order_by(col = "function_param_pos", dir = "desc"),
        versioned(current = "tables_current__with_names"),
        states(
            // Available user tables are all the tables that are not deleted.
            User = and!(or!(TableStatus::Active, TableStatus::Frozen), System::FALSE),
            // Available system tables are all the tables that are not deleted.
            System = and!(or!(TableStatus::Active, TableStatus::Frozen), System::TRUE),
        )
    )]
    #[inherits(TableDB)]
    pub struct TableDBWithNames {
        #[dxo(extractor)]
        pub id: TableVersionId,
        #[dxo(extractor)]
        pub table_id: TableId,
        #[dxo(extractor)]
        pub name: TableName,
        #[dxo(extractor)]
        pub function_id: FunctionId,

        pub system: System,
        pub defined_by: UserName,
        pub collection: CollectionName,
        pub function: FunctionName,
    }

    #[ta_objects::Dto]
    #[dto(list(on = TableDBWithNames))]
    #[inherits(TableDBWithNames)]
    pub struct Table {
        #[dto(list(pagination_by = "+"))]
        pub id: TableVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: TableName,
        #[dto(list(filter, filter_like, order_by))]
        pub collection: CollectionName,
        #[dto(list(filter, filter_like, order_by))]
        pub function: FunctionName,
    }

    #[ta_objects::Dto]
    pub struct TableFlowGraphResponse {
        pub table: TableVersionId,
        // functions info
        pub all_functions: HashMap<FunctionVersionId, FunctionNodeResponse>,
        // tables info
        pub all_tables: HashMap<TableVersionId, TableNodeResponse>,
        pub system_tables: HashSet<TableVersionId>,
        pub user_tables: HashSet<TableVersionId>,
        // relations info
        pub relations: Vec<(FunctionVersionId, TableVersionId, GraphEdge<TableVersions>)>,
    }
}
