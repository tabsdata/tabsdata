//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::defs::FunctionDB;
    use crate::types::bool::{Private, System, Versioned};
    use crate::types::i32::TableFunctionParamPos;
    use crate::types::id::{
        CollectionId, FunctionId, FunctionVersionId, TableDataVersionId, TableId, TableVersionId,
        UserId,
    };
    use crate::types::string::{CollectionName, FunctionName, TableName, UserName};
    use crate::types::timestamp::AtTime;
    use crate::types::typed_enum::TableStatus;

    #[td_type::Dao]
    #[dao(
        sql_table = "tables",
        versioned(
            order_by = "defined_on",
            partition_by = "table_id",
        ),
        states(
            All = &[],
            Active = &[&TableStatus::Active],
            Frozen = &[&TableStatus::Frozen],
            Available = &[&TableStatus::Active, &TableStatus::Frozen],
            Output = &[&TableStatus::Active],
            InputDependency = &[&TableStatus::Active, &TableStatus::Frozen],
            Readable = &[&TableStatus::Active, &TableStatus::Frozen, &System::TRUE],
        )
    )]
    #[td_type(
        builder(try_from = TableDB),
        builder(try_from = FunctionDB, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct TableDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: TableVersionId,
        #[td_type(extractor, builder(include))]
        pub collection_id: CollectionId,
        #[td_type(extractor)]
        pub table_id: TableId,
        #[td_type(extractor)]
        pub name: TableName,
        #[td_type(builder(try_from = FunctionDB, field = "function_id"))]
        pub function_id: FunctionId,
        #[td_type(builder(try_from = FunctionDB, field = "id"))]
        pub function_version_id: FunctionVersionId,
        pub function_param_pos: Option<TableFunctionParamPos>,
        #[builder(default = "Private::FALSE")]
        pub private: Private,
        #[builder(default = "Versioned::FALSE")]
        pub partitioned: Versioned,
        #[td_type(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        pub status: TableStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "tables__with_names", order_by = "function_param_pos")]
    #[inherits(TableDB)]
    pub struct TableDBWithNames {
        #[td_type(extractor)]
        pub id: TableVersionId,
        #[td_type(extractor)]
        pub table_id: TableId,
        #[td_type(extractor)]
        pub name: TableName,

        pub system: System,
        pub defined_by: UserName,
        pub collection: CollectionName,
        pub function: FunctionName,
    }

    #[td_type::Dao]
    #[dao(sql_table = "tables__read")]
    #[inherits(TableDBWithNames)]
    pub struct TableDBRead {
        pub last_data_version: Option<TableDataVersionId>,
        // pub last_data_changed_version: Option<TableDataVersionId>,
    }

    #[td_type::Dto]
    #[dto(list(on = TableDBRead))]
    #[td_type(builder(try_from = TableDBRead))]
    #[inherits(TableDBRead)]
    pub struct Table {
        #[dto(list(pagination_by = "+"))]
        pub id: TableVersionId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: TableName,
        #[td_type(builder(include, field = "collection"))]
        #[dto(list(filter, filter_like, order_by))]
        pub collection_name: CollectionName,
        #[td_type(builder(include, field = "function"))]
        #[dto(list(filter, filter_like, order_by))]
        pub function_name: FunctionName,
    }
}
