//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::FunctionDB;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, FunctionId, System, TableId, TriggerId,
        TriggerStatus, TriggerVersionId, UserId,
    };

    #[td_type::Dao]
    #[dao(
        sql_table = "triggers",
        versioned(
            order_by = "defined_on",
            partition_by = "trigger_id",
        ),
        recursive(up = "trigger_by_function_id", down = "function_id"),
        states(
            Active = &[&TriggerStatus::Active],
            Available = &[&TriggerStatus::Active, &TriggerStatus::Frozen],
            Frozen = &[&TriggerStatus::Frozen],
            UserDefined = &[&System::FALSE],
        )
    )]
    #[td_type(
        builder(try_from = FunctionDB, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct TriggerDB {
        #[builder(default)]
        pub id: TriggerVersionId,
        #[td_type(builder(include))]
        pub collection_id: CollectionId,
        #[builder(default)]
        pub trigger_id: TriggerId,
        #[td_type(builder(include, field = "function_id"))]
        pub function_id: FunctionId,
        pub trigger_by_collection_id: CollectionId,
        pub trigger_by_function_id: FunctionId,
        pub trigger_by_table_id: TableId,
        pub status: TriggerStatus,
        #[td_type(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        pub system: System,
    }

    #[td_type::Dao]
    #[dao(sql_table = "triggers__with_names")]
    #[inherits(TriggerDB)]
    pub struct TriggerDBWithNames {
        #[td_type(extractor)]
        pub trigger_by_table_id: TableId,

        pub collection: CollectionName,
        pub trigger_by_collection: CollectionName,
    }
}
