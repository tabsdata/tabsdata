//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::function::FunctionDB;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, FunctionId, System, TableId, TriggerId,
        TriggerStatus, TriggerVersionId, UserId,
    };
    use ta_objects::or;

    #[ta_objects::Dao]
    #[dao(
        sql_table = "triggers",
        versioned(
            version_by = "defined_on",
            partition_by = "trigger_id",
            pk = "id",
            current = "triggers_current",
        ),
        recursive(up = "trigger_by_function_id", down = "function_id"),
        states(
            // Active triggers are available to be used in executions.
            Active = TriggerStatus::Active,
            // Frozen triggers mean the trigger exists, but cannot be used in executions.
            Frozen = TriggerStatus::Frozen,
            // Available triggers are all the triggers that are not deleted.
            Available = or!(TriggerStatus::Active, TriggerStatus::Frozen),
        )
    )]
    #[dxo(
        builder(try_from = FunctionDB, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct TriggerDB {
        #[builder(default)]
        pub id: TriggerVersionId,
        #[dxo(builder(include))]
        pub collection_id: CollectionId,
        #[builder(default)]
        pub trigger_id: TriggerId,
        #[dxo(builder(include, field = "function_id"))]
        pub function_id: FunctionId,
        pub trigger_by_collection_id: CollectionId,
        pub trigger_by_function_id: FunctionId,
        pub trigger_by_table_id: TableId,
        pub status: TriggerStatus,
        #[dxo(updater(include, field = "time"))]
        pub defined_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub defined_by_id: UserId,
        pub system: System,
    }

    #[ta_objects::Dao]
    #[dao(
        sql_table = "triggers__with_names",
        versioned(current = "triggers_current__with_names")
    )]
    #[inherits(TriggerDB)]
    pub struct TriggerDBWithNames {
        #[dxo(extractor)]
        pub trigger_by_table_id: TableId,

        pub collection: CollectionName,
        pub trigger_by_collection: CollectionName,
    }
}
