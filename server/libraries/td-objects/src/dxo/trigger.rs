//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{
    CollectionId, CollectionName, FunctionId, ProjectId, System, TableId, TriggerBy, TriggerId,
    TriggerStatus, TriggerVersionId, UserId,
};
use ta_objects::sql_condition::SqlCondition;

#[ta_objects::Dao]
#[dao(table = "triggers", ops(insert), scd2, recursive, project_scoped)]
pub struct TriggerInsertDB {
    #[dao(pk)]
    pub id: TriggerVersionId,
    pub collection_id: CollectionId,
    #[dao(partition_by)]
    pub trigger_id: TriggerId,
    #[dao(recursive(down))]
    pub function_id: FunctionId,
    pub trigger_by_collection_id: TriggerBy<CollectionId>,
    #[dao(recursive(up))]
    pub trigger_by_function_id: TriggerBy<FunctionId>,
    pub trigger_by_table_id: TableId,
    pub status: TriggerStatus,
    pub defined_by_id: UserId,
    pub system: System,
}

#[ta_objects::Dao]
#[dao(table = "triggers", ops(select), scd2, recursive, project_scoped)]
pub struct TriggerDB {
    #[dao(flatten)]
    pub base: TriggerInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl TriggerDB {
    /// Triggers usable in executions.
    pub fn active() -> impl SqlCondition<Self> {
        TriggerStatus::Active
    }
}

#[ta_objects::Dao]
#[dao(view = "triggers__with_names", scd2, recursive, project_scoped)]
pub struct TriggerDBWithNames {
    #[dao(flatten)]
    pub base: TriggerDB,
    pub collection: CollectionName,
    pub trigger_by_collection: TriggerBy<CollectionName>,
}

impl TriggerDBWithNames {
    /// Triggers usable in executions.
    pub fn active() -> impl SqlCondition<Self> {
        TriggerStatus::Active
    }
}
