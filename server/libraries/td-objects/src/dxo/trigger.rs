//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, System, TableId, TriggerBy, TriggerId,
    TriggerStatus, TriggerVersionId, UserId,
};
use ta_objects::StateFilter;
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
    recursive(up = "trigger_by_function_id", down = "function_id")
)]
pub struct TriggerDB {
    pub id: TriggerVersionId,
    pub collection_id: CollectionId,
    pub trigger_id: TriggerId,
    pub function_id: FunctionId,
    pub trigger_by_collection_id: TriggerBy<CollectionId>,
    pub trigger_by_function_id: TriggerBy<FunctionId>,
    pub trigger_by_table_id: TableId,
    pub status: TriggerStatus,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub system: System,
}

impl TriggerDB {
    /// Triggers usable in executions.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(TriggerStatus::Active)
    }

    /// A new Deleted version of this trigger.
    pub fn deleted(&self, ctx: &RequestContext) -> Self {
        Self {
            id: TriggerVersionId::default(),
            status: TriggerStatus::Deleted,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            ..self.clone()
        }
    }

    /// Every non-deleted trigger (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(TriggerStatus::Active, TriggerStatus::Frozen))
    }
}

#[ta_objects::Dao]
#[dao(
    sql_table = "triggers__with_names",
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "trigger_id",
        current = "triggers_current__with_names"
    ),
    recursive(up = "trigger_by_function_id", down = "function_id")
)]
pub struct TriggerDBWithNames {
    #[dao(flatten)]
    pub base: TriggerDB,
    pub collection: CollectionName,
    pub trigger_by_collection: TriggerBy<CollectionName>,
}

impl TriggerDBWithNames {
    /// Triggers usable in executions.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(TriggerStatus::Active)
    }

    /// Every non-deleted trigger (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(TriggerStatus::Active, TriggerStatus::Frozen))
    }
}
