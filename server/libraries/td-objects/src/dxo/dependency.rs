//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
    DependencyVersionId, FunctionId, System, TableId, UserId,
};
use crate::types::composed::TableVersions;
use ta_objects::StateFilter;
use ta_objects::and;

#[ta_objects::Dao]
#[dao(
    sql_table = "dependencies",
    order_by(col = "dep_pos", dir = "desc"),
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "dependency_id",
        current = "dependencies_current"
    ),
    recursive(up = "table_function_id", down = "function_id")
)]
pub struct DependencyDB {
    pub id: DependencyVersionId,
    pub collection_id: CollectionId,
    pub dependency_id: DependencyId,
    pub function_id: FunctionId,
    pub table_collection_id: CollectionId,
    pub table_function_id: FunctionId,
    pub table_id: TableId,
    pub table_versions: TableVersions,
    pub dep_pos: DependencyPos,
    pub status: DependencyStatus,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub system: System,
}

impl DependencyDB {
    /// Dependencies usable in executions.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(DependencyStatus::Active)
    }

    /// A new Deleted version of this dependency.
    pub fn deleted(&self, ctx: &RequestContext) -> Self {
        Self {
            id: DependencyVersionId::default(),
            status: DependencyStatus::Deleted,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            ..self.clone()
        }
    }
}

#[ta_objects::Dao]
#[dao(
    sql_table = "dependencies__with_names",
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "dependency_id",
        current = "dependencies_current__with_names"
    ),
    recursive(up = "table_function_id", down = "function_id")
)]
pub struct DependencyDBWithNames {
    #[dao(flatten)]
    pub base: DependencyDB,
    pub collection: CollectionName,
    pub table_collection: CollectionName,
}

impl DependencyDBWithNames {
    /// Dependencies usable in executions.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(DependencyStatus::Active)
    }

    /// Active user (non-system) dependencies.
    pub fn user() -> StateFilter<Self> {
        StateFilter::of(and!(DependencyStatus::Active, System::FALSE))
    }
}
