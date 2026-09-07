//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{
    CollectionId, CollectionName, DependencyId, DependencyPos, DependencyVersionId, FunctionId,
    OfTable, ProjectId, System, TableId, UserId,
};
use crate::types::composed::TableVersions;
use ta_objects::sql_condition::SqlCondition;

#[ta_objects::Dao]
#[dao(table = "dependencies", ops(insert), scd2, recursive, project_scoped)]
pub struct DependencyInsertDB {
    #[dao(pk)]
    pub id: DependencyVersionId,
    pub collection_id: CollectionId,
    #[dao(partition_by)]
    pub dependency_id: DependencyId,
    #[dao(recursive(down))]
    pub function_id: FunctionId,
    pub table_collection_id: OfTable<CollectionId>,
    #[dao(recursive(up))]
    pub table_function_id: OfTable<FunctionId>,
    pub table_id: TableId,
    pub table_versions: TableVersions,
    #[dao(order_by)]
    pub dep_pos: DependencyPos,
    pub defined_by_id: UserId,
    pub system: System,
}

#[ta_objects::Dao]
#[dao(table = "dependencies", ops(select), scd2, recursive, project_scoped)]
pub struct DependencyDB {
    #[dao(flatten)]
    pub base: DependencyInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

#[ta_objects::Dao]
#[dao(view = "dependencies__with_names", scd2, recursive, project_scoped)]
pub struct DependencyDBWithNames {
    #[dao(flatten)]
    pub base: DependencyDB,
    pub collection: CollectionName,
    pub table_collection: OfTable<CollectionName>,
}

impl DependencyDBWithNames {
    /// Dependencies usable in executions.
    pub fn user() -> impl SqlCondition<Self> {
        System::FALSE
    }
}
