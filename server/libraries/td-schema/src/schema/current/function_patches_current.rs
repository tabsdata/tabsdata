//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, Iden, Table, TableCreateStatement};

use crate::schema::Entity;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
#[iden = "function_patches_current"]
pub enum FunctionPatchesCurrent {
    Table,
    Id,
    ParentFunctionVersionId,
    FunctionId,
    CollectionId,
    Snippet,
    BundleId,
    RuntimeValues,
    Description,
    DefinedOn,
    DefinedById,
    Status,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        // function_patches_current — holds the latest patch per
        // parent_function_version_id. Maintained by an AFTER INSERT
        // trigger on function_patches. Mint reads this via the versioned
        // current view to resolve "current patch of head X at time T".
        vec![
            Table::create()
                .table(FunctionPatchesCurrent::Table)
                .col(ColumnDef::new(FunctionPatchesCurrent::Id).text().not_null().primary_key())
                .col(ColumnDef::new(FunctionPatchesCurrent::ParentFunctionVersionId).text().not_null().unique_key())
                .col(ColumnDef::new(FunctionPatchesCurrent::FunctionId).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::CollectionId).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::Snippet).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::BundleId).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::RuntimeValues).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::Description).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::DefinedById).text().not_null())
                .col(ColumnDef::new(FunctionPatchesCurrent::Status).text().not_null())
                .to_owned(),
        ]
    }
}
