//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, Table, TableCreateStatement};

use super::function_patches_current::FunctionPatchesCurrent;
use super::functions::Functions;
use crate::schema::Entity;
use crate::schema::sql::{ImmutableTriggerDef, TriggerSpec, VersionedTriggerDef, cols_of};

/// `function_patches` — code-only overrides of a parent
/// `FunctionVersionId`. Lives in its own table so every "find latest
/// function version" query against `functions` is patch-blind by
/// construction.
#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum FunctionPatches {
    Table,
    Id,
    ProjectId,
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
        // function_patches — code-only overrides of a parent fvid.
        // Must be declared before any table whose FK targets it
        // (e.g. `physical_function_runs.function_version_patch_id`).
        // Postgres validates FK targets at CREATE TABLE time within the
        vec![
            Table::create()
                .table(FunctionPatches::Table)
                .col(ColumnDef::new(FunctionPatches::Id).text().not_null().primary_key())
                .col(ColumnDef::new(FunctionPatches::ProjectId).text().not_null())
                .col(ColumnDef::new(FunctionPatches::ParentFunctionVersionId).text().not_null())
                .col(ColumnDef::new(FunctionPatches::FunctionId).text().not_null())
                .col(ColumnDef::new(FunctionPatches::CollectionId).text().not_null())
                .col(ColumnDef::new(FunctionPatches::Snippet).text().not_null())
                .col(ColumnDef::new(FunctionPatches::BundleId).text().not_null())
                .col(ColumnDef::new(FunctionPatches::RuntimeValues).text().not_null())
                .col(ColumnDef::new(FunctionPatches::Description).text().not_null())
                .col(ColumnDef::new(FunctionPatches::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(FunctionPatches::DefinedById).text().not_null())
                .col(ColumnDef::new(FunctionPatches::Status).text().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(FunctionPatches::Table, FunctionPatches::ParentFunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // List-patches scans by `(function_id, defined_on DESC)`.
            Index::create()
                .name("function_patches___function_id__defined_on___idx")
                .table(FunctionPatches::Table)
                .col(FunctionPatches::FunctionId)
                .col(FunctionPatches::DefinedOn)
                .to_owned(),
            // Mint (plan build / replay) ranks at-time within
            // `parent_function_version_id` partitions.
            Index::create()
                .name("function_patches___parent_function_version_id__defined_on__id___idx")
                .table(FunctionPatches::Table)
                .col(FunctionPatches::ParentFunctionVersionId)
                .col(FunctionPatches::DefinedOn)
                .col(FunctionPatches::Id)
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![
            TriggerSpec::VersionedCurrent(VersionedTriggerDef {
                name: format!("{}__maintain_current", FunctionPatches::Table.to_string()),
                source_table: FunctionPatches::Table.into_iden(),
                target_table: FunctionPatchesCurrent::Table.into_iden(),
                partition_col: FunctionPatches::ParentFunctionVersionId.into_iden(),
                version_col: FunctionPatches::DefinedOn.into_iden(),
                tie_break_col: FunctionPatches::Id.into_iden(),
                columns: cols_of::<FunctionPatches>(),
            }),
            // Patches are an event log of code amendments — never updated or deleted.
            TriggerSpec::Immutable(ImmutableTriggerDef {
                name: format!("{}__immutable", FunctionPatches::Table.to_string()),
                source_table: FunctionPatches::Table.into_iden(),
            }),
        ]
    }
}
