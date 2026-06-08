//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::Collections;
use super::executions::Executions;
use super::function_patches::FunctionPatches;
use super::functions::Functions;
use super::physical_function_runs::PhysicalFunctionRuns;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

/// View name for `physical_table_data_versions__with_function`, reused by
/// the `__active` and `__with_names` views below.
#[derive(Iden, Clone, Copy)]
#[iden = "physical_table_data_versions__with_function"]
pub struct PtdvWithFunction;

#[derive(Iden, Clone, Copy)]
#[iden = "physical_table_data_versions__with_names"]
pub struct PtdvWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum PhysicalTableDataVersions {
    Table,
    Id,
    ProjectId,
    CollectionId,
    TableId,
    Name,
    TableVersionId,
    FunctionVersionId,
    FunctionVersionPatchId,
    HasData,
    SchemaChanged,
    ColumnCount,
    RowCount,
    DataBytes,
    SchemaHash,
    ExecutionId,
    TransactionId,
    PhysicalFunctionRunId,
    FunctionParamPos,
    WithDataTableDataVersionId,
    TriggeredOn,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(PhysicalTableDataVersions::Table)
                .col(ColumnDef::new(PhysicalTableDataVersions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(PhysicalTableDataVersions::ProjectId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::CollectionId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::TableId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::Name).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::TableVersionId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::FunctionVersionId).text().not_null())
                // Patch id used at the producer attempt, if any. NULL → no
                // patch. FK to `function_patches(id)`. Mirrors
                // `physical_function_runs.function_version_patch_id`.
                .col(ColumnDef::new(PhysicalTableDataVersions::FunctionVersionPatchId).text().null())
                .col(ColumnDef::new(PhysicalTableDataVersions::HasData).boolean().null())
                .col(ColumnDef::new(PhysicalTableDataVersions::SchemaChanged).boolean().null())
                // i64 Rust types (`ColumnCount`, `RowCount`); sqlx encodes as BIGINT,
                // PG rejects decode against INT4. Use `big_integer()` for 8-byte
                // width consistency across all backends.
                .col(ColumnDef::new(PhysicalTableDataVersions::ColumnCount).big_integer().null())
                .col(ColumnDef::new(PhysicalTableDataVersions::RowCount).big_integer().null())
                // Output-table byte count reported by the worker yaml's
                // `TableInfo.data_bytes` (Option<i64>). NULL when the worker
                // omits it (e.g., older Python worker yamls).
                .col(ColumnDef::new(PhysicalTableDataVersions::DataBytes).big_integer().null())
                .col(ColumnDef::new(PhysicalTableDataVersions::SchemaHash).text().null())
                .col(ColumnDef::new(PhysicalTableDataVersions::ExecutionId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::TransactionId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::PhysicalFunctionRunId).text().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::FunctionParamPos).integer().not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::WithDataTableDataVersionId).text().null())
                .col(ColumnDef::new(PhysicalTableDataVersions::TriggeredOn).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::FunctionVersionPatchId)
                        .to(FunctionPatches::Table, FunctionPatches::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::PhysicalFunctionRunId)
                        .to(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("physical_table_data_versions___collection_id___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::CollectionId)
                .to_owned(),
            Index::create()
                .name("physical_table_data_versions___execution_id___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::ExecutionId)
                .to_owned(),
            Index::create()
                .name("physical_table_data_versions___physical_function_run_id___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::PhysicalFunctionRunId)
                .to_owned(),
            // Note: single table_id index removed — it's a prefix of these composites
            Index::create()
                .name("physical_table_data_versions___table_id___has_data___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::TableId)
                .col(PhysicalTableDataVersions::HasData)
                .to_owned(),
            // Composite index for versioned queries:
            // PARTITION BY table_id ORDER BY triggered_on DESC, id DESC
            Index::create()
                .name("physical_table_data_versions___table_id__triggered_on__id___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::TableId)
                .col(PhysicalTableDataVersions::TriggeredOn)
                .col(PhysicalTableDataVersions::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![
            // physical_table_data_versions__with_function
            //
            // Per-attempt audit view: which concrete FR (and function
            // version) produced this concrete TDV. `status` is the FR-side
            // per-attempt status — for canonical (sticky-Committed)
            // flow-control read LTDV.status instead, via the LTDV identity
            // row (chain queries already do this).
            ViewDef::new(
                PtdvWithFunction,
                Query::select()
                    .expr(Expr::col((PhysicalTableDataVersions::Table, Asterisk)))
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::TriggeredById)), Alias::new("triggered_by_id"))
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::StartedOn)), Alias::new("started_on"))
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOn)), Alias::new("ended_on"))
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Status)), Alias::new("status"))
                    .expr_as(Expr::col((Functions::Table, Functions::DataLocation)), Alias::new("data_location"))
                    .expr_as(Expr::col((Functions::Table, Functions::StorageVersion)), Alias::new("storage_version"))
                    .from(PhysicalTableDataVersions::Table)
                    .join(
                        JoinType::LeftJoin,
                        Functions::Table,
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::FunctionVersionId))
                            .equals((Functions::Table, Functions::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        PhysicalFunctionRuns::Table,
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::PhysicalFunctionRunId))
                            .equals((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id)),
                    )
                    .to_owned(),
            ),
            // physical_table_data_versions__with_names
            ViewDef::new(
                PtdvWithNames,
                Query::select()
                    .expr(Expr::col((PtdvWithFunction, Asterisk)))
                    .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                    .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                    .expr_as(
                        b.name_or_id((Users::Table, Users::Name), (PtdvWithFunction, Alias::new("triggered_by_id"))),
                        Alias::new("created_by"),
                    )
                    .from(PtdvWithFunction)
                    .join(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Expr::col((PtdvWithFunction, PhysicalTableDataVersions::CollectionId))
                            .equals((Collections::Table, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Functions::Table,
                        Expr::col((PtdvWithFunction, PhysicalTableDataVersions::FunctionVersionId))
                            .equals((Functions::Table, Functions::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((PtdvWithFunction, Alias::new("triggered_by_id"))).equals((Users::Table, Users::Id)),
                    )
                    .to_owned(),
            ),
        ]
    }
}
