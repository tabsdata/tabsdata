//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::executions::Executions;
use super::function_patches::FunctionPatches;
use super::functions::Functions;
use super::logical_table_data_versions::LogicalTableDataVersions;
use super::physical_function_runs::PhysicalFunctionRuns;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{Scd2TriggerDef, ViewDef};
use crate::schema::sql::{id_column, logical_id_column};

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
    ColumnarLineage,
    ExecutionId,
    TransactionId,
    PhysicalFunctionRunId,
    FunctionParamPos,
    WithDataTableDataVersionId,
    ValidFrom,
    BasePath,
    LogicalTableDataVersionId,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(PhysicalTableDataVersions::Table)
                .col(logical_id_column(PhysicalTableDataVersions::Id).not_null().primary_key())
                .col(id_column(PhysicalTableDataVersions::ProjectId).not_null())
                .col(id_column(PhysicalTableDataVersions::CollectionId).not_null())
                .col(id_column(PhysicalTableDataVersions::TableId).not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::Name).text().not_null())
                .col(id_column(PhysicalTableDataVersions::TableVersionId).not_null())
                .col(logical_id_column(PhysicalTableDataVersions::FunctionVersionId).not_null())
                // Patch id used at the producer attempt, if any. NULL → no
                // patch. FK to `function_patches(id)`. Mirrors
                // `physical_function_runs.function_version_patch_id`.
                .col(logical_id_column(PhysicalTableDataVersions::FunctionVersionPatchId).null())
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
                .col(ColumnDef::new(PhysicalTableDataVersions::ColumnarLineage).text().null())
                .col(logical_id_column(PhysicalTableDataVersions::ExecutionId).not_null())
                .col(logical_id_column(PhysicalTableDataVersions::TransactionId).not_null())
                .col(logical_id_column(PhysicalTableDataVersions::PhysicalFunctionRunId).not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::FunctionParamPos).integer().not_null())
                .col(logical_id_column(PhysicalTableDataVersions::WithDataTableDataVersionId).null())
                .col(ColumnDef::new(PhysicalTableDataVersions::ValidFrom).timestamp_with_time_zone().not_null())
                // Storage prefix frozen at mint = function.base_path ?? collection.base_path.
                // Self-describing: reads use this directly, never recompute, so data stays
                // findable after the collection/function base_path later changes.
                .col(ColumnDef::new(PhysicalTableDataVersions::BasePath).text().not_null())
                // SCD2 partition: materializations version their logical version by `valid_from`.
                .col(logical_id_column(PhysicalTableDataVersions::LogicalTableDataVersionId).not_null())
                .col(ColumnDef::new(PhysicalTableDataVersions::ValidTo).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalTableDataVersions::Table, PhysicalTableDataVersions::LogicalTableDataVersionId)
                        .to(LogicalTableDataVersions::Table, LogicalTableDataVersions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
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
            // A run writes each of its tables once; the constraint is what
            // makes `(physical_function_run_id, table_id)` an addressing read.
            // Shortened name: PostgreSQL truncates past 63 chars.
            Index::create()
                .name("ptdv___physical_function_run_id__table_id___uidx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::PhysicalFunctionRunId)
                .col(PhysicalTableDataVersions::TableId)
                .unique()
                .to_owned(),
            // One open interval per logical version.
            Index::create()
                .name("ptdv___logical_table_data_version_id___current_uidx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::LogicalTableDataVersionId)
                .unique()
                .and_where(open_interval(PhysicalTableDataVersions::ValidTo))
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("ptdv___project_id__id___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::ProjectId)
                .col(PhysicalTableDataVersions::Id)
                .to_owned(),
            // At-time reads; `(valid_from, id)` is the splice order.
            Index::create()
                .name("ptdv___logical_table_data_version_id__valid_from__id___idx")
                .table(PhysicalTableDataVersions::Table)
                .col(PhysicalTableDataVersions::LogicalTableDataVersionId)
                .col(PhysicalTableDataVersions::ValidFrom)
                .col(PhysicalTableDataVersions::Id)
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", PhysicalTableDataVersions::Table.to_string()),
                source_table: PhysicalTableDataVersions::Table.into_iden(),
                partition_col: PhysicalTableDataVersions::LogicalTableDataVersionId.into_iden(),
                valid_from_col: PhysicalTableDataVersions::ValidFrom.into_iden(),
                valid_to_col: PhysicalTableDataVersions::ValidTo.into_iden(),
                pk_col: PhysicalTableDataVersions::Id.into_iden(),
            }
            .derived(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
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
                    .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                    .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                    .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                    .expr_as(
                        name_or_id((Users::Table, Users::Name), (PtdvWithFunction, Alias::new("triggered_by_id"))),
                        Alias::new("created_by"),
                    )
                    .from(PtdvWithFunction)
                    .join(
                        JoinType::LeftJoin,
                        CollectionsResolved,
                        Expr::col((PtdvWithFunction, PhysicalTableDataVersions::CollectionId))
                            .equals((CollectionsResolved, Collections::Id)),
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
