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
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "physical_function_runs__with_names"]
pub struct PhysicalFunctionRunsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum PhysicalFunctionRuns {
    Table,
    Id,
    CollectionId,
    FunctionVersionId,
    // Patch id used at this attempt, if any. NULL → ran the head fvid
    // verbatim. FK to function_patches(id).
    FunctionVersionPatchId,
    ExecutionId,
    TransactionId,
    TriggeredOn,
    TriggeredById,
    Trigger,
    StartedOn,
    EndedOn,
    FunctionId,
    // Per-attempt audit status. Canonical flow-control status lives on
    // `logical_function_runs.status` — the two are independent.
    Status,
    // UTC epoch-ms bucket floors derived from `ended_on` at terminalization.
    // NULL until the attempt reaches a terminal status.
    EndedOnHourMs,
    EndedOnDayMs,
    EndedOnWeekMs,
    EndedOnMonthMs,
    // Aggregated per-attempt metrics. NULL except when the attempt
    // reached `Done` (i.e. produced output). Populated atomically with
    // `status` and `ended_on` from the worker callback's `RunMetrics`.
    MetricTableVersions,
    MetricRows,
    MetricDataBytes,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(PhysicalFunctionRuns::Table)
                .col(ColumnDef::new(PhysicalFunctionRuns::Id).text().not_null().primary_key())
                .col(ColumnDef::new(PhysicalFunctionRuns::CollectionId).text().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::FunctionVersionId).text().not_null())
                // Patch id used at this attempt, if any. NULL → the head
                // fvid in `function_version_id` ran verbatim; non-NULL → the
                // patch's bundle_id/snippet ran (head's table topology is
                // preserved, since patches don't redefine tables). FK to
                // `function_patches(id)`.
                .col(ColumnDef::new(PhysicalFunctionRuns::FunctionVersionPatchId).text().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::ExecutionId).text().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::TransactionId).text().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::TriggeredById).text().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::Trigger).text().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::EndedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::FunctionId).text().not_null().default(""))
                // Per-attempt audit status. Worker callbacks write this on
                // every transition. The LFR row carries the canonical
                // (sticky-Committed) flow-control status — independent.
                .col(ColumnDef::new(PhysicalFunctionRuns::Status).text().not_null())
                // Bucket + metric columns. All nullable — written
                // atomically with status/ended_on at terminalization. The
                // metric columns are populated only when the attempt
                // produced output (status = Done).
                .col(ColumnDef::new(PhysicalFunctionRuns::EndedOnHourMs).big_integer().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::EndedOnDayMs).big_integer().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::EndedOnWeekMs).big_integer().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::EndedOnMonthMs).big_integer().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::MetricTableVersions).big_integer().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::MetricRows).big_integer().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::MetricDataBytes).big_integer().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::FunctionVersionPatchId)
                        .to(FunctionPatches::Table, FunctionPatches::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("function_runs___collection_id___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::CollectionId)
                .to_owned(),
            Index::create()
                .name("function_runs___function_version_id___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::FunctionVersionId)
                .to_owned(),
            // Per-attempt status indices (FR-listing endpoints).
            Index::create()
                .name("function_runs___execution_id__status___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::ExecutionId)
                .col(PhysicalFunctionRuns::Status)
                .to_owned(),
            Index::create()
                .name("function_runs___transaction_id__status___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::TransactionId)
                .col(PhysicalFunctionRuns::Status)
                .to_owned(),
            Index::create()
                .name("function_runs___status___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::Status)
                .to_owned(),
            // Metrics / Sankey time-window composites. WHERE filters
            // on raw `ended_on`; GROUP BY uses the precomputed bucket-
            // floor columns but doesn't need an index for that.
            Index::create()
                .name("function_runs___collection_id__ended_on___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::CollectionId)
                .col(PhysicalFunctionRuns::EndedOn)
                .to_owned(),
            Index::create()
                .name("function_runs___status__ended_on___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::Status)
                .col(PhysicalFunctionRuns::EndedOn)
                .to_owned(),
            Index::create()
                .name("function_runs___function_version_id__ended_on___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::FunctionVersionId)
                .col(PhysicalFunctionRuns::EndedOn)
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![ViewDef::new(
            PhysicalFunctionRunsWithNames,
            Query::select()
                .expr(Expr::col((PhysicalFunctionRuns::Table, Asterisk)))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("name"))
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(Expr::col((Executions::Table, Executions::FunctionVersionId)), Alias::new("execution_function_version_id"))
                .expr_as(
                    b.name_or_id((Users::Table, Users::Name), (PhysicalFunctionRuns::Table, PhysicalFunctionRuns::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .from(PhysicalFunctionRuns::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::FunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::ExecutionId)).equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::TriggeredById)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }
}
