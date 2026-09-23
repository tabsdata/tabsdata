//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::connections::Connections;
use super::executions::Executions;
use super::function_patches::FunctionPatches;
use super::functions::Functions;
use super::logical_function_runs::LogicalFunctionRuns;
use super::projects::{Projects, ProjectsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{Scd2TriggerDef, ViewDef};
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy)]
#[iden = "physical_function_runs__with_names"]
pub struct PhysicalFunctionRunsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum PhysicalFunctionRuns {
    Table,
    Id,
    ProjectId,
    CollectionId,
    FunctionVersionId,
    // Patch id used at this attempt, if any. NULL → ran the head fvid
    // verbatim. FK to function_patches(id).
    FunctionVersionPatchId,
    ExecutionId,
    TransactionId,
    ValidFrom,
    TriggeredById,
    Trigger,
    // The run's effective prefix (function-version override, else the
    // collection's), frozen at mint so the run's own content survives a
    // later base_path move. Same resolution its output TDVs stamp.
    BasePath,
    // Stamped at dispatch; NULL until then. Basis for the stuck-run sweep.
    DispatchedOn,
    StartedOn,
    EndedOn,
    FunctionId,
    // Per-attempt audit status. Canonical flow-control status lives on
    // `logical_function_runs.status` — the two are independent.
    Status,
    LogicalFunctionRunId,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(PhysicalFunctionRuns::Table)
                .col(logical_id_column(PhysicalFunctionRuns::Id).not_null().primary_key())
                .col(id_column(PhysicalFunctionRuns::ProjectId).not_null())
                .col(id_column(PhysicalFunctionRuns::CollectionId).not_null())
                .col(logical_id_column(PhysicalFunctionRuns::FunctionVersionId).not_null())
                // Patch id used at this attempt, if any. NULL → the head
                // fvid in `function_version_id` ran verbatim; non-NULL → the
                // patch's bundle_id/snippet ran (head's table topology is
                // preserved, since patches don't redefine tables). FK to
                // `function_patches(id)`.
                .col(logical_id_column(PhysicalFunctionRuns::FunctionVersionPatchId).null())
                .col(logical_id_column(PhysicalFunctionRuns::ExecutionId).not_null())
                .col(logical_id_column(PhysicalFunctionRuns::TransactionId).not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::ValidFrom).timestamp_with_time_zone().not_null())
                .col(id_column(PhysicalFunctionRuns::TriggeredById).not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::Trigger).text().not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::DispatchedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(PhysicalFunctionRuns::EndedOn).timestamp_with_time_zone().null())
                .col(id_column(PhysicalFunctionRuns::FunctionId).not_null().default(""))
                // Per-attempt audit status. Worker callbacks write this on
                // every transition. The LFR row carries the canonical
                // (sticky-Committed) flow-control status — independent.
                .col(ColumnDef::new(PhysicalFunctionRuns::Status).text().not_null())
                // Bucket + metric columns. All nullable — written
                // atomically with status/ended_on at terminalization. The
                // metric columns are populated only when the attempt
                // produced output (status = Done).
                // Declared last so the appending delta lands on the same attnum a
                // fresh install does: the with-names view expands `*`.
                .col(ColumnDef::new(PhysicalFunctionRuns::BasePath).text().not_null())
                // SCD2 partition: attempts version their logical run by `valid_from`.
                .col(logical_id_column(PhysicalFunctionRuns::LogicalFunctionRunId).not_null())
                .col(ColumnDef::new(PhysicalFunctionRuns::ValidTo).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::LogicalFunctionRunId)
                        .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
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
            // The dispatch tick: `status = 'Requested' ORDER BY valid_from`,
            // per project; recovery reconciliation reads the same status across
            // projects, so `project_id` sits after the shared prefix rather
            // than in front of it.
            Index::create()
                .name("pfr___status__project_id__valid_from___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::Status)
                .col(PhysicalFunctionRuns::ProjectId)
                .col(PhysicalFunctionRuns::ValidFrom)
                .to_owned(),
            // The stale-dispatch sweep: `status = 'Dispatched' AND
            // dispatched_on < cutoff`; `dispatched_on` is otherwise unindexed.
            Index::create()
                .name("pfr___status__project_id__dispatched_on___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::Status)
                .col(PhysicalFunctionRuns::ProjectId)
                .col(PhysicalFunctionRuns::DispatchedOn)
                .to_owned(),
            // The roll-up's MIN(started_on) / MAX(ended_on) probe, index-only.
            Index::create()
                .name("pfr___project_id__execution_id__started_on__ended_on___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::ProjectId)
                .col(PhysicalFunctionRuns::ExecutionId)
                .col(PhysicalFunctionRuns::StartedOn)
                .col(PhysicalFunctionRuns::EndedOn)
                .to_owned(),
            Index::create()
                .name("pfr___project_id__transaction_id__started_on__ended_on___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::ProjectId)
                .col(PhysicalFunctionRuns::TransactionId)
                .col(PhysicalFunctionRuns::StartedOn)
                .col(PhysicalFunctionRuns::EndedOn)
                .to_owned(),
            // Declared FK with no index; the executions-of-a-function-version
            // list matches patch anchors through this column.
            Index::create()
                .name("pfr___function_version_patch_id___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::FunctionVersionPatchId)
                .to_owned(),
            Index::create()
                .name("pfr___function_version_id___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::FunctionVersionId)
                .to_owned(),
            // Metrics / Sankey time-window: the LFR-canonical view resolves
            // only `ended_on` to this table (status/dimensions are the LFR's),
            // and only the open attempt counts.
            Index::create()
                .name("pfr___project_id__ended_on___current_idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::ProjectId)
                .col(PhysicalFunctionRuns::EndedOn)
                .and_where(open_interval(PhysicalFunctionRuns::ValidTo))
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("pfr___project_id__id___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::ProjectId)
                .col(PhysicalFunctionRuns::Id)
                .to_owned(),
            // One open interval per logical run.
            Index::create()
                .name("pfr___logical_function_run_id___current_uidx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::LogicalFunctionRunId)
                .unique()
                .and_where(open_interval(PhysicalFunctionRuns::ValidTo))
                .to_owned(),
            // At-time reads and the SCD2 close-out trigger's own lookup;
            // `(valid_from, id)` is the splice order.
            Index::create()
                .name("pfr___logical_function_run_id__valid_from__id___idx")
                .table(PhysicalFunctionRuns::Table)
                .col(PhysicalFunctionRuns::LogicalFunctionRunId)
                .col(PhysicalFunctionRuns::ValidFrom)
                .col(PhysicalFunctionRuns::Id)
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", PhysicalFunctionRuns::Table.to_string()),
                source_table: PhysicalFunctionRuns::Table.into_iden(),
                partition_col: PhysicalFunctionRuns::LogicalFunctionRunId.into_iden(),
                valid_from_col: PhysicalFunctionRuns::ValidFrom.into_iden(),
                valid_to_col: PhysicalFunctionRuns::ValidTo.into_iden(),
                pk_col: PhysicalFunctionRuns::Id.into_iden(),
            }
            .derived(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            PhysicalFunctionRunsWithNames,
            Query::select()
                .expr(Expr::col((PhysicalFunctionRuns::Table, Asterisk)))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("name"))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((CollectionsResolved, Collections::ConnectionId)), Alias::new("connection_id"))
                .expr_as(Expr::col((Connections::Table, Connections::Kind)), Alias::new("connection"))
                .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("project"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(Expr::col((Executions::Table, Executions::FunctionVersionId)), Alias::new("execution_function_version_id"))
                .expr_as(Expr::col((Functions::Table, Functions::StorageVersion)), Alias::new("storage_version"))
                .expr_as(
                    name_or_id((Users::Table, Users::Name), (PhysicalFunctionRuns::Table, PhysicalFunctionRuns::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .from(PhysicalFunctionRuns::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::CollectionId))
                        .equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Connections::Table,
                    Expr::col((CollectionsResolved, Collections::ConnectionId)).equals((Connections::Table, Connections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    ProjectsResolved,
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::ProjectId)).equals((ProjectsResolved, Projects::Id)),
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
