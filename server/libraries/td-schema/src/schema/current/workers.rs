//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::connections::Connections;
use super::executions::Executions;
use super::functions::Functions;
use super::logical_function_runs::LogicalFunctionRuns;
use super::physical_function_runs::PhysicalFunctionRuns;
use super::projects::{Projects, ProjectsResolved};
use super::transactions::Transactions;
use crate::schema::Entity;
use crate::schema::source::Derived;
use crate::schema::sql::{Transitions, TransitionsTriggerDef, ViewDef};
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy)]
#[iden = "workers__with_names"]
pub struct WorkersWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Workers {
    Table,
    Id,
    ProjectId,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionVersionId,
    PhysicalFunctionRunId,
    MessageStatus,
    StartedOn,
    EndedOn,
    Status,
    WorkerType,
    WorkerEnabled,
    WorkerConfig,
    Failures,
    RetryAt,
    DestinationWriteOn,
}

/// Every status change a worker may make, per worker type. A function runner
/// runs once. A trigger stops from any state its workload may still exist in:
/// a cron trigger parks at D or F between fires, and a stage trigger restarts
/// from F once its retry is due, or stops.
const WORKER_TRANSITIONS: &[(&str, &[(&str, &str)])] = &[
    ("F", &[("RR", "R"), ("RR", "F"), ("R", "D"), ("R", "F")]),
    (
        "CT",
        &[
            ("S", "RR"),
            ("RR", "R"),
            ("RR", "F"),
            ("R", "D"),
            ("R", "F"),
            ("D", "R"),
            ("D", "F"),
            ("F", "R"),
            ("F", "D"),
            ("S", "SPS"),
            ("RR", "SPS"),
            ("R", "SPS"),
            ("D", "SPS"),
            ("F", "SPS"),
            ("SPS", "SPR"),
            ("SPS", "SP"),
            ("SPR", "SP"),
        ],
    ),
    (
        "ST",
        &[
            ("S", "RR"),
            ("RR", "R"),
            ("RR", "F"),
            ("R", "D"),
            ("R", "F"),
            ("F", "R"),
            ("S", "SPS"),
            ("RR", "SPS"),
            ("R", "SPS"),
            ("D", "SPS"),
            ("F", "SPS"),
            ("SPS", "SPR"),
            ("SPS", "SP"),
            ("SPR", "SP"),
        ],
    ),
];

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Workers::Table)
                .col(logical_id_column(Workers::Id).not_null().primary_key())
                .col(id_column(Workers::ProjectId).not_null())
                .col(id_column(Workers::CollectionId).not_null())
                .col(logical_id_column(Workers::ExecutionId).null())
                .col(logical_id_column(Workers::TransactionId).null())
                .col(logical_id_column(Workers::FunctionVersionId).not_null())
                .col(logical_id_column(Workers::PhysicalFunctionRunId).null())
                .col(ColumnDef::new(Workers::MessageStatus).text().not_null())
                .col(ColumnDef::new(Workers::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(Workers::EndedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(Workers::Status).text().not_null())
                .col(ColumnDef::new(Workers::WorkerType).text().not_null())
                .col(ColumnDef::new(Workers::WorkerEnabled).boolean().not_null())
                // `WorkerConfig` is a string-typed Rust value (serialized JSON
                // text), so the column is TEXT. Was BLOB historically, but PG
                // rejects text→BYTEA on INSERT ("column is of type bytea but
                // expression is of type text").
                .col(ColumnDef::new(Workers::WorkerConfig).text().null())
                // A failed stream's restarts: failures in a row, and when the
                // next restart is due (NULL = none; on F, it is not restarted).
                .col(ColumnDef::new(Workers::Failures).integer().not_null().default(0))
                .col(ColumnDef::new(Workers::RetryAt).timestamp_with_time_zone().null())
                // When the attempt began a destination write a re-run would
                // repeat; recovery leaves such an attempt to the user.
                .col(ColumnDef::new(Workers::DestinationWriteOn).timestamp_with_time_zone().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Workers::Table, Workers::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Workers::Table, Workers::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Workers::Table, Workers::TransactionId)
                        .to(Transactions::Table, Transactions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Workers::Table, Workers::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Workers::Table, Workers::PhysicalFunctionRunId)
                        .to(PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // Collection delete stops every cron and stage worker of the
            // collection with one UPDATE.
            Index::create().name("workers___collection_id___idx").table(Workers::Table).col(Workers::CollectionId).to_owned(),
            // Register/update/read list a function version's cron and stage
            // workers; `worker_type` is always part of that predicate.
            Index::create()
                .name("workers___function_version_id__worker_type___idx")
                .table(Workers::Table)
                .col(Workers::FunctionVersionId)
                .col(Workers::WorkerType)
                .to_owned(),
            // The scheduler tick claims workers by `(worker_enabled, worker_type,
            // status)`; one row per run, so without this every tick scans the
            // table. The tick is per project and the recovery snapshot reads the
            // same triple across projects, so `project_id` sits after the shared
            // prefix, where it still narrows the tick and leaves `id` ordering
            // intact.
            Index::create()
                .name("workers___enabled__worker_type__status__project_id__id___idx")
                .table(Workers::Table)
                .col(Workers::WorkerEnabled)
                .col(Workers::WorkerType)
                .col(Workers::Status)
                .col(Workers::ProjectId)
                .col(Workers::Id)
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("workers___started_on__id___idx")
                .table(Workers::Table)
                .col(Workers::StartedOn)
                .col(Workers::Id)
                .to_owned(),
            Index::create()
                .name("workers___project_id__started_on__id___idx")
                .table(Workers::Table)
                .col(Workers::ProjectId)
                .col(Workers::StartedOn)
                .col(Workers::Id)
                .to_owned(),
            Index::create()
                .name("workers___project_id__status__started_on__id___idx")
                .table(Workers::Table)
                .col(Workers::ProjectId)
                .col(Workers::Status)
                .col(Workers::StartedOn)
                .col(Workers::Id)
                .to_owned(),
            Index::create()
                .name("workers___project_id__id___idx")
                .table(Workers::Table)
                .col(Workers::ProjectId)
                .col(Workers::Id)
                .to_owned(),
            // Dispatch poll anti-joins workers on this column to find PFRs
            // without a worker yet; callbacks fetch the worker the same way.
            Index::create()
                .name("workers___physical_function_run_id___idx")
                .table(Workers::Table)
                .col(Workers::PhysicalFunctionRunId)
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            TransitionsTriggerDef {
                name: format!("{}__transitions", Workers::Table.to_string()),
                source_table: Workers::Table.into_iden(),
                status_col: Workers::Status.into_iden(),
                transitions: Transitions::ByKind(Workers::WorkerType.into_iden(), WORKER_TRANSITIONS),
            }
            .derived(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            WorkersWithNames,
            Query::select()
                .expr(Expr::col((Workers::Table, Asterisk)))
                .expr_as(Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)), Alias::new("logical_function_run_id"))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((CollectionsResolved, Collections::ConnectionId)), Alias::new("connection_id"))
                .expr_as(Expr::col((Connections::Table, Connections::Kind)), Alias::new("connection"))
                .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                .expr_as(Expr::col((Functions::Table, Functions::FunctionId)), Alias::new("function_id"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .expr_as(Expr::col((Functions::Table, Functions::Enabled)), Alias::new("enabled"))
                .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("project"))
                // Patch id from the attempt's PFR (NULL = head). Distinguishes
                // which code each attempt ran; the worker row itself lacks it.
                .expr_as(
                    Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::FunctionVersionPatchId)),
                    Alias::new("function_version_patch_id"),
                )
                .from(Workers::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Workers::Table, Workers::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Connections::Table,
                    Expr::col((CollectionsResolved, Collections::ConnectionId)).equals((Connections::Table, Connections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((Workers::Table, Workers::ExecutionId)).equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Workers::Table, Workers::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                // LFR resolved directly from (Workers.execution_id,
                // Functions.function_id) — the LFR's natural key. No
                // PFR detour needed; the only thing PFR added was the
                // function_id, which Functions already carries.
                .join(
                    JoinType::LeftJoin,
                    LogicalFunctionRuns::Table,
                    Expr::col((Workers::Table, Workers::ExecutionId))
                        .equals((LogicalFunctionRuns::Table, LogicalFunctionRuns::ExecutionId))
                        .and(
                            Expr::col((Functions::Table, Functions::FunctionId))
                                .equals((LogicalFunctionRuns::Table, LogicalFunctionRuns::FunctionId)),
                        ),
                )
                .join(
                    JoinType::LeftJoin,
                    PhysicalFunctionRuns::Table,
                    Expr::col((Workers::Table, Workers::PhysicalFunctionRunId))
                        .equals((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    ProjectsResolved,
                    Expr::col((Workers::Table, Workers::ProjectId)).equals((ProjectsResolved, Projects::Id)),
                )
                .and_where(Expr::col((Workers::Table, Workers::WorkerEnabled)).eq(true))
                .to_owned(),
        )]
    }
}
