//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::functions::Functions;
use super::logical_function_runs::{LogicalFunctionRuns, RUN_LADDER_FALLBACK, RUN_TERMINALS, run_status_count};
use super::physical_function_runs::PhysicalFunctionRuns;
use super::projects::{Projects, ProjectsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::{Logical, Parent, Physical, RollupArm, RollupDef, ViewDef};
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy)]
#[iden = "executions__with_names"]
pub struct ExecutionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Executions {
    Table,
    Id,
    ProjectId,
    Name,
    CollectionId,
    FunctionVersionId,
    TriggeredOn,
    TriggeredById,
    RecursionOptions,
    FunctionId,
    TriggerType,
    ExternalUid,
    Status,
    StartedOn,
    EndedOn,
}

/// The execution status ladder: the first arm holding over the execution's
/// logical-run statuses wins. Rolled up onto `executions.status`.
const EXECUTION_LADDER: &[RollupArm] = &[
    RollupArm::All { all: &["S"], status: "S" },
    RollupArm::All {
        all: &["C", "X", "Y"],
        status: "F",
    },
    RollupArm::AllWithAny {
        all: &["C", "D", "F", "H"],
        any: &["F", "H"],
        status: "L",
    },
    RollupArm::Any {
        any: &["S", "RR", "RS", "R", "D", "E"],
        status: "R",
    },
];

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Executions::Table)
                .col(logical_id_column(Executions::Id).not_null().primary_key())
                .col(id_column(Executions::ProjectId).not_null())
                .col(ColumnDef::new(Executions::Name).text().null())
                .col(id_column(Executions::CollectionId).not_null())
                .col(logical_id_column(Executions::FunctionVersionId).not_null())
                .col(ColumnDef::new(Executions::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(id_column(Executions::TriggeredById).not_null())
                // `Option<Json<RecursionOptions>>` in the DAO; sqlx encodes
                // `Json<T>` as JSONB in Postgres, so the column must be JSONB
                // decode time with a "mismatched types" error.
                .col(ColumnDef::new(Executions::RecursionOptions).json_binary().null())
                .col(id_column(Executions::FunctionId).not_null().default(""))
                .col(ColumnDef::new(Executions::TriggerType).text().not_null().default("M"))
                .col(ColumnDef::new(Executions::ExternalUid).text().null())
                // What the ladder yields before any run is planned.
                .col(ColumnDef::new(Executions::Status).text().not_null().default("S"))
                .col(ColumnDef::new(Executions::StartedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(Executions::EndedOn).timestamp_with_time_zone().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Executions::Table, Executions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Executions::Table, Executions::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // Trigger gate: the latest execution for a function version and
            // trigger type, on every execution trigger.
            Index::create()
                .name("executions___fv_id__trigger_type__triggered_on___idx")
                .table(Executions::Table)
                .col(Executions::FunctionVersionId)
                .col(Executions::TriggerType)
                .col(Executions::TriggeredOn)
                .to_owned(),
            // The execution list's default time ordering, per project.
            Index::create()
                .name("executions___project_id__triggered_on__id___idx")
                .table(Executions::Table)
                .col(Executions::ProjectId)
                .col(Executions::TriggeredOn)
                .col(Executions::Id)
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("executions___project_id__id___idx")
                .table(Executions::Table)
                .col(Executions::ProjectId)
                .col(Executions::Id)
                .to_owned(),
            // Dedup key: a non-NULL `external_uid` is unique per function.
            // NULLs are distinct on all backends, so untagged triggers are
            // never deduplicated.
            Index::create()
                .name("executions___function_id__external_uid___idx")
                .table(Executions::Table)
                .col(Executions::FunctionId)
                .col(Executions::ExternalUid)
                .unique()
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            ExecutionsWithNames,
            Query::select()
                .expr(Expr::col((Executions::Table, Asterisk)))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .expr_as(
                    name_or_id((Users::Table, Users::Name), (Executions::Table, Executions::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("project"))
                .expr_as(
                    run_status_count(LogicalFunctionRuns::ExecutionId, (Executions::Table, Executions::Id)),
                    Alias::new("function_run_status_count"),
                )
                .from(Executions::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Executions::Table, Executions::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Executions::Table, Executions::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Executions::Table, Executions::TriggeredById)).equals((Users::Table, Users::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    ProjectsResolved,
                    Expr::col((Executions::Table, Executions::ProjectId)).equals((ProjectsResolved, Projects::Id)),
                )
                .to_owned(),
        )]
    }
}

pub fn rollup() -> RollupDef {
    RollupDef {
        parent: Parent {
            table: Executions::Table.into_iden(),
            id: Executions::Id.into_iden(),
            status: Executions::Status.into_iden(),
            started_on: Executions::StartedOn.into_iden(),
            ended_on: Executions::EndedOn.into_iden(),
        },
        logical: Logical {
            table: LogicalFunctionRuns::Table.into_iden(),
            id: LogicalFunctionRuns::Id.into_iden(),
            parent: LogicalFunctionRuns::ExecutionId.into_iden(),
            status: LogicalFunctionRuns::Status.into_iden(),
        },
        physical: Physical {
            table: PhysicalFunctionRuns::Table.into_iden(),
            id: PhysicalFunctionRuns::Id.into_iden(),
            parent: PhysicalFunctionRuns::ExecutionId.into_iden(),
            started_on: PhysicalFunctionRuns::StartedOn.into_iden(),
            ended_on: PhysicalFunctionRuns::EndedOn.into_iden(),
        },
        ladder: EXECUTION_LADDER,
        fallback: RUN_LADDER_FALLBACK,
        finished: RUN_TERMINALS,
    }
}
