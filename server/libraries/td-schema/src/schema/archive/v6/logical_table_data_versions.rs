//
// Copyright 2026 Tabsdata Inc.
//

//! Logical table-data-version identity rows.
//!
//! Insert-once at plan-build (and at lazy-mint over V1-born concrete TDVs).
//! Owns the cross-cutting `table_data_versions__with_names` view that
//! merges LTDV identity with the currently-bound PTDV (the one whose SCD2
//! binding interval is open).

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::executions::Executions;
use super::functions::Functions;
use super::groups::Groups;
use super::logical_function_runs::LogicalFunctionRuns;
use super::physical_function_runs::PhysicalFunctionRuns;
use super::physical_table_data_versions::PhysicalTableDataVersions;
use super::transactions::Transactions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;

#[derive(Iden, Clone, Copy)]
#[iden = "table_data_versions__with_names"]
pub struct TableDataVersionsWithNames;

#[derive(Iden, Clone, Copy)]
#[iden = "logical_table_data_versions__active"]
pub struct LtdvActive;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum LogicalTableDataVersions {
    Table,
    Id,
    ProjectId,
    CollectionId,
    ExecutionId,
    TransactionId,
    TableId,
    ProducerLogicalFunctionRunId,
    FunctionParamPos,
    // Table privateness snapshot, copied from the output table's
    // `tables.private` at LTDV mint. Lets metrics filter private
    // tables without joining the versioned `tables` table.
    Private,
    TriggeredOn,
    TriggeredById,
    // Group snapshot, copied from collections.group_id at LTDV mint.
    // Immutable thereafter. Equal by construction to the producing
    // LFR's group_id (both stamped in the same plan-build pass).
    GroupId,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(LogicalTableDataVersions::Table)
                .col(id_column(LogicalTableDataVersions::Id).not_null().primary_key())
                .col(id_column(LogicalTableDataVersions::ProjectId).not_null())
                .col(id_column(LogicalTableDataVersions::CollectionId).not_null())
                .col(id_column(LogicalTableDataVersions::ExecutionId).not_null())
                .col(id_column(LogicalTableDataVersions::TransactionId).not_null())
                .col(id_column(LogicalTableDataVersions::TableId).not_null())
                // Nullable: NULL when the LTDV is a lazy-mint over a V1-born
                // physical_table_data_versions row whose producer pre-dates the logic
                // layer, so no LFR exists for the producer.
                .col(id_column(LogicalTableDataVersions::ProducerLogicalFunctionRunId).null())
                // Nullable for the same reason as ProducerLogicalFunctionRunId.
                .col(ColumnDef::new(LogicalTableDataVersions::FunctionParamPos).integer().null())
                .col(ColumnDef::new(LogicalTableDataVersions::Private).boolean().not_null())
                .col(ColumnDef::new(LogicalTableDataVersions::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(id_column(LogicalTableDataVersions::TriggeredById).not_null())
                .col(id_column(LogicalTableDataVersions::GroupId).not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalTableDataVersions::Table, LogicalTableDataVersions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    // Composite: group ids are only unique per project.
                    ForeignKey::create()
                        .from(LogicalTableDataVersions::Table, (LogicalTableDataVersions::ProjectId, LogicalTableDataVersions::GroupId))
                        .to(Groups::Table, (Groups::ProjectId, Groups::Id))
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalTableDataVersions::Table, LogicalTableDataVersions::ExecutionId)
                        .to(Executions::Table, Executions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalTableDataVersions::Table, LogicalTableDataVersions::TransactionId)
                        .to(Transactions::Table, Transactions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                // No FK on table_id: this is the logical table id (groups
                // multiple versions), matching V1 physical_table_data_versions.table_id
                // which is similarly unconstrained.
                .foreign_key(
                    ForeignKey::create()
                        .from(LogicalTableDataVersions::Table, LogicalTableDataVersions::ProducerLogicalFunctionRunId)
                        .to(LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // The group-delete RESTRICT FK probe (see `collections`); without
            // it the probe scans the highest-row-count table in the schema.
            Index::create()
                .name("logical_table_data_versions___project_id__group_id___idx")
                .table(LogicalTableDataVersions::Table)
                .col(LogicalTableDataVersions::ProjectId)
                .col(LogicalTableDataVersions::GroupId)
                .to_owned(),
            // LTDV: hot path — `Head(0)` resolution scans by table_id ordered
            // by `(triggered_on DESC, id DESC)`.
            Index::create()
                .name("logical_table_data_versions___table_id__triggered_on__id___idx")
                .table(LogicalTableDataVersions::Table)
                .col(LogicalTableDataVersions::TableId)
                .col(LogicalTableDataVersions::TriggeredOn)
                .col(LogicalTableDataVersions::Id)
                .to_owned(),
            // LTDV: producer-side lookup (find every output an LFR produces).
            // Short prefix: the spelled-out name exceeds PostgreSQL's 63-byte
            // identifier limit and would be silently truncated.
            Index::create()
                .name("ltdv___producer_logical_function_run_id___idx")
                .table(LogicalTableDataVersions::Table)
                .col(LogicalTableDataVersions::ProducerLogicalFunctionRunId)
                .to_owned(),
            // One row per `(execution, table)`; the plan resolver reads it per
            // output table, and it covers the executions RESTRICT FK probe.
            Index::create()
                .name("logical_table_data_versions___execution_id__table_id___idx")
                .table(LogicalTableDataVersions::Table)
                .col(LogicalTableDataVersions::ExecutionId)
                .col(LogicalTableDataVersions::TableId)
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("logical_table_data_versions___project_id__id___idx")
                .table(LogicalTableDataVersions::Table)
                .col(LogicalTableDataVersions::ProjectId)
                .col(LogicalTableDataVersions::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        // The single table-data-version listing view: LTDV identity rows
        // merged with the currently-bound PTDV's per-attempt fields,
        // resolved via the open binding interval. PTDV-derived fields
        // (and everything reached through PTDV) are NULL when not bound.
        let groups_alias = Alias::new("groups_for_ltdv");
        vec![
            ViewDef::new(
                TableDataVersionsWithNames,
                Query::select()
                    .expr(Expr::col((LogicalTableDataVersions::Table, Asterisk)))
                    .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                    .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                    .expr_as(Expr::col((Executions::Table, Executions::Name)), Alias::new("execution"))
                    .expr_as(
                        name_or_id((Users::Table, Users::Name), (LogicalTableDataVersions::Table, LogicalTableDataVersions::TriggeredById)),
                        Alias::new("triggered_by"),
                    )
                    .expr_as(Expr::col((groups_alias.clone(), Groups::Name)), Alias::new("group"))
                    // PTDV-derived columns (the bound materialization).
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::Id)),
                        Alias::new("physical_table_data_version_id"),
                    )
                    .expr_as(Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::Name)), Alias::new("name"))
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::TableVersionId)),
                        Alias::new("table_version_id"),
                    )
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::FunctionVersionId)),
                        Alias::new("function_version_id"),
                    )
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::FunctionVersionPatchId)),
                        Alias::new("function_version_patch_id"),
                    )
                    .expr_as(Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::HasData)), Alias::new("has_data"))
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::SchemaChanged)),
                        Alias::new("schema_changed"),
                    )
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::ColumnCount)),
                        Alias::new("column_count"),
                    )
                    .expr_as(Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::RowCount)), Alias::new("row_count"))
                    .expr_as(Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::DataBytes)), Alias::new("data_bytes"))
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::SchemaHash)),
                        Alias::new("schema_hash"),
                    )
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::WithDataTableDataVersionId)),
                        Alias::new("with_data_table_data_version_id"),
                    )
                    .expr_as(
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::PhysicalFunctionRunId)),
                        Alias::new("physical_function_run_id"),
                    )
                    // PFR-side audit reached through the PTDV's producer.
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::StartedOn)), Alias::new("started_on"))
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::EndedOn)), Alias::new("ended_on"))
                    // Canonical LFR-side status of the LTDV's producer LFR
                    // (sticky-Committed). Use this for committed-only filters.
                    .expr_as(Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status)), Alias::new("status"))
                    // PFR-side per-attempt status (Running, Done, Error, …).
                    .expr_as(Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Status)), Alias::new("physical_status"))
                    .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                    .expr_as(Expr::col((Functions::Table, Functions::StorageVersion)), Alias::new("storage_version"))
                    .from(LogicalTableDataVersions::Table)
                    .join(
                        JoinType::LeftJoin,
                        CollectionsResolved,
                        Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::CollectionId))
                            .equals((CollectionsResolved, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Executions::Table,
                        Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::ExecutionId))
                            .equals((Executions::Table, Executions::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::TriggeredById))
                            .equals((Users::Table, Users::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        PhysicalTableDataVersions::Table,
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::LogicalTableDataVersionId))
                            .equals((LogicalTableDataVersions::Table, LogicalTableDataVersions::Id))
                            .and(open_interval((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::ValidTo))),
                    )
                    .join(
                        JoinType::LeftJoin,
                        PhysicalFunctionRuns::Table,
                        Expr::col((PhysicalFunctionRuns::Table, PhysicalFunctionRuns::Id))
                            .equals((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::PhysicalFunctionRunId)),
                    )
                    // Producer LFR — the LTDV-level canonical status comes
                    // from here (sticky-Committed); separate from the
                    // PFR's per-attempt status reached via PTDV above.
                    .join(
                        JoinType::LeftJoin,
                        LogicalFunctionRuns::Table,
                        Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::ProducerLogicalFunctionRunId))
                            .equals((LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Functions::Table,
                        Expr::col((PhysicalTableDataVersions::Table, PhysicalTableDataVersions::FunctionVersionId))
                            .equals((Functions::Table, Functions::Id)),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        Groups::Table,
                        groups_alias.clone(),
                        Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::GroupId))
                            .equals((groups_alias.clone(), Groups::Id))
                            .and(
                                Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::ProjectId))
                                    .equals((groups_alias, Groups::ProjectId)),
                            ),
                    )
                    .to_owned(),
            ),
            // logical_table_data_versions__active — LTDVs whose producer
            // LFR is not Canceled/Yanked (NULL producer = lazy-mint, kept).
            // A cancel "resets the table to that point": the version
            // resolver reads this view, so a cancelled head disappears and
            // HEAD falls back to the previous live version. Cancel/Yank are
            // logical-only statuses, so the filter lives here, not on the
            // physical side (which has no such statuses).
            //
            // Caveat: the join reads the producer's *current* status, so the
            // view is point-in-time correct for plan-time resolution (always
            // "now") but anachronistic for any historic/audit read — a version
            // produced by a since-cancelled run would be hidden retroactively.
            // Don't reuse this view for as-of-then queries without rethinking.
            ViewDef::new(
                LtdvActive,
                Query::select()
                    .expr(Expr::col((LogicalTableDataVersions::Table, Asterisk)))
                    .from(LogicalTableDataVersions::Table)
                    .join(
                        JoinType::LeftJoin,
                        LogicalFunctionRuns::Table,
                        Expr::col((LogicalTableDataVersions::Table, LogicalTableDataVersions::ProducerLogicalFunctionRunId))
                            .equals((LogicalFunctionRuns::Table, LogicalFunctionRuns::Id)),
                    )
                    // "Y" = LogicalFunctionRunStatus::Yanked, "X" =
                    // ::Canceled. These are raw codes, not the enum: this
                    // crate is below td-objects in the dependency graph and
                    // can't reference it (same reason the physical views use
                    // literals). A divergent rename is caught by the canary
                    // test `test_sticky_terminal_codes_pinned` in td-objects.
                    .and_where(
                        Expr::col((LogicalFunctionRuns::Table, LogicalFunctionRuns::Status)).is_null().or(Expr::col((
                            LogicalFunctionRuns::Table,
                            LogicalFunctionRuns::Status,
                        ))
                        .is_not_in(["Y", "X"])),
                    )
                    .to_owned(),
            ),
        ]
    }
}
