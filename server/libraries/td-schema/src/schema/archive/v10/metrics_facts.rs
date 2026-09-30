//
// Copyright 2026 Tabsdata Inc.
//

//! `/metrics` facts: one row per event of an ended attempt (`A`), per version
//! written (`O`) or read (`I`), plus an egress copy (`E`) of each read when
//! the run writes nothing. Read by `GROUP BY` + `SUM` over a window.

use sea_query::{ColumnDef, Iden, Index, IndexCreateStatement, Table, TableCreateStatement};

use crate::schema::Entity;
use crate::schema::sql::{PartialIndexDef, StatisticsDef};
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum MetricsFacts {
    Table,
    Ts,
    TsHourMs,
    TsDayMs,
    TsWeekMs,
    TsMonthMs,
    ProjectId,
    FromGroupId,
    FromCollectionId,
    GroupId,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionId,
    FunctionRunId,
    AttemptId,
    AttemptOutcome,
    RunStatus,
    Replay,
    Current,
    RunHasInputs,
    RunHasOutputs,
    Side,
    TableId,
    TableVersionId,
    Private,
    HasData,
    RowCount,
    DataBytes,
    // Makes a window's unconsumed output a scalar test instead of an anti-join.
    FirstConsumedOn,
    EdgeKind,
    // Runs per edge become a plain count instead of a `COUNT(DISTINCT)`.
    EdgeFirst,
    EdgeFirstGroup,
}

/// Carried in both covering indexes so the window is an index-only scan.
const READ_COLUMNS: [MetricsFacts; 16] = [
    MetricsFacts::FromGroupId,
    MetricsFacts::FromCollectionId,
    MetricsFacts::GroupId,
    MetricsFacts::CollectionId,
    MetricsFacts::RunStatus,
    MetricsFacts::Current,
    MetricsFacts::Side,
    MetricsFacts::TableId,
    MetricsFacts::Private,
    MetricsFacts::HasData,
    MetricsFacts::RowCount,
    MetricsFacts::DataBytes,
    MetricsFacts::FirstConsumedOn,
    MetricsFacts::EdgeKind,
    MetricsFacts::EdgeFirst,
    MetricsFacts::EdgeFirstGroup,
];

/// The grouping columns describe one lineage, so their distinct counts
/// multiply out far above the real group count and the planner sorts and
/// spills where it should hash. Eight is the statistics-object limit;
/// `project_id` adds no groups.
const GROUPING_COLUMNS: [MetricsFacts; 8] = [
    MetricsFacts::Side,
    MetricsFacts::EdgeKind,
    MetricsFacts::FromCollectionId,
    MetricsFacts::FromGroupId,
    MetricsFacts::CollectionId,
    MetricsFacts::GroupId,
    MetricsFacts::RunStatus,
    MetricsFacts::TableId,
];

fn big(col: MetricsFacts) -> ColumnDef {
    let mut def = ColumnDef::new(col);
    def.big_integer().not_null();
    def
}

fn flag(col: MetricsFacts) -> ColumnDef {
    let mut def = ColumnDef::new(col);
    def.boolean().not_null();
    def
}

fn text(col: MetricsFacts) -> ColumnDef {
    let mut def = ColumnDef::new(col);
    def.text().not_null();
    def
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(MetricsFacts::Table)
                .col(ColumnDef::new(MetricsFacts::Ts).timestamp_with_time_zone().not_null())
                .col(big(MetricsFacts::TsHourMs))
                .col(big(MetricsFacts::TsDayMs))
                .col(big(MetricsFacts::TsWeekMs))
                .col(big(MetricsFacts::TsMonthMs))
                .col(id_column(MetricsFacts::ProjectId).not_null())
                .col(id_column(MetricsFacts::FromGroupId).null())
                .col(id_column(MetricsFacts::FromCollectionId).null())
                .col(id_column(MetricsFacts::GroupId).not_null())
                .col(id_column(MetricsFacts::CollectionId).not_null())
                .col(logical_id_column(MetricsFacts::ExecutionId).not_null())
                .col(logical_id_column(MetricsFacts::TransactionId).not_null())
                .col(id_column(MetricsFacts::FunctionId).not_null())
                .col(logical_id_column(MetricsFacts::FunctionRunId).not_null())
                .col(logical_id_column(MetricsFacts::AttemptId).not_null())
                .col(text(MetricsFacts::AttemptOutcome))
                .col(text(MetricsFacts::RunStatus))
                .col(big(MetricsFacts::Replay))
                .col(flag(MetricsFacts::Current))
                .col(flag(MetricsFacts::RunHasInputs))
                .col(flag(MetricsFacts::RunHasOutputs))
                .col(text(MetricsFacts::Side))
                .col(id_column(MetricsFacts::TableId).null())
                .col(logical_id_column(MetricsFacts::TableVersionId).null())
                .col(flag(MetricsFacts::Private))
                .col(flag(MetricsFacts::HasData))
                .col(big(MetricsFacts::RowCount))
                .col(big(MetricsFacts::DataBytes))
                .col(ColumnDef::new(MetricsFacts::FirstConsumedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(MetricsFacts::EdgeKind).text().null())
                .col(flag(MetricsFacts::EdgeFirst))
                .col(flag(MetricsFacts::EdgeFirstGroup))
                .to_owned(),
        ]
    }

    /// Serves `versions_consumed`, which only ever looks up written rows.
    fn create_partial_indices(&self) -> Vec<PartialIndexDef> {
        vec![PartialIndexDef::new(
            "metrics_facts___table_version_id___written___idx",
            MetricsFacts::Table,
            [MetricsFacts::TableVersionId],
            "side = 'O'",
        )]
    }

    fn create_statistics(&self) -> Vec<StatisticsDef> {
        vec![StatisticsDef::n_distinct(
            "metrics_facts___grouping___stx",
            MetricsFacts::Table,
            GROUPING_COLUMNS,
        )]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        let mut project_window = Index::create();
        project_window
            .name("metrics_facts___project_id__ts___idx")
            .table(MetricsFacts::Table)
            .col(MetricsFacts::ProjectId)
            .col(MetricsFacts::Ts);
        let mut window = Index::create();
        window.name("metrics_facts___ts___idx").table(MetricsFacts::Table).col(MetricsFacts::Ts).include(MetricsFacts::ProjectId);
        for column in READ_COLUMNS {
            project_window.include(column);
            window.include(column);
        }
        vec![
            project_window.to_owned(),
            window.to_owned(),
            Index::create()
                .name("metrics_facts___function_run_id___idx")
                .table(MetricsFacts::Table)
                .col(MetricsFacts::FunctionRunId)
                .to_owned(),
            Index::create().name("metrics_facts___attempt_id___idx").table(MetricsFacts::Table).col(MetricsFacts::AttemptId).to_owned(),
        ]
    }
}
