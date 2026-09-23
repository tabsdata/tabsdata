//
// Copyright 2026 Tabsdata Inc.
//

//! v9 → v10: `metrics_facts`, derived from the attempts that ended before v10
//! in the same transaction that creates it, then the attempt columns the old
//! `/metrics` aggregated are dropped. The derivation is frozen here like the
//! rest of the rung, and `metrics_facts_backfill` hands it to the DAO gate.
//!
//! The same rung adds the parents' rolled-up `status` / `started_on` /
//! `ended_on`, backfilled once here and kept current by the roll-up door.

use sea_query::{
    Alias, Asterisk, ColumnDef, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, IntoIden, JoinType, PostgresQueryBuilder,
    Query, SelectStatement, SimpleExpr, Table, TableCreateStatement, UnionType, WithClause,
};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::sql::{Logical, Parent, Physical, RollupArm, RollupDef, id_column, logical_id_column};
use crate::schema::version::SchemaDelta;

/// Frozen: the delta cannot follow a later rename of the enums.
const SIDE_ATTEMPT: &str = "A";
const SIDE_WRITTEN: &str = "O";
const SIDE_READ: &str = "I";
const SIDE_EGRESS: &str = "E";
const EDGE_REAL: &str = "R";
const EDGE_EXTERNAL_SOURCE: &str = "S";
const EDGE_EGRESS: &str = "E";
const ATTEMPT_ERROR: &str = "E";
const ATTEMPT_FAILED: &str = "F";

/// Measured flat from 32 MB up on a million-attempt database.
const DERIVATION_WORK_MEM: &str = "64MB";

/// Only the old `/metrics` read these.
const RETIRED_ATTEMPT_COLUMNS: [&str; 7] = [
    "ended_on_hour_ms",
    "ended_on_day_ms",
    "ended_on_week_ms",
    "ended_on_month_ms",
    "metric_table_versions",
    "metric_rows",
    "metric_data_bytes",
];

const TENANT_PREDICATE: &str = "project_id = current_setting('app.project_id', true)";
const ACROSS_PROJECTS_ROLE: &str = "td_across_projects";

#[derive(Iden, Clone, Copy)]
enum MetricsFacts {
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
    FirstConsumedOn,
    EdgeKind,
    EdgeFirst,
    EdgeFirstGroup,
}

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

fn facts() -> TableCreateStatement {
    use MetricsFacts as C;
    Table::create()
        .table(C::Table)
        .col(ColumnDef::new(C::Ts).timestamp_with_time_zone().not_null())
        .col(big(C::TsHourMs))
        .col(big(C::TsDayMs))
        .col(big(C::TsWeekMs))
        .col(big(C::TsMonthMs))
        .col(id_column(C::ProjectId).not_null())
        .col(id_column(C::FromGroupId).null())
        .col(id_column(C::FromCollectionId).null())
        .col(id_column(C::GroupId).not_null())
        .col(id_column(C::CollectionId).not_null())
        .col(logical_id_column(C::ExecutionId).not_null())
        .col(logical_id_column(C::TransactionId).not_null())
        .col(id_column(C::FunctionId).not_null())
        .col(logical_id_column(C::FunctionRunId).not_null())
        .col(logical_id_column(C::AttemptId).not_null())
        .col(text(C::AttemptOutcome))
        .col(text(C::RunStatus))
        .col(big(C::Replay))
        .col(flag(C::Current))
        .col(flag(C::RunHasInputs))
        .col(flag(C::RunHasOutputs))
        .col(text(C::Side))
        .col(id_column(C::TableId).null())
        .col(logical_id_column(C::TableVersionId).null())
        .col(flag(C::Private))
        .col(flag(C::HasData))
        .col(big(C::RowCount))
        .col(big(C::DataBytes))
        .col(ColumnDef::new(C::FirstConsumedOn).timestamp_with_time_zone().null())
        .col(ColumnDef::new(C::EdgeKind).text().null())
        .col(flag(C::EdgeFirst))
        .col(flag(C::EdgeFirstGroup))
        .to_owned()
}

fn policies(table: &str) -> Vec<String> {
    vec![
        format!(
            "ALTER TABLE {table} ALTER COLUMN project_id SET DEFAULT \
             NULLIF(current_setting('app.project_id', true), '')"
        ),
        format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY project_isolation ON {table} USING ({TENANT_PREDICATE}) WITH CHECK ({TENANT_PREDICATE})"),
        format!("CREATE POLICY across_projects ON {table} TO {ACROSS_PROJECTS_ROLE} USING (true) WITH CHECK (true)"),
    ]
}

/// Frozen at the shape they had at v10.
#[derive(Iden, Clone, Copy)]
enum PhysicalFunctionRuns {
    Table,
    Id,
    ProjectId,
    LogicalFunctionRunId,
    ExecutionId,
    TransactionId,
    StartedOn,
    EndedOn,
    Status,
    ValidFrom,
    ValidTo,
}

#[derive(Iden, Clone, Copy)]
enum LogicalFunctionRuns {
    Table,
    Id,
    ProjectId,
    CollectionId,
    GroupId,
    ExecutionId,
    TransactionId,
    FunctionId,
    Status,
}

#[derive(Iden, Clone, Copy)]
enum LogicalTableDataVersions {
    Table,
    Id,
    TableId,
    CollectionId,
    GroupId,
    ProducerLogicalFunctionRunId,
    FunctionParamPos,
    Private,
}

#[derive(Iden, Clone, Copy)]
enum LogicalFunctionRunInputs {
    Table,
    LogicalFunctionRunId,
    RequirementLogicalTableDataVersionId,
    RequirementDependencyPos,
}

#[derive(Iden, Clone, Copy)]
enum PhysicalTableDataVersions {
    Table,
    PhysicalFunctionRunId,
    LogicalTableDataVersionId,
    ValidFrom,
    ValidTo,
    HasData,
    RowCount,
    DataBytes,
}

const INSERTED_COLUMNS: [MetricsFacts; 32] = [
    MetricsFacts::Ts,
    MetricsFacts::TsHourMs,
    MetricsFacts::TsDayMs,
    MetricsFacts::TsWeekMs,
    MetricsFacts::TsMonthMs,
    MetricsFacts::ProjectId,
    MetricsFacts::FromGroupId,
    MetricsFacts::FromCollectionId,
    MetricsFacts::GroupId,
    MetricsFacts::CollectionId,
    MetricsFacts::ExecutionId,
    MetricsFacts::TransactionId,
    MetricsFacts::FunctionId,
    MetricsFacts::FunctionRunId,
    MetricsFacts::AttemptId,
    MetricsFacts::AttemptOutcome,
    MetricsFacts::RunStatus,
    MetricsFacts::Replay,
    MetricsFacts::Current,
    MetricsFacts::RunHasInputs,
    MetricsFacts::RunHasOutputs,
    MetricsFacts::Side,
    MetricsFacts::TableId,
    MetricsFacts::TableVersionId,
    MetricsFacts::Private,
    MetricsFacts::HasData,
    MetricsFacts::RowCount,
    MetricsFacts::DataBytes,
    MetricsFacts::FirstConsumedOn,
    MetricsFacts::EdgeKind,
    MetricsFacts::EdgeFirst,
    MetricsFacts::EdgeFirstGroup,
];

#[derive(Iden, Clone, Copy)]
enum Cte {
    Attempts,
    Writes,
    Reads,
    Reading,
    Writing,
    Consuming,
}

#[derive(Iden, Clone, Copy)]
enum Fact {
    EndedOn,
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
    TableId,
    TableVersionId,
    Private,
    HasData,
    RowCount,
    DataBytes,
    FirstConsumedOn,
}

fn col<T: IntoIden, C: IntoIden>(table: T, column: C) -> SimpleExpr {
    Expr::col((table.into_iden(), column.into_iden()))
}

/// The same value `BucketFloors` computes in process.
fn floor_ms(from: &str, unit: &str) -> SimpleExpr {
    Expr::cust(format!("(EXTRACT(EPOCH FROM date_trunc('{unit}', \"{from}\".\"ended_on\" AT TIME ZONE 'UTC')) * 1000)::bigint"))
}

/// The same row the in-process builder marks first.
fn first_of(from: &str, partition: &[&str]) -> SimpleExpr {
    let partition = partition.iter().map(|c| format!("\"{from}\".\"{c}\"")).collect::<Vec<_>>().join(", ");
    Expr::cust(format!("row_number() OVER (PARTITION BY {partition} ORDER BY \"{from}\".\"table_version_id\") = 1"))
}

fn distinct_run_ids<T: IntoIden + Copy, C: IntoIden>(table: T, run_id: C, when: Condition) -> SelectStatement {
    Query::select().distinct().expr_as(col(table, run_id), Fact::FunctionRunId).from(table).cond_where(when).take()
}

/// System tables sit at negative positions.
fn is_user_table<T: IntoIden + Copy>(table: T) -> SimpleExpr {
    col(table, LogicalTableDataVersions::FunctionParamPos).gte(0i32)
}

fn attempts_cte() -> CommonTableExpression {
    use self::LogicalFunctionRuns as L;
    use self::PhysicalFunctionRuns as P;

    let outcome = col(P::Table, P::Status);
    let failed = outcome.clone().is_in([ATTEMPT_ERROR, ATTEMPT_FAILED]);
    let run_status: SimpleExpr = Expr::case(failed, outcome.clone()).finally(col(L::Table, L::Status)).into();

    // LEFT JOINs, not select-list `EXISTS`: the ladder drops every index
    // before a rung, and a correlated `EXISTS` then seq-scans per attempt.
    let reads_anything = distinct_run_ids(
        LogicalFunctionRunInputs::Table,
        LogicalFunctionRunInputs::LogicalFunctionRunId,
        Condition::all()
            .add(col(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::RequirementLogicalTableDataVersionId).is_not_null())
            .add(col(LogicalFunctionRunInputs::Table, LogicalFunctionRunInputs::RequirementDependencyPos).gte(0i32)),
    );
    let writes_anything = distinct_run_ids(
        LogicalTableDataVersions::Table,
        LogicalTableDataVersions::ProducerLogicalFunctionRunId,
        Condition::all().add(is_user_table(LogicalTableDataVersions::Table)),
    );

    let mut select = Query::select();
    select
        .expr_as(col(P::Table, P::EndedOn), Fact::EndedOn)
        .expr_as(col(L::Table, L::ProjectId), Fact::ProjectId)
        .expr_as(col(L::Table, L::GroupId), Fact::GroupId)
        .expr_as(col(L::Table, L::CollectionId), Fact::CollectionId)
        .expr_as(col(L::Table, L::ExecutionId), Fact::ExecutionId)
        .expr_as(col(L::Table, L::TransactionId), Fact::TransactionId)
        .expr_as(col(L::Table, L::FunctionId), Fact::FunctionId)
        .expr_as(col(L::Table, L::Id), Fact::FunctionRunId)
        .expr_as(col(P::Table, P::Id), Fact::AttemptId)
        .expr_as(outcome, Fact::AttemptOutcome)
        .expr_as(run_status, Fact::RunStatus)
        .expr_as(
            Expr::cust(
                "row_number() OVER (PARTITION BY \"physical_function_runs\".\"logical_function_run_id\" \
                 ORDER BY \"physical_function_runs\".\"valid_from\") - 1",
            ),
            Fact::Replay,
        )
        .expr_as(col(P::Table, P::ValidTo).eq(Expr::cust("'infinity'::timestamptz")), Fact::Current)
        .expr_as(col(Cte::Reading, Fact::FunctionRunId).is_not_null(), Fact::RunHasInputs)
        .expr_as(col(Cte::Writing, Fact::FunctionRunId).is_not_null(), Fact::RunHasOutputs)
        .from(P::Table)
        .join(JoinType::Join, L::Table, col(L::Table, L::Id).eq(col(P::Table, P::LogicalFunctionRunId)))
        .join_subquery(JoinType::LeftJoin, reads_anything, Cte::Reading, col(Cte::Reading, Fact::FunctionRunId).eq(col(L::Table, L::Id)))
        .join_subquery(JoinType::LeftJoin, writes_anything, Cte::Writing, col(Cte::Writing, Fact::FunctionRunId).eq(col(L::Table, L::Id)))
        .and_where(col(P::Table, P::EndedOn).is_not_null());

    CommonTableExpression::new().query(select).table_name(Cte::Attempts).materialized(true).to_owned()
}

/// The instant `versions_consumed` stamps live, as a minimum over attempts.
fn first_consumption() -> SelectStatement {
    use self::LogicalFunctionRunInputs as I;
    use self::PhysicalFunctionRuns as P;

    Query::select()
        .expr_as(col(I::Table, I::RequirementLogicalTableDataVersionId), Fact::TableVersionId)
        .expr_as(Func::min(col(P::Table, P::EndedOn)), Fact::FirstConsumedOn)
        .from(I::Table)
        .join(JoinType::Join, P::Table, col(P::Table, P::LogicalFunctionRunId).eq(col(I::Table, I::LogicalFunctionRunId)))
        .and_where(col(P::Table, P::EndedOn).is_not_null())
        .and_where(col(I::Table, I::RequirementLogicalTableDataVersionId).is_not_null())
        .and_where(col(I::Table, I::RequirementDependencyPos).gte(0i32))
        .group_by_col((I::Table, I::RequirementLogicalTableDataVersionId))
        .take()
}

fn writes_cte() -> CommonTableExpression {
    use self::LogicalTableDataVersions as O;
    use self::PhysicalTableDataVersions as PT;
    let materialization = || Alias::new("written");

    let mut select = Query::select();
    select
        .expr(Expr::col((Cte::Attempts, Asterisk)))
        .expr_as(col(O::Table, O::TableId), Fact::TableId)
        .expr_as(col(O::Table, O::Id), Fact::TableVersionId)
        .expr_as(col(O::Table, O::Private), Fact::Private)
        .expr_as(Expr::expr(col(materialization(), PT::HasData)).if_null(false), Fact::HasData)
        .expr_as(Expr::expr(col(materialization(), PT::RowCount)).if_null(0i64), Fact::RowCount)
        .expr_as(Expr::expr(col(materialization(), PT::DataBytes)).if_null(0i64), Fact::DataBytes)
        .expr_as(col(Cte::Consuming, Fact::FirstConsumedOn), Fact::FirstConsumedOn)
        .from(Cte::Attempts)
        .join(
            JoinType::Join,
            O::Table,
            Expr::col((O::Table, O::ProducerLogicalFunctionRunId))
                .equals((Cte::Attempts, Fact::FunctionRunId))
                .and(is_user_table(O::Table)),
        )
        .join_as(
            JoinType::LeftJoin,
            PT::Table,
            materialization(),
            Expr::col((materialization(), PT::PhysicalFunctionRunId))
                .equals((Cte::Attempts, Fact::AttemptId))
                .and(col(materialization(), PT::LogicalTableDataVersionId).eq(col(O::Table, O::Id))),
        )
        .join_subquery(
            JoinType::LeftJoin,
            first_consumption(),
            Cte::Consuming,
            col(Cte::Consuming, Fact::TableVersionId).eq(col(O::Table, O::Id)),
        );

    CommonTableExpression::new().query(select).table_name(Cte::Writes).materialized(true).to_owned()
}

/// Reads carry the source's materialization as of the reading attempt's end,
/// which is what the live writer froze; a later replay of the producer does
/// not rewrite what was read.
fn reads_cte() -> CommonTableExpression {
    use self::LogicalFunctionRunInputs as I;
    use self::LogicalTableDataVersions as S;
    use self::PhysicalTableDataVersions as PT;
    let materialization = || Alias::new("current");

    let mut select = Query::select();
    select
        .distinct()
        .expr(Expr::col((Cte::Attempts, Asterisk)))
        .expr_as(col(S::Table, S::GroupId), Fact::FromGroupId)
        .expr_as(col(S::Table, S::CollectionId), Fact::FromCollectionId)
        .expr_as(col(S::Table, S::TableId), Fact::TableId)
        .expr_as(col(S::Table, S::Id), Fact::TableVersionId)
        .expr_as(col(S::Table, S::Private), Fact::Private)
        .expr_as(Expr::expr(col(materialization(), PT::HasData)).if_null(false), Fact::HasData)
        .expr_as(Expr::expr(col(materialization(), PT::RowCount)).if_null(0i64), Fact::RowCount)
        .expr_as(Expr::expr(col(materialization(), PT::DataBytes)).if_null(0i64), Fact::DataBytes)
        .from(Cte::Attempts)
        .join(
            JoinType::Join,
            I::Table,
            Expr::col((I::Table, I::LogicalFunctionRunId))
                .equals((Cte::Attempts, Fact::FunctionRunId))
                .and(col(I::Table, I::RequirementLogicalTableDataVersionId).is_not_null())
                .and(col(I::Table, I::RequirementDependencyPos).gte(0i32)),
        )
        .join(
            JoinType::Join,
            S::Table,
            col(S::Table, S::Id).eq(col(I::Table, I::RequirementLogicalTableDataVersionId)).and(is_user_table(S::Table)),
        )
        .join_as(
            JoinType::LeftJoin,
            PT::Table,
            materialization(),
            col(materialization(), PT::LogicalTableDataVersionId)
                .eq(col(S::Table, S::Id))
                .and(col(materialization(), PT::ValidFrom).lte(col(Cte::Attempts, Fact::EndedOn)))
                .and(col(materialization(), PT::ValidTo).gt(col(Cte::Attempts, Fact::EndedOn))),
        );

    CommonTableExpression::new().query(select).table_name(Cte::Reads).materialized(true).to_owned()
}

fn ctes() -> WithClause {
    WithClause::new().cte(attempts_cte()).cte(writes_cte()).cte(reads_cte()).to_owned()
}

struct Side {
    code: &'static str,
    from_endpoints: bool,
    version: bool,
    first_consumed: SimpleExpr,
    edge_kind: SimpleExpr,
    edge_first: SimpleExpr,
    edge_first_group: SimpleExpr,
}

/// In [`INSERTED_COLUMNS`] order.
fn side_select<T: IntoIden + Copy>(from: T, side: Side) -> SelectStatement {
    let carried = |column: Fact| col(from, column);
    let null = || Expr::value(sea_query::Value::String(None));
    let table = from.into_iden().to_string();
    let mut select = Query::select();
    select
        .expr(carried(Fact::EndedOn))
        .expr(floor_ms(&table, "hour"))
        .expr(floor_ms(&table, "day"))
        .expr(floor_ms(&table, "week"))
        .expr(floor_ms(&table, "month"))
        .expr(carried(Fact::ProjectId));
    if side.from_endpoints {
        select.expr(carried(Fact::FromGroupId)).expr(carried(Fact::FromCollectionId));
    } else {
        select.expr(null()).expr(null());
    }
    select
        .expr(carried(Fact::GroupId))
        .expr(carried(Fact::CollectionId))
        .expr(carried(Fact::ExecutionId))
        .expr(carried(Fact::TransactionId))
        .expr(carried(Fact::FunctionId))
        .expr(carried(Fact::FunctionRunId))
        .expr(carried(Fact::AttemptId))
        .expr(carried(Fact::AttemptOutcome))
        .expr(carried(Fact::RunStatus))
        .expr(carried(Fact::Replay))
        .expr(carried(Fact::Current))
        .expr(carried(Fact::RunHasInputs))
        .expr(carried(Fact::RunHasOutputs))
        .expr(Expr::val(side.code));
    if side.version {
        select
            .expr(carried(Fact::TableId))
            .expr(carried(Fact::TableVersionId))
            .expr(carried(Fact::Private))
            .expr(carried(Fact::HasData))
            .expr(carried(Fact::RowCount))
            .expr(carried(Fact::DataBytes));
    } else {
        select.expr(null()).expr(null()).expr(Expr::val(false)).expr(Expr::val(false)).expr(Expr::val(0i64)).expr(Expr::val(0i64));
    }
    select.expr(side.first_consumed).expr(side.edge_kind).expr(side.edge_first).expr(side.edge_first_group).from(from);
    select
}

fn rows() -> SelectStatement {
    let no_edge = || Expr::value(sea_query::Value::String(None));
    let never = || Expr::val(false);
    let kind = |kind: &'static str| Expr::val(kind);
    let writes = Cte::Writes.to_string();
    let reads = Cte::Reads.to_string();

    let unconsumed = || Expr::value(sea_query::Value::ChronoDateTimeUtc(None));

    let mut all = side_select(
        Cte::Attempts,
        Side {
            code: SIDE_ATTEMPT,
            from_endpoints: false,
            version: false,
            first_consumed: unconsumed(),
            edge_kind: no_edge(),
            edge_first: never(),
            edge_first_group: never(),
        },
    );

    let source_edge = || col(Cte::Writes, Fact::RunHasInputs).eq(false);
    let written_first = || first_of(&writes, &["attempt_id"]).and(source_edge());
    let written = side_select(
        Cte::Writes,
        Side {
            code: SIDE_WRITTEN,
            from_endpoints: false,
            version: true,
            first_consumed: col(Cte::Writes, Fact::FirstConsumedOn),
            edge_kind: Expr::case(source_edge(), kind(EDGE_EXTERNAL_SOURCE)).finally(no_edge()).into(),
            edge_first: written_first(),
            edge_first_group: written_first(),
        },
    );

    let read = side_select(
        Cte::Reads,
        Side {
            code: SIDE_READ,
            from_endpoints: true,
            version: true,
            first_consumed: unconsumed(),
            edge_kind: kind(EDGE_REAL),
            edge_first: first_of(&reads, &["attempt_id", "from_collection_id"]),
            edge_first_group: first_of(&reads, &["attempt_id", "from_group_id"]),
        },
    );

    let mut egress = side_select(
        Cte::Reads,
        Side {
            code: SIDE_EGRESS,
            from_endpoints: true,
            version: true,
            first_consumed: unconsumed(),
            edge_kind: kind(EDGE_EGRESS),
            edge_first: first_of(&reads, &["attempt_id"]),
            edge_first_group: first_of(&reads, &["attempt_id"]),
        },
    );
    egress.and_where(col(Cte::Reads, Fact::RunHasOutputs).eq(false));

    all.union(UnionType::All, written);
    all.union(UnionType::All, read);
    all.union(UnionType::All, egress);
    all
}

pub(super) fn backfill_statement() -> String {
    let mut insert = Query::insert();
    insert
        .into_table(MetricsFacts::Table)
        .columns(INSERTED_COLUMNS)
        .select_from(rows())
        .expect("the projection matches the inserted columns")
        .with_cte(ctes());
    insert.to_string(PostgresQueryBuilder)
}

/// The listing views the roll-up columns replace. Standing at v9 and absent
/// at v10, so the ladder takes their drops from this rung.
const RETIRED_VIEWS: [&str; 5] = [
    "executions__with_status",
    "transactions__with_status",
    "execution_status_summary",
    "transaction_status_summary",
    "global_status_summary",
];

/// Frozen at the shape they had at v10.
#[derive(Iden, Clone, Copy)]
enum Executions {
    Table,
    Id,
    Status,
    StartedOn,
    EndedOn,
}

#[derive(Iden, Clone, Copy)]
enum Transactions {
    Table,
    Id,
    Status,
    StartedOn,
    EndedOn,
}

const RUN_TERMINALS: &[&str] = &["C", "X", "Y"];
const RUN_LADDER_FALLBACK: &str = "U";

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

const TRANSACTION_LADDER: &[RollupArm] = &[
    RollupArm::All { all: &["S"], status: "S" },
    RollupArm::All { all: &["C"], status: "C" },
    RollupArm::All { all: &["X"], status: "X" },
    RollupArm::All { all: &["Y"], status: "Y" },
    RollupArm::AllWithAny {
        all: &["D", "F", "H"],
        any: &["F", "H"],
        status: "L",
    },
    RollupArm::Any {
        any: &["S", "RR", "RS", "R", "D", "E"],
        status: "R",
    },
];

fn rollup_columns<T: IntoIden + Copy>(table: T, status: impl IntoIden, started: impl IntoIden, ended: impl IntoIden) -> String {
    render_schema(
        Table::alter()
            .table(table)
            .add_column(ColumnDef::new(status).text().not_null().default("S"))
            .add_column(ColumnDef::new(started).timestamp_with_time_zone().null())
            .add_column(ColumnDef::new(ended).timestamp_with_time_zone().null())
            .to_owned(),
    )
}

fn execution_rollup() -> RollupDef {
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

fn transaction_rollup() -> RollupDef {
    RollupDef {
        parent: Parent {
            table: Transactions::Table.into_iden(),
            id: Transactions::Id.into_iden(),
            status: Transactions::Status.into_iden(),
            started_on: Transactions::StartedOn.into_iden(),
            ended_on: Transactions::EndedOn.into_iden(),
        },
        logical: Logical {
            table: LogicalFunctionRuns::Table.into_iden(),
            id: LogicalFunctionRuns::Id.into_iden(),
            parent: LogicalFunctionRuns::TransactionId.into_iden(),
            status: LogicalFunctionRuns::Status.into_iden(),
        },
        physical: Physical {
            table: PhysicalFunctionRuns::Table.into_iden(),
            id: PhysicalFunctionRuns::Id.into_iden(),
            parent: PhysicalFunctionRuns::TransactionId.into_iden(),
            started_on: PhysicalFunctionRuns::StartedOn.into_iden(),
            ended_on: PhysicalFunctionRuns::EndedOn.into_iden(),
        },
        ladder: TRANSACTION_LADDER,
        fallback: RUN_LADDER_FALLBACK,
        finished: RUN_TERMINALS,
    }
}

/// The v10 roll-up of the parents from their runs. Exposed so the DAO gate
/// can hold the live door to the rows the rung derives.
#[cfg(feature = "td-test")]
pub fn status_rollup_backfill() -> Vec<String> {
    vec![
        render_query(execution_rollup().backfill()),
        render_query(transaction_rollup().backfill()),
    ]
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 9;
    const TO_VERSION: usize = 10;

    fn retired() -> Vec<String> {
        RETIRED_VIEWS.iter().map(|view| format!("DROP VIEW IF EXISTS {view}")).collect()
    }

    fn up() -> Vec<String> {
        let mut out = vec![render_schema(facts())];
        out.push(format!("SET LOCAL work_mem = '{DERIVATION_WORK_MEM}'"));
        out.push(backfill_statement());
        out.push("RESET work_mem".to_string());
        out.extend(RETIRED_ATTEMPT_COLUMNS.iter().map(|column| format!("ALTER TABLE physical_function_runs DROP COLUMN {column}")));
        out.extend(policies("metrics_facts"));
        out.push(rollup_columns(Executions::Table, Executions::Status, Executions::StartedOn, Executions::EndedOn));
        out.push(rollup_columns(Transactions::Table, Transactions::Status, Transactions::StartedOn, Transactions::EndedOn));
        out.push(format!("SET LOCAL work_mem = '{DERIVATION_WORK_MEM}'"));
        out.push(render_query(execution_rollup().backfill()));
        out.push(render_query(transaction_rollup().backfill()));
        out.push("RESET work_mem".to_string());
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}
