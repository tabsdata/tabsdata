//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, CommonTableExpression, Expr, ExprTrait, ForeignKey,
    ForeignKeyAction, Func, Iden, InsertStatement, JoinType, Query, Table, TableCreateStatement,
    WithClause,
};

use super::collections::Collections;
use super::users::Users;
use crate::schema::backend::{Backend, ViewDef};

// ---------------------------------------------------------------------------
// Iden enums for all function-related tables
// ---------------------------------------------------------------------------

#[derive(Iden)]
pub enum Functions {
    Table,
    Id,
    CollectionId,
    Name,
    Description,
    Decorator,
    RuntimeValues,
    FunctionId,
    DataLocation,
    StorageVersion,
    BundleId,
    Snippet,
    DefinedOn,
    DefinedById,
    Status,
    Connector,
    Enabled,
}

#[derive(Iden)]
pub enum Tables {
    Table,
    Id,
    CollectionId,
    TableId,
    Name,
    FunctionId,
    FunctionVersionId,
    FunctionParamPos,
    Private,
    Partitioned,
    DefinedOn,
    DefinedById,
    Status,
    TableType,
}

#[derive(Iden)]
pub enum Bundles {
    Table,
    Id,
    CollectionId,
    Hash,
    CreatedOn,
    CreatedById,
}

#[derive(Iden)]
pub enum Dependencies {
    Table,
    Id,
    CollectionId,
    DependencyId,
    FunctionId,
    TableCollectionId,
    TableFunctionId,
    TableId,
    TableVersions,
    DepPos,
    Status,
    DefinedOn,
    DefinedById,
    System,
}

#[derive(Iden)]
pub enum Triggers {
    Table,
    Id,
    CollectionId,
    TriggerId,
    FunctionId,
    TriggerByCollectionId,
    TriggerByFunctionId,
    TriggerByTableId,
    Status,
    DefinedOn,
    DefinedById,
    System,
}

#[derive(Iden)]
pub enum Executions {
    Table,
    Id,
    Name,
    CollectionId,
    FunctionVersionId,
    TriggeredOn,
    TriggeredById,
    RecursionOptions,
    FunctionId,
    TriggerType,
}

#[derive(Iden)]
pub enum Transactions {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionBy,
    TransactionKey,
    TriggeredOn,
    TriggeredById,
}

#[derive(Iden)]
pub enum FunctionRuns {
    Table,
    Id,
    CollectionId,
    FunctionVersionId,
    ExecutionId,
    TransactionId,
    TriggeredOn,
    TriggeredById,
    Trigger,
    StartedOn,
    EndedOn,
    Status,
    FunctionId,
}

#[derive(Iden)]
pub enum TableDataVersions {
    Table,
    Id,
    CollectionId,
    TableId,
    Name,
    TableVersionId,
    FunctionVersionId,
    HasData,
    ColumnCount,
    RowCount,
    SchemaHash,
    ExecutionId,
    TransactionId,
    FunctionRunId,
    FunctionParamPos,
    WithDataTableDataVersionId,
}

#[derive(Iden)]
pub enum TablePartitions {
    Table,
    Id,
    CollectionId,
    TableId,
    TableVersionId,
    FunctionVersionId,
    TableDataVersionId,
    PartitionKey,
    PartitionDeleted,
}

#[derive(Iden)]
pub enum FunctionRequirements {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionRunId,
    RequirementTableId,
    RequirementFunctionVersionId,
    RequirementTableVersionId,
    RequirementFunctionRunId,
    RequirementTableDataVersionId,
    RequirementInputIdx,
    RequirementDependencyPos,
    RequirementVersionPos,
}

#[derive(Iden)]
pub enum Workers {
    Table,
    Id,
    CollectionId,
    ExecutionId,
    TransactionId,
    FunctionVersionId,
    FunctionRunId,
    MessageStatus,
    StartedOn,
    EndedOn,
    Status,
    WorkerType,
    WorkerEnabled,
    WorkerConfig,
}

// ---------------------------------------------------------------------------
// Table definitions
// ---------------------------------------------------------------------------

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        // functions
        Table::create()
            .table(Functions::Table)
            .col(
                ColumnDef::new(Functions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Functions::CollectionId).text().not_null())
            .col(ColumnDef::new(Functions::Name).text().not_null())
            .col(ColumnDef::new(Functions::Description).text().not_null())
            .col(ColumnDef::new(Functions::Decorator).text().not_null())
            .col(ColumnDef::new(Functions::RuntimeValues).text().not_null())
            .col(ColumnDef::new(Functions::FunctionId).text().not_null())
            .col(ColumnDef::new(Functions::DataLocation).text().not_null())
            .col(ColumnDef::new(Functions::StorageVersion).text().not_null())
            .col(ColumnDef::new(Functions::BundleId).text().not_null())
            .col(ColumnDef::new(Functions::Snippet).text().not_null())
            .col(ColumnDef::new(Functions::DefinedOn).timestamp().not_null())
            .col(ColumnDef::new(Functions::DefinedById).text().not_null())
            .col(ColumnDef::new(Functions::Status).text().not_null())
            .col(ColumnDef::new(Functions::Connector).text().null())
            .col(ColumnDef::new(Functions::Enabled).boolean().default(true))
            .foreign_key(
                ForeignKey::create()
                    .from(Functions::Table, Functions::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // tables
        Table::create()
            .table(Tables::Table)
            .col(ColumnDef::new(Tables::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Tables::CollectionId).text().not_null())
            .col(ColumnDef::new(Tables::TableId).text().not_null())
            .col(ColumnDef::new(Tables::Name).text().not_null())
            .col(ColumnDef::new(Tables::FunctionId).text().null())
            .col(ColumnDef::new(Tables::FunctionVersionId).text().not_null())
            .col(ColumnDef::new(Tables::FunctionParamPos).integer().null())
            .col(ColumnDef::new(Tables::Private).boolean().not_null())
            .col(ColumnDef::new(Tables::Partitioned).boolean().null())
            .col(ColumnDef::new(Tables::DefinedOn).timestamp().not_null())
            .col(ColumnDef::new(Tables::DefinedById).text().not_null())
            .col(ColumnDef::new(Tables::Status).text().not_null())
            .col(
                ColumnDef::new(Tables::TableType)
                    .text()
                    .not_null()
                    .default("R"),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Tables::Table, Tables::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Tables::Table, Tables::FunctionVersionId)
                    .to(Functions::Table, Functions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // bundles
        Table::create()
            .table(Bundles::Table)
            .col(ColumnDef::new(Bundles::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Bundles::CollectionId).text().not_null())
            .col(ColumnDef::new(Bundles::Hash).text().not_null())
            .col(ColumnDef::new(Bundles::CreatedOn).timestamp().not_null())
            .col(ColumnDef::new(Bundles::CreatedById).text().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(Bundles::Table, Bundles::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // dependencies
        Table::create()
            .table(Dependencies::Table)
            .col(
                ColumnDef::new(Dependencies::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Dependencies::CollectionId).text().not_null())
            .col(ColumnDef::new(Dependencies::DependencyId).text().not_null())
            .col(ColumnDef::new(Dependencies::FunctionId).text().not_null())
            .col(
                ColumnDef::new(Dependencies::TableCollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Dependencies::TableFunctionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Dependencies::TableId).text().not_null())
            .col(
                ColumnDef::new(Dependencies::TableVersions)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Dependencies::DepPos).integer().not_null())
            .col(ColumnDef::new(Dependencies::Status).text().not_null())
            .col(
                ColumnDef::new(Dependencies::DefinedOn)
                    .timestamp()
                    .not_null(),
            )
            .col(ColumnDef::new(Dependencies::DefinedById).text().not_null())
            .col(ColumnDef::new(Dependencies::System).boolean().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(Dependencies::Table, Dependencies::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // triggers
        Table::create()
            .table(Triggers::Table)
            .col(ColumnDef::new(Triggers::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Triggers::CollectionId).text().not_null())
            .col(ColumnDef::new(Triggers::TriggerId).text().not_null())
            .col(ColumnDef::new(Triggers::FunctionId).text().not_null())
            .col(
                ColumnDef::new(Triggers::TriggerByCollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Triggers::TriggerByFunctionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Triggers::TriggerByTableId).text().not_null())
            .col(ColumnDef::new(Triggers::Status).text().not_null())
            .col(ColumnDef::new(Triggers::DefinedOn).timestamp().not_null())
            .col(ColumnDef::new(Triggers::DefinedById).text().not_null())
            .col(ColumnDef::new(Triggers::System).boolean().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(Triggers::Table, Triggers::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // executions
        Table::create()
            .table(Executions::Table)
            .col(
                ColumnDef::new(Executions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Executions::Name).text().null())
            .col(ColumnDef::new(Executions::CollectionId).text().not_null())
            .col(
                ColumnDef::new(Executions::FunctionVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Executions::TriggeredOn)
                    .timestamp()
                    .not_null(),
            )
            .col(ColumnDef::new(Executions::TriggeredById).text().not_null())
            .col(ColumnDef::new(Executions::RecursionOptions).text().null())
            .col(
                ColumnDef::new(Executions::FunctionId)
                    .text()
                    .not_null()
                    .default(""),
            )
            .col(
                ColumnDef::new(Executions::TriggerType)
                    .text()
                    .not_null()
                    .default("M"),
            )
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
        // transactions
        Table::create()
            .table(Transactions::Table)
            .col(
                ColumnDef::new(Transactions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Transactions::CollectionId).text().not_null())
            .col(ColumnDef::new(Transactions::ExecutionId).text().not_null())
            .col(
                ColumnDef::new(Transactions::TransactionBy)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Transactions::TransactionKey)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Transactions::TriggeredOn)
                    .timestamp()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Transactions::TriggeredById)
                    .text()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Transactions::Table, Transactions::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Transactions::Table, Transactions::ExecutionId)
                    .to(Executions::Table, Executions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // function_runs
        Table::create()
            .table(FunctionRuns::Table)
            .col(
                ColumnDef::new(FunctionRuns::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(FunctionRuns::CollectionId).text().not_null())
            .col(
                ColumnDef::new(FunctionRuns::FunctionVersionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(FunctionRuns::ExecutionId).text().not_null())
            .col(
                ColumnDef::new(FunctionRuns::TransactionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRuns::TriggeredOn)
                    .timestamp()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRuns::TriggeredById)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(FunctionRuns::Trigger).text().not_null())
            .col(ColumnDef::new(FunctionRuns::StartedOn).timestamp().null())
            .col(ColumnDef::new(FunctionRuns::EndedOn).timestamp().null())
            .col(ColumnDef::new(FunctionRuns::Status).text().not_null())
            .col(
                ColumnDef::new(FunctionRuns::FunctionId)
                    .text()
                    .not_null()
                    .default(""),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(FunctionRuns::Table, FunctionRuns::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(FunctionRuns::Table, FunctionRuns::FunctionVersionId)
                    .to(Functions::Table, Functions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(FunctionRuns::Table, FunctionRuns::ExecutionId)
                    .to(Executions::Table, Executions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // table_data_versions
        Table::create()
            .table(TableDataVersions::Table)
            .col(
                ColumnDef::new(TableDataVersions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(TableDataVersions::CollectionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(TableDataVersions::TableId).text().not_null())
            .col(ColumnDef::new(TableDataVersions::Name).text().not_null())
            .col(
                ColumnDef::new(TableDataVersions::TableVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TableDataVersions::FunctionVersionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(TableDataVersions::HasData).boolean().null())
            .col(
                ColumnDef::new(TableDataVersions::ColumnCount)
                    .integer()
                    .null(),
            )
            .col(ColumnDef::new(TableDataVersions::RowCount).integer().null())
            .col(ColumnDef::new(TableDataVersions::SchemaHash).text().null())
            .col(
                ColumnDef::new(TableDataVersions::ExecutionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TableDataVersions::TransactionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TableDataVersions::FunctionRunId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TableDataVersions::FunctionParamPos)
                    .integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TableDataVersions::WithDataTableDataVersionId)
                    .text()
                    .null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(TableDataVersions::Table, TableDataVersions::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        TableDataVersions::Table,
                        TableDataVersions::FunctionVersionId,
                    )
                    .to(Functions::Table, Functions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(TableDataVersions::Table, TableDataVersions::ExecutionId)
                    .to(Executions::Table, Executions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(TableDataVersions::Table, TableDataVersions::FunctionRunId)
                    .to(FunctionRuns::Table, FunctionRuns::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // table_partitions
        Table::create()
            .table(TablePartitions::Table)
            .col(
                ColumnDef::new(TablePartitions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(TablePartitions::CollectionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(TablePartitions::TableId).text().not_null())
            .col(
                ColumnDef::new(TablePartitions::TableVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TablePartitions::FunctionVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TablePartitions::TableDataVersionId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(TablePartitions::PartitionKey).text().null())
            .col(
                ColumnDef::new(TablePartitions::PartitionDeleted)
                    .boolean()
                    .null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(TablePartitions::Table, TablePartitions::CollectionId)
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(TablePartitions::Table, TablePartitions::FunctionVersionId)
                    .to(Functions::Table, Functions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(TablePartitions::Table, TablePartitions::TableDataVersionId)
                    .to(TableDataVersions::Table, TableDataVersions::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // function_requirements
        Table::create()
            .table(FunctionRequirements::Table)
            .col(
                ColumnDef::new(FunctionRequirements::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::CollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::ExecutionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::TransactionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::FunctionRunId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementTableId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementFunctionVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementTableVersionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementFunctionRunId)
                    .text()
                    .null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementTableDataVersionId)
                    .text()
                    .null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementInputIdx)
                    .integer()
                    .null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementDependencyPos)
                    .integer()
                    .null(),
            )
            .col(
                ColumnDef::new(FunctionRequirements::RequirementVersionPos)
                    .integer()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        FunctionRequirements::Table,
                        FunctionRequirements::CollectionId,
                    )
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        // workers
        Table::create()
            .table(Workers::Table)
            .col(ColumnDef::new(Workers::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Workers::CollectionId).text().not_null())
            .col(ColumnDef::new(Workers::ExecutionId).text().null())
            .col(ColumnDef::new(Workers::TransactionId).text().null())
            .col(ColumnDef::new(Workers::FunctionVersionId).text().not_null())
            .col(ColumnDef::new(Workers::FunctionRunId).text().null())
            .col(ColumnDef::new(Workers::MessageStatus).text().not_null())
            .col(ColumnDef::new(Workers::StartedOn).timestamp().null())
            .col(ColumnDef::new(Workers::EndedOn).timestamp().null())
            .col(ColumnDef::new(Workers::Status).text().not_null())
            .col(ColumnDef::new(Workers::WorkerType).text().not_null())
            .col(ColumnDef::new(Workers::WorkerEnabled).boolean().not_null())
            .col(ColumnDef::new(Workers::WorkerConfig).blob().null())
            .to_owned(),
    ]
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

pub fn views(b: &Backend) -> Vec<ViewDef> {
    let mut v = vec![
        // functions__with_names
        ViewDef::new(
            "functions__with_names",
            Query::select()
                .expr(Expr::col((Functions::Table, Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    b.name_or_id("users.name", "functions.defined_by_id"),
                    Alias::new("defined_by"),
                )
                .from(Functions::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Functions::Table, Functions::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Functions::Table, Functions::DefinedById))
                        .equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        ),
        // tables__with_names
        ViewDef::new(
            "tables__with_names",
            Query::select()
                .expr(Expr::col((Tables::Table, Asterisk)))
                .expr_as(
                    Expr::col((Tables::Table, Tables::FunctionParamPos)).lt(0),
                    Alias::new("system"),
                )
                .expr_as(
                    b.name_or_id("users.name", "tables.defined_by_id"),
                    Alias::new("defined_by"),
                )
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Functions::Table, Functions::Name)),
                    Alias::new("function"),
                )
                .from(Tables::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Tables::Table, Tables::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Tables::Table, Tables::FunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Tables::Table, Tables::DefinedById))
                        .equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        ),
        // dependencies__with_names (collections joined twice: once for collection_id, once for table_collection_id)
        ViewDef::new(
            "dependencies__with_names",
            Query::select()
                .expr(Expr::col((Dependencies::Table, Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Alias::new("table_collection"), Collections::Name)),
                    Alias::new("trigger_by_collection"),
                )
                .expr_as(
                    Expr::col((Alias::new("table_collection"), Collections::Name)),
                    Alias::new("table_collection"),
                )
                .from(Dependencies::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Dependencies::Table, Dependencies::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Dependencies::Table, Dependencies::DefinedById))
                        .equals((Users::Table, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Alias::new("table_collection"),
                    Expr::col((Dependencies::Table, Dependencies::TableCollectionId))
                        .equals((Alias::new("table_collection"), Collections::Id)),
                )
                .to_owned(),
        ),
        // triggers__with_names (collections joined twice: once for collection_id, once for trigger_by_collection_id)
        ViewDef::new(
            "triggers__with_names",
            Query::select()
                .expr(Expr::col((Triggers::Table, Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Alias::new("trigger_by_collection"), Collections::Name)),
                    Alias::new("trigger_by_collection"),
                )
                .from(Triggers::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Triggers::Table, Triggers::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Triggers::Table, Triggers::DefinedById))
                        .equals((Users::Table, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Alias::new("trigger_by_collection"),
                    Expr::col((Triggers::Table, Triggers::TriggerByCollectionId))
                        .equals((Alias::new("trigger_by_collection"), Collections::Id)),
                )
                .to_owned(),
        ),
        // triggers__read
        ViewDef::new(
            "triggers__read",
            Query::select()
                .expr(Expr::col(Asterisk))
                .from(Alias::new("triggers__with_names"))
                .and_where(
                    Expr::col((Alias::new("triggers__with_names"), Alias::new("system"))).eq(false),
                )
                .to_owned(),
        ),
        // executions__with_names
        ViewDef::new(
            "executions__with_names",
            Query::select()
                .expr(Expr::col((Executions::Table, Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Functions::Table, Functions::Name)),
                    Alias::new("function"),
                )
                .expr_as(
                    b.name_or_id("users.name", "executions.triggered_by_id"),
                    Alias::new("triggered_by"),
                )
                .from(Executions::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Executions::Table, Executions::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Executions::Table, Executions::FunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Executions::Table, Executions::TriggeredById))
                        .equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        ),
        // transactions__with_names
        ViewDef::new(
            "transactions__with_names",
            Query::select()
                .expr(Expr::col((Transactions::Table, Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Executions::Table, Executions::Name)),
                    Alias::new("execution"),
                )
                .expr_as(
                    b.name_or_id("users.name", "transactions.triggered_by_id"),
                    Alias::new("triggered_by"),
                )
                .from(Transactions::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Transactions::Table, Transactions::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((Transactions::Table, Transactions::ExecutionId))
                        .equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Transactions::Table, Transactions::TriggeredById))
                        .equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        ),
        // function_runs__with_names
        ViewDef::new(
            "function_runs__with_names",
            Query::select()
                .expr(Expr::col((FunctionRuns::Table, Asterisk)))
                .expr_as(
                    Expr::col((Functions::Table, Functions::Name)),
                    Alias::new("name"),
                )
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Executions::Table, Executions::Name)),
                    Alias::new("execution"),
                )
                .expr_as(
                    Expr::col((Executions::Table, Executions::FunctionVersionId)),
                    Alias::new("execution_function_version_id"),
                )
                .expr_as(
                    b.name_or_id("users.name", "function_runs.triggered_by_id"),
                    Alias::new("triggered_by"),
                )
                .from(FunctionRuns::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((FunctionRuns::Table, FunctionRuns::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((FunctionRuns::Table, FunctionRuns::FunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Executions::Table,
                    Expr::col((FunctionRuns::Table, FunctionRuns::ExecutionId))
                        .equals((Executions::Table, Executions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((FunctionRuns::Table, FunctionRuns::TriggeredById))
                        .equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        ),
        // table_data_versions__with_function
        ViewDef::new(
            "table_data_versions__with_function",
            Query::select()
                .expr(Expr::col((TableDataVersions::Table, Asterisk)))
                .expr_as(
                    Expr::col((FunctionRuns::Table, FunctionRuns::TriggeredOn)),
                    Alias::new("triggered_on"),
                )
                .expr_as(
                    Expr::col((FunctionRuns::Table, FunctionRuns::TriggeredById)),
                    Alias::new("triggered_by_id"),
                )
                .expr_as(
                    Expr::col((FunctionRuns::Table, FunctionRuns::StartedOn)),
                    Alias::new("started_on"),
                )
                .expr_as(
                    Expr::col((FunctionRuns::Table, FunctionRuns::EndedOn)),
                    Alias::new("ended_on"),
                )
                .expr_as(
                    Expr::col((FunctionRuns::Table, FunctionRuns::Status)),
                    Alias::new("status"),
                )
                .expr_as(
                    Expr::col((Functions::Table, Functions::DataLocation)),
                    Alias::new("data_location"),
                )
                .expr_as(
                    Expr::col((Functions::Table, Functions::StorageVersion)),
                    Alias::new("storage_version"),
                )
                .from(TableDataVersions::Table)
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((
                        TableDataVersions::Table,
                        TableDataVersions::FunctionVersionId,
                    ))
                    .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    FunctionRuns::Table,
                    Expr::col((TableDataVersions::Table, TableDataVersions::FunctionRunId))
                        .equals((FunctionRuns::Table, FunctionRuns::Id)),
                )
                .to_owned(),
        ),
        // table_data_versions__active
        ViewDef::new(
            "table_data_versions__active",
            Query::select()
                .expr(Expr::col(Asterisk))
                .from(Alias::new("table_data_versions__with_function"))
                .and_where(Expr::col(Alias::new("status")).is_not_in(["Y", "X"]))
                .to_owned(),
        ),
    ];

    // table_data_versions__with_names
    {
        let tdv_view = Alias::new("table_data_versions__with_function");
        v.push(ViewDef::new(
            "table_data_versions__with_names",
            Query::select()
                .expr(Expr::col((tdv_view.clone(), Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Functions::Table, Functions::Name)),
                    Alias::new("function"),
                )
                .expr_as(
                    b.name_or_id(
                        "users.name",
                        "table_data_versions__with_function.triggered_by_id",
                    ),
                    Alias::new("created_by"),
                )
                .from(tdv_view.clone())
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((tdv_view.clone(), TableDataVersions::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((tdv_view.clone(), TableDataVersions::FunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((tdv_view, Alias::new("triggered_by_id")))
                        .equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        ));
    }

    // function_requirements__with_status
    {
        let tdv_view = Alias::new("table_data_versions__with_function");
        v.push(ViewDef::new(
            "function_requirements__with_status",
            Query::select()
                .expr(Expr::col((FunctionRequirements::Table, Asterisk)))
                .expr_as(
                    Expr::case(
                        Expr::col((
                            FunctionRequirements::Table,
                            FunctionRequirements::RequirementTableDataVersionId,
                        ))
                        .is_null(),
                        Expr::val("C"),
                    )
                    .finally(Expr::col((tdv_view.clone(), Alias::new("status")))),
                    Alias::new("status"),
                )
                .from(FunctionRequirements::Table)
                .join_as(
                    JoinType::LeftJoin,
                    tdv_view.clone(),
                    tdv_view.clone(),
                    Expr::col((
                        FunctionRequirements::Table,
                        FunctionRequirements::RequirementTableDataVersionId,
                    ))
                    .equals((tdv_view, Alias::new("id"))),
                )
                .to_owned(),
        ));
    }

    // function_requirements__with_names
    {
        let req_view = Alias::new("function_requirements__with_status");
        v.push(ViewDef::new(
            "function_requirements__with_names",
            Query::select()
                .expr(Expr::col((req_view.clone(), Asterisk)))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("collection"),
                )
                .expr_as(
                    Expr::col((Functions::Table, Functions::Name)),
                    Alias::new("function"),
                )
                .expr_as(
                    Expr::col((Tables::Table, Alias::new("name"))),
                    Alias::new("requirement_table"),
                )
                .from(req_view.clone())
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((req_view.clone(), FunctionRequirements::CollectionId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Tables::Table,
                    Expr::col((
                        req_view.clone(),
                        FunctionRequirements::RequirementTableVersionId,
                    ))
                    .equals((Tables::Table, Tables::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((req_view, FunctionRequirements::RequirementFunctionVersionId))
                        .equals((Functions::Table, Functions::Id)),
                )
                .to_owned(),
        ));
    }

    // Status summary views (dialect-specific JSON aggregation)
    v.extend(status_summary_views(b));

    // executions__with_status (depends on execution_status_summary)
    {
        let exec_view = Alias::new("executions__with_names");
        let ess_view = Alias::new("execution_status_summary");
        v.push(ViewDef::new(
            "executions__with_status",
            Query::select()
                .expr(Expr::col((exec_view.clone(), Asterisk)))
                .expr_as(
                    Func::min(Expr::col((FunctionRuns::Table, FunctionRuns::StartedOn))),
                    Alias::new("started_on"),
                )
                .expr_as(
                    Expr::case(
                        Expr::col((ess_view.clone(), Alias::new("finished"))),
                        Func::max(Expr::col((FunctionRuns::Table, FunctionRuns::EndedOn))),
                    ),
                    Alias::new("ended_on"),
                )
                .expr_as(
                    Expr::col((ess_view.clone(), Alias::new("status"))),
                    Alias::new("status"),
                )
                .expr_as(
                    Expr::col((ess_view.clone(), Alias::new("function_run_status_count"))),
                    Alias::new("function_run_status_count"),
                )
                .from(exec_view.clone())
                .join(
                    JoinType::LeftJoin,
                    FunctionRuns::Table,
                    Expr::col((FunctionRuns::Table, FunctionRuns::ExecutionId))
                        .equals((exec_view.clone(), Alias::new("id"))),
                )
                .join_as(
                    JoinType::LeftJoin,
                    ess_view.clone(),
                    ess_view.clone(),
                    Expr::col((ess_view, Alias::new("execution_id")))
                        .equals((exec_view.clone(), Alias::new("id"))),
                )
                .group_by_col((exec_view, Alias::new("id")))
                .to_owned(),
        ));
    }

    // transactions__with_status (depends on transaction_status_summary)
    {
        let txn_view = Alias::new("transactions__with_names");
        let tss_view = Alias::new("transaction_status_summary");
        v.push(ViewDef::new(
            "transactions__with_status",
            Query::select()
                .expr(Expr::col((txn_view.clone(), Asterisk)))
                .expr_as(
                    Func::min(Expr::col((FunctionRuns::Table, FunctionRuns::StartedOn))),
                    Alias::new("started_on"),
                )
                .expr_as(
                    Expr::case(
                        Expr::col((tss_view.clone(), Alias::new("finished"))),
                        Func::max(Expr::col((FunctionRuns::Table, FunctionRuns::EndedOn))),
                    ),
                    Alias::new("ended_on"),
                )
                .expr_as(
                    Expr::col((tss_view.clone(), Alias::new("status"))),
                    Alias::new("status"),
                )
                .expr_as(
                    Expr::col((tss_view.clone(), Alias::new("function_run_status_count"))),
                    Alias::new("function_run_status_count"),
                )
                .from(txn_view.clone())
                .join(
                    JoinType::LeftJoin,
                    FunctionRuns::Table,
                    Expr::col((FunctionRuns::Table, FunctionRuns::TransactionId))
                        .equals((txn_view.clone(), Alias::new("id"))),
                )
                .join_as(
                    JoinType::LeftJoin,
                    tss_view.clone(),
                    tss_view.clone(),
                    Expr::col((tss_view, Alias::new("transaction_id")))
                        .equals((txn_view.clone(), Alias::new("id"))),
                )
                .group_by_col((txn_view, Alias::new("id")))
                .to_owned(),
        ));
    }

    // function_runs__to_execute (CTE)
    {
        let cte_select = Query::select()
            .distinct()
            .column(Alias::new("function_run_id"))
            .from(Alias::new("function_requirements__with_status"))
            .and_where(Expr::col(Alias::new("status")).is_not_in(["D", "C"]))
            .to_owned();

        let cte = CommonTableExpression::new()
            .query(cte_select)
            .table_name(Alias::new("runs_with_pending_reqs"))
            .to_owned();

        let with_clause = WithClause::new().cte(cte).to_owned();

        let fr_view = Alias::new("function_runs__with_names");
        let pending_cte = Alias::new("runs_with_pending_reqs");
        v.push(ViewDef::with_cte(
            "function_runs__to_execute",
            Query::select()
                .expr(Expr::col((fr_view.clone(), Asterisk)))
                .expr(Expr::col((Functions::Table, Functions::DataLocation)))
                .expr(Expr::col((Functions::Table, Functions::StorageVersion)))
                .expr(Expr::col((Functions::Table, Functions::BundleId)))
                .expr(Expr::col((Functions::Table, Functions::DefinedOn)))
                .from(fr_view.clone())
                .join(
                    JoinType::Join,
                    Functions::Table,
                    Expr::col((fr_view.clone(), Alias::new("function_version_id")))
                        .equals((Functions::Table, Functions::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    pending_cte.clone(),
                    pending_cte.clone(),
                    Expr::col((fr_view.clone(), Alias::new("id")))
                        .equals((pending_cte.clone(), Alias::new("function_run_id"))),
                )
                .and_where(Expr::col((fr_view, Alias::new("status"))).is_in(["S", "RS"]))
                .and_where(Expr::col((pending_cte, Alias::new("function_run_id"))).is_null())
                .to_owned(),
            with_clause,
        ));
    }

    // function_runs__to_commit (2 CTEs)
    {
        let cte1_select = Query::select()
            .column(FunctionRuns::TransactionId)
            .from(FunctionRuns::Table)
            .group_by_col(FunctionRuns::TransactionId)
            .and_having(
                Expr::expr(Func::sum(
                    Expr::case(Expr::col(FunctionRuns::Status).ne("D"), 1).finally(0),
                ))
                .eq(0),
            )
            .to_owned();

        let cte1 = CommonTableExpression::new()
            .query(cte1_select)
            .table_name(Alias::new("transactions_with_done_runs"))
            .to_owned();

        let cte2_select = Query::select()
            .distinct()
            .column(Alias::new("transaction_id"))
            .from(Alias::new("function_requirements__with_status"))
            .and_where(Expr::col(Alias::new("status")).is_not_in(["D", "C"]))
            .to_owned();

        let cte2 = CommonTableExpression::new()
            .query(cte2_select)
            .table_name(Alias::new("transactions_with_pending_reqs"))
            .to_owned();

        let with_clause = WithClause::new().cte(cte1).cte(cte2).to_owned();

        let done_cte = Alias::new("transactions_with_done_runs");
        let pending_cte = Alias::new("transactions_with_pending_reqs");
        v.push(ViewDef::with_cte(
            "function_runs__to_commit",
            Query::select()
                .expr(Expr::col((FunctionRuns::Table, Asterisk)))
                .from(FunctionRuns::Table)
                .join_as(
                    JoinType::Join,
                    done_cte.clone(),
                    done_cte.clone(),
                    Expr::col((FunctionRuns::Table, FunctionRuns::TransactionId))
                        .equals((done_cte, Alias::new("transaction_id"))),
                )
                .join_as(
                    JoinType::LeftJoin,
                    pending_cte.clone(),
                    pending_cte.clone(),
                    Expr::col((FunctionRuns::Table, FunctionRuns::TransactionId))
                        .equals((pending_cte.clone(), Alias::new("transaction_id"))),
                )
                .and_where(Expr::col((pending_cte, Alias::new("transaction_id"))).is_null())
                .to_owned(),
            with_clause,
        ));
    }

    // workers__with_names
    v.push(ViewDef::new(
        "workers__with_names",
        Query::select()
            .expr(Expr::col((Workers::Table, Asterisk)))
            .expr_as(
                Expr::col((Collections::Table, Collections::Name)),
                Alias::new("collection"),
            )
            .expr_as(
                Expr::col((Executions::Table, Executions::Name)),
                Alias::new("execution"),
            )
            .expr_as(
                Expr::col((Functions::Table, Functions::Name)),
                Alias::new("function"),
            )
            .expr_as(
                Expr::col((Functions::Table, Functions::Enabled)),
                Alias::new("enabled"),
            )
            .from(Workers::Table)
            .join(
                JoinType::LeftJoin,
                Collections::Table,
                Expr::col((Workers::Table, Workers::CollectionId))
                    .equals((Collections::Table, Collections::Id)),
            )
            .join(
                JoinType::LeftJoin,
                Executions::Table,
                Expr::col((Workers::Table, Workers::ExecutionId))
                    .equals((Executions::Table, Executions::Id)),
            )
            .join(
                JoinType::LeftJoin,
                Functions::Table,
                Expr::col((Workers::Table, Workers::FunctionVersionId))
                    .equals((Functions::Table, Functions::Id)),
            )
            .and_where(Expr::col((Workers::Table, Workers::WorkerEnabled)).eq(true))
            .to_owned(),
    ));

    v
}

/// Status summary views that use dialect-specific JSON aggregation.
/// The CASE/COUNT aggregate expressions use sea-query builders; only the
/// outermost JSON aggregation wrapper remains as `Expr::cust()` because
/// the function name is inherently dialect-specific.
fn status_summary_views(b: &Backend) -> Vec<ViewDef> {
    // Helper: COUNT(CASE WHEN function_runs.status IN (values) THEN 1 END)
    let count_status_in = |statuses: &[&str]| {
        Expr::expr(Func::count(Expr::expr(Expr::case(
            Expr::col((FunctionRuns::Table, FunctionRuns::Status)).is_in(statuses.iter().copied()),
            Expr::val(1),
        ))))
    };

    // Helper: COUNT(function_runs.status)
    let count_all = || {
        Expr::expr(Func::count(Expr::col((
            FunctionRuns::Table,
            FunctionRuns::Status,
        ))))
    };

    // --- status CASE (global + execution) ---
    let status_case = Expr::case(count_status_in(&["S"]).eq(count_all()), Expr::val("S"))
        .case(
            count_status_in(&["C", "X", "Y"]).eq(count_all()),
            Expr::val("F"),
        )
        .case(
            count_status_in(&["C", "D", "F", "H"])
                .eq(count_all())
                .and(count_status_in(&["F", "H"]).gt(0)),
            Expr::val("L"),
        )
        .case(
            count_status_in(&["S", "RR", "RS", "R", "D", "E"]).gt(0),
            Expr::val("R"),
        )
        .finally(Expr::val("U"));

    // --- finished (global + execution) ---
    let finished =
        Expr::expr(Func::count(Expr::col(Asterisk))).eq(count_status_in(&["C", "X", "Y"]));

    // --- txn status CASE (transaction — individual C/X/Y branches) ---
    let txn_status_case = Expr::case(count_status_in(&["S"]).eq(count_all()), Expr::val("S"))
        .case(count_status_in(&["C"]).eq(count_all()), Expr::val("C"))
        .case(count_status_in(&["X"]).eq(count_all()), Expr::val("X"))
        .case(count_status_in(&["Y"]).eq(count_all()), Expr::val("Y"))
        .case(
            count_status_in(&["D", "F", "H"])
                .eq(count_all())
                .and(count_status_in(&["F", "H"]).gt(0)),
            Expr::val("L"),
        )
        .case(
            count_status_in(&["S", "RR", "RS", "R", "D", "E"]).gt(0),
            Expr::val("R"),
        )
        .finally(Expr::val("U"));

    // --- txn finished ---
    let txn_finished =
        Expr::expr(Func::count(Expr::col(Asterisk))).eq(count_status_in(&["C", "X", "Y"]));

    // --- JSON subqueries (inner SELECTs via sea-query, JSON agg wrapper via format!) ---
    let json_agg = b.json_object_agg("inner_fr.status", "inner_fr.count");

    let inner_global_sql = b.build_query(
        Query::select()
            .expr(Expr::col((FunctionRuns::Table, FunctionRuns::Status)))
            .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
            .from(FunctionRuns::Table)
            .group_by_col((FunctionRuns::Table, FunctionRuns::Status))
            .to_owned(),
    );
    let json_subquery = format!("(SELECT {json_agg} FROM ({inner_global_sql}) AS inner_fr)");

    let inner_exec_sql = b.build_query(
        Query::select()
            .expr(Expr::col((FunctionRuns::Table, FunctionRuns::Status)))
            .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
            .from(FunctionRuns::Table)
            .and_where(
                Expr::col((FunctionRuns::Table, FunctionRuns::ExecutionId))
                    .equals((Executions::Table, Executions::Id)),
            )
            .group_by_col((FunctionRuns::Table, FunctionRuns::Status))
            .to_owned(),
    );
    let exec_json_subquery = format!("(SELECT {json_agg} FROM ({inner_exec_sql}) AS inner_fr)");

    let inner_txn_sql = b.build_query(
        Query::select()
            .expr(Expr::col((FunctionRuns::Table, FunctionRuns::Status)))
            .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
            .from(FunctionRuns::Table)
            .and_where(
                Expr::col((FunctionRuns::Table, FunctionRuns::TransactionId))
                    .equals((Transactions::Table, Transactions::Id)),
            )
            .group_by_col((FunctionRuns::Table, FunctionRuns::Status))
            .to_owned(),
    );
    let txn_json_subquery = format!("(SELECT {json_agg} FROM ({inner_txn_sql}) AS inner_fr)");

    // --- View definitions ---
    let global = ViewDef::new(
        "global_status_summary",
        Query::select()
            .expr_as(status_case.clone(), Alias::new("status"))
            .expr_as(finished.clone(), Alias::new("finished"))
            .expr_as(
                Expr::cust(json_subquery),
                Alias::new("function_run_status_count"),
            )
            .from(FunctionRuns::Table)
            .to_owned(),
    );

    let execution = ViewDef::new(
        "execution_status_summary",
        Query::select()
            .expr_as(
                Expr::col((Executions::Table, Executions::Id)),
                Alias::new("execution_id"),
            )
            .expr_as(status_case, Alias::new("status"))
            .expr_as(finished, Alias::new("finished"))
            .expr_as(
                Expr::cust(exec_json_subquery),
                Alias::new("function_run_status_count"),
            )
            .from(Executions::Table)
            .join(
                JoinType::LeftJoin,
                FunctionRuns::Table,
                Expr::col((FunctionRuns::Table, FunctionRuns::ExecutionId))
                    .equals((Executions::Table, Executions::Id)),
            )
            .group_by_col((Executions::Table, Executions::Id))
            .to_owned(),
    );

    let transaction = ViewDef::new(
        "transaction_status_summary",
        Query::select()
            .expr_as(
                Expr::col((Transactions::Table, Transactions::Id)),
                Alias::new("transaction_id"),
            )
            .expr_as(txn_status_case, Alias::new("status"))
            .expr_as(txn_finished, Alias::new("finished"))
            .expr_as(
                Expr::cust(txn_json_subquery),
                Alias::new("function_run_status_count"),
            )
            .from(Transactions::Table)
            .join(
                JoinType::LeftJoin,
                FunctionRuns::Table,
                Expr::col((FunctionRuns::Table, FunctionRuns::TransactionId))
                    .equals((Transactions::Table, Transactions::Id)),
            )
            .group_by_col((Transactions::Table, Transactions::Id))
            .to_owned(),
    );

    vec![global, execution, transaction]
}

pub fn seed() -> Vec<InsertStatement> {
    vec![]
}
