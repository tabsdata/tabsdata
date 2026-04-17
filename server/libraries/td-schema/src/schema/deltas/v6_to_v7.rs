//
// Copyright 2026 Tabs Data Inc.
//

//! Migration from v6 to v7:
//!
//! 0. Deduplicate `triggered_on` from `table_data_versions`.
//! 1. Remove `triggered_on` from `table_data_versions__with_function` view.
//! 2. Add `_current` tables for functions, tables, dependencies, and triggers
//!    with triggers, views, seed data, and indices.

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Func, Iden, Index, JoinType, Order, OverStatement,
    Query, Table, WindowStatement,
};

use crate::schema::SchemaDelta;
use crate::schema::backend::{Backend, TriggerDef, ViewDef};
use tracing::info;

// Local Iden enums — frozen for this delta, independent of current/.

#[derive(Iden)]
enum TableDataVersions {
    Table,
    Id,
    TableId,
    FunctionVersionId,
    FunctionRunId,
    TriggeredOn,
}

#[derive(Iden)]
enum Functions {
    Table,
    Id,
    Name,
    DataLocation,
    StorageVersion,
}

#[derive(Iden)]
enum FunctionRuns {
    Table,
    Id,
    TriggeredById,
    StartedOn,
    EndedOn,
    Status,
    TriggeredOn,
}

#[derive(Iden)]
enum Collections {
    Table,
    Id,
    Name,
}

#[derive(Iden)]
enum Users {
    Table,
    Id,
}

#[derive(Iden)]
#[iden = "functions_current"]
enum FunctionsCurrent {
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
#[iden = "tables_current"]
enum TablesCurrent {
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
#[iden = "dependencies_current"]
enum DependenciesCurrent {
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
#[iden = "triggers_current"]
enum TriggersCurrent {
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

/// Marker type for the v6-to-v7 migration.
pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 6;
    const TO_VERSION: usize = 7;
    const FINGERPRINTS: [&'static str; 3] = [
        "0f39251e4ef93ee863b54091acaaf8ea7d98b03cf281f7ccc426bd087531e20b", // Sqlite
        "a44f915d715532dda4832ca08e136699996686d3d9a849b396ebe2e62d92f558", // Postgres
        "40d31379fc933664d9835b3c540ac7f20e8739b8749f18ac9f1e10f8617725ea", // MySql
    ];

    fn up(b: Backend) -> Vec<String> {
        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            backend = ?b,
            "Applying migration"
        );

        let mut stmts = Vec::new();

        // 0. Denormalize triggered_on into table_data_versions
        stmts.push(
            b.build_schema(
                Table::alter()
                    .table(TableDataVersions::Table)
                    .add_column(
                        ColumnDef::new(TableDataVersions::TriggeredOn)
                            .timestamp()
                            .not_null()
                            .default("1970-01-01 00:00:00"),
                    )
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_query(
                Query::update()
                    .table(TableDataVersions::Table)
                    .value(
                        TableDataVersions::TriggeredOn,
                        Query::select()
                            .expr(Expr::col((FunctionRuns::Table, FunctionRuns::TriggeredOn)))
                            .from(FunctionRuns::Table)
                            .and_where(Expr::col((FunctionRuns::Table, FunctionRuns::Id)).equals((
                                TableDataVersions::Table,
                                TableDataVersions::FunctionRunId,
                            )))
                            .to_owned(),
                    )
                    .to_owned(),
            ),
        );

        // 1. Drop and recreate table_data_versions__with_function view
        //    (remove triggered_on column).
        {
            let view_name = "table_data_versions__with_function";
            let select = Query::select()
                .expr(Expr::col((TableDataVersions::Table, Asterisk)))
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
                .to_owned();
            stmts.push(b.drop_view(view_name));
            stmts.push(b.create_view(&ViewDef::new(view_name, select)));
        }

        // 2. CREATE TABLE functions_current
        stmts.push(
            b.build_schema(
                Table::create()
                    .table(FunctionsCurrent::Table)
                    .col(
                        ColumnDef::new(FunctionsCurrent::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::CollectionId)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(FunctionsCurrent::Name).text().not_null())
                    .col(
                        ColumnDef::new(FunctionsCurrent::Description)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::Decorator)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::RuntimeValues)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::FunctionId)
                            .text()
                            .not_null()
                            .unique_key(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::DataLocation)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::StorageVersion)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(FunctionsCurrent::BundleId).text().not_null())
                    .col(ColumnDef::new(FunctionsCurrent::Snippet).text().not_null())
                    .col(
                        ColumnDef::new(FunctionsCurrent::DefinedOn)
                            .timestamp()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FunctionsCurrent::DefinedById)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(FunctionsCurrent::Status).text().not_null())
                    .col(ColumnDef::new(FunctionsCurrent::Connector).text().null())
                    .col(
                        ColumnDef::new(FunctionsCurrent::Enabled)
                            .boolean()
                            .default(true),
                    )
                    .to_owned(),
            ),
        );

        // 3. CREATE TABLE tables_current
        stmts.push(
            b.build_schema(
                Table::create()
                    .table(TablesCurrent::Table)
                    .col(
                        ColumnDef::new(TablesCurrent::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(TablesCurrent::CollectionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TablesCurrent::TableId)
                            .text()
                            .not_null()
                            .unique_key(),
                    )
                    .col(ColumnDef::new(TablesCurrent::Name).text().not_null())
                    .col(ColumnDef::new(TablesCurrent::FunctionId).text().null())
                    .col(
                        ColumnDef::new(TablesCurrent::FunctionVersionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TablesCurrent::FunctionParamPos)
                            .integer()
                            .null(),
                    )
                    .col(ColumnDef::new(TablesCurrent::Private).boolean().not_null())
                    .col(ColumnDef::new(TablesCurrent::Partitioned).boolean().null())
                    .col(
                        ColumnDef::new(TablesCurrent::DefinedOn)
                            .timestamp()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TablesCurrent::DefinedById).text().not_null())
                    .col(ColumnDef::new(TablesCurrent::Status).text().not_null())
                    .col(
                        ColumnDef::new(TablesCurrent::TableType)
                            .text()
                            .not_null()
                            .default("R"),
                    )
                    .to_owned(),
            ),
        );

        // 4. CREATE TRIGGER for functions → functions_current
        stmts.push(b.create_trigger(&TriggerDef {
            name: "functions__maintain_current",
            source_table: "functions",
            target_table: "functions_current",
            partition_col: "function_id",
            columns: vec![
                "id",
                "collection_id",
                "name",
                "description",
                "decorator",
                "runtime_values",
                "function_id",
                "data_location",
                "storage_version",
                "bundle_id",
                "snippet",
                "defined_on",
                "defined_by_id",
                "status",
                "connector",
                "enabled",
            ],
        }));

        // 5. CREATE TRIGGER for tables → tables_current
        stmts.push(b.create_trigger(&TriggerDef {
            name: "tables__maintain_current",
            source_table: "tables",
            target_table: "tables_current",
            partition_col: "table_id",
            columns: vec![
                "id",
                "collection_id",
                "table_id",
                "name",
                "function_id",
                "function_version_id",
                "function_param_pos",
                "private",
                "partitioned",
                "defined_on",
                "defined_by_id",
                "status",
                "table_type",
            ],
        }));

        // 6. Seed functions_current from existing data
        stmts.push(seed_current_table(
            &b,
            "functions_current",
            "functions",
            "function_id",
            "defined_on",
            &[
                "id",
                "collection_id",
                "name",
                "description",
                "decorator",
                "runtime_values",
                "function_id",
                "data_location",
                "storage_version",
                "bundle_id",
                "snippet",
                "defined_on",
                "defined_by_id",
                "status",
                "connector",
                "enabled",
            ],
        ));

        // 7. Seed tables_current from existing data
        stmts.push(seed_current_table(
            &b,
            "tables_current",
            "tables",
            "table_id",
            "defined_on",
            &[
                "id",
                "collection_id",
                "table_id",
                "name",
                "function_id",
                "function_version_id",
                "function_param_pos",
                "private",
                "partitioned",
                "defined_on",
                "defined_by_id",
                "status",
                "table_type",
            ],
        ));

        // 8. CREATE VIEW functions_current__with_names
        stmts.push(
            b.create_view(&ViewDef::new(
                "functions_current__with_names",
                Query::select()
                    .expr(Expr::col((FunctionsCurrent::Table, Asterisk)))
                    .expr_as(
                        Expr::col((Collections::Table, Collections::Name)),
                        Alias::new("collection"),
                    )
                    .expr_as(
                        b.name_or_id("users.name", "functions_current.defined_by_id"),
                        Alias::new("defined_by"),
                    )
                    .from(FunctionsCurrent::Table)
                    .join(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Expr::col((FunctionsCurrent::Table, FunctionsCurrent::CollectionId))
                            .equals((Collections::Table, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((FunctionsCurrent::Table, FunctionsCurrent::DefinedById))
                            .equals((Users::Table, Users::Id)),
                    )
                    .to_owned(),
            )),
        );

        // 9. CREATE VIEW tables_current__with_names
        stmts.push(
            b.create_view(&ViewDef::new(
                "tables_current__with_names",
                Query::select()
                    .expr(Expr::col((TablesCurrent::Table, Asterisk)))
                    .expr_as(
                        Expr::col((TablesCurrent::Table, TablesCurrent::FunctionParamPos)).lt(0),
                        Alias::new("system"),
                    )
                    .expr_as(
                        b.name_or_id("users.name", "tables_current.defined_by_id"),
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
                    .from(TablesCurrent::Table)
                    .join(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Expr::col((TablesCurrent::Table, TablesCurrent::CollectionId))
                            .equals((Collections::Table, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Functions::Table,
                        Expr::col((TablesCurrent::Table, TablesCurrent::FunctionVersionId))
                            .equals((Functions::Table, Functions::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((TablesCurrent::Table, TablesCurrent::DefinedById))
                            .equals((Users::Table, Users::Id)),
                    )
                    .to_owned(),
            )),
        );

        // 10. CREATE TABLE dependencies_current
        stmts.push(
            b.build_schema(
                Table::create()
                    .table(DependenciesCurrent::Table)
                    .col(
                        ColumnDef::new(DependenciesCurrent::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::CollectionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::DependencyId)
                            .text()
                            .not_null()
                            .unique_key(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::FunctionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::TableCollectionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::TableFunctionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::TableId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::TableVersions)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::DepPos)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::Status)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::DefinedOn)
                            .timestamp()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::DefinedById)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DependenciesCurrent::System)
                            .boolean()
                            .not_null(),
                    )
                    .to_owned(),
            ),
        );

        // 11. CREATE TABLE triggers_current
        stmts.push(
            b.build_schema(
                Table::create()
                    .table(TriggersCurrent::Table)
                    .col(
                        ColumnDef::new(TriggersCurrent::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::CollectionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::TriggerId)
                            .text()
                            .not_null()
                            .unique_key(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::FunctionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::TriggerByCollectionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::TriggerByFunctionId)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::TriggerByTableId)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TriggersCurrent::Status).text().not_null())
                    .col(
                        ColumnDef::new(TriggersCurrent::DefinedOn)
                            .timestamp()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TriggersCurrent::DefinedById)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TriggersCurrent::System).boolean().not_null())
                    .to_owned(),
            ),
        );

        // 12. CREATE TRIGGER for dependencies → dependencies_current
        stmts.push(b.create_trigger(&TriggerDef {
            name: "dependencies__maintain_current",
            source_table: "dependencies",
            target_table: "dependencies_current",
            partition_col: "dependency_id",
            columns: vec![
                "id",
                "collection_id",
                "dependency_id",
                "function_id",
                "table_collection_id",
                "table_function_id",
                "table_id",
                "table_versions",
                "dep_pos",
                "status",
                "defined_on",
                "defined_by_id",
                "system",
            ],
        }));

        // 13. CREATE TRIGGER for triggers → triggers_current
        stmts.push(b.create_trigger(&TriggerDef {
            name: "triggers__maintain_current",
            source_table: "triggers",
            target_table: "triggers_current",
            partition_col: "trigger_id",
            columns: vec![
                "id",
                "collection_id",
                "trigger_id",
                "function_id",
                "trigger_by_collection_id",
                "trigger_by_function_id",
                "trigger_by_table_id",
                "status",
                "defined_on",
                "defined_by_id",
                "system",
            ],
        }));

        // 14. Seed dependencies_current from existing data
        stmts.push(seed_current_table(
            &b,
            "dependencies_current",
            "dependencies",
            "dependency_id",
            "defined_on",
            &[
                "id",
                "collection_id",
                "dependency_id",
                "function_id",
                "table_collection_id",
                "table_function_id",
                "table_id",
                "table_versions",
                "dep_pos",
                "status",
                "defined_on",
                "defined_by_id",
                "system",
            ],
        ));

        // 15. Seed triggers_current from existing data
        stmts.push(seed_current_table(
            &b,
            "triggers_current",
            "triggers",
            "trigger_id",
            "defined_on",
            &[
                "id",
                "collection_id",
                "trigger_id",
                "function_id",
                "trigger_by_collection_id",
                "trigger_by_function_id",
                "trigger_by_table_id",
                "status",
                "defined_on",
                "defined_by_id",
                "system",
            ],
        ));

        // 16. CREATE VIEW dependencies_current__with_names
        stmts.push(
            b.create_view(&ViewDef::new(
                "dependencies_current__with_names",
                Query::select()
                    .expr(Expr::col((DependenciesCurrent::Table, Asterisk)))
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
                    .from(DependenciesCurrent::Table)
                    .join(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Expr::col((
                            DependenciesCurrent::Table,
                            DependenciesCurrent::CollectionId,
                        ))
                        .equals((Collections::Table, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((DependenciesCurrent::Table, DependenciesCurrent::DefinedById))
                            .equals((Users::Table, Users::Id)),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Alias::new("table_collection"),
                        Expr::col((
                            DependenciesCurrent::Table,
                            DependenciesCurrent::TableCollectionId,
                        ))
                        .equals((Alias::new("table_collection"), Collections::Id)),
                    )
                    .to_owned(),
            )),
        );

        // 17. CREATE VIEW triggers_current__with_names
        stmts.push(
            b.create_view(&ViewDef::new(
                "triggers_current__with_names",
                Query::select()
                    .expr(Expr::col((TriggersCurrent::Table, Asterisk)))
                    .expr_as(
                        Expr::col((Collections::Table, Collections::Name)),
                        Alias::new("collection"),
                    )
                    .expr_as(
                        Expr::col((Alias::new("trigger_by_collection"), Collections::Name)),
                        Alias::new("trigger_by_collection"),
                    )
                    .from(TriggersCurrent::Table)
                    .join(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Expr::col((TriggersCurrent::Table, TriggersCurrent::CollectionId))
                            .equals((Collections::Table, Collections::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((TriggersCurrent::Table, TriggersCurrent::DefinedById))
                            .equals((Users::Table, Users::Id)),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        Collections::Table,
                        Alias::new("trigger_by_collection"),
                        Expr::col((
                            TriggersCurrent::Table,
                            TriggersCurrent::TriggerByCollectionId,
                        ))
                        .equals((Alias::new("trigger_by_collection"), Collections::Id)),
                    )
                    .to_owned(),
            )),
        );

        // 18. Index on table_data_versions for the new triggered_on column
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("table_data_versions___table_id__triggered_on__id___idx")
                    .table(TableDataVersions::Table)
                    .col(TableDataVersions::TableId)
                    .col(TableDataVersions::TriggeredOn)
                    .col(TableDataVersions::Id)
                    .to_owned(),
            ),
        );

        // 19. Indices on _current tables
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("functions_current___collection_id___idx")
                    .table(FunctionsCurrent::Table)
                    .col(FunctionsCurrent::CollectionId)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("functions_current___status___idx")
                    .table(FunctionsCurrent::Table)
                    .col(FunctionsCurrent::Status)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("tables_current___collection_id___idx")
                    .table(TablesCurrent::Table)
                    .col(TablesCurrent::CollectionId)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("tables_current___function_id___idx")
                    .table(TablesCurrent::Table)
                    .col(TablesCurrent::FunctionId)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("tables_current___status___idx")
                    .table(TablesCurrent::Table)
                    .col(TablesCurrent::Status)
                    .to_owned(),
            ),
        );

        // dependencies_current indices
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("dependencies_current___function_id___idx")
                    .table(DependenciesCurrent::Table)
                    .col(DependenciesCurrent::FunctionId)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("dependencies_current___table_function_id___idx")
                    .table(DependenciesCurrent::Table)
                    .col(DependenciesCurrent::TableFunctionId)
                    .to_owned(),
            ),
        );
        // triggers_current indices
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("triggers_current___collection_id___idx")
                    .table(TriggersCurrent::Table)
                    .col(TriggersCurrent::CollectionId)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("triggers_current___function_id___idx")
                    .table(TriggersCurrent::Table)
                    .col(TriggersCurrent::FunctionId)
                    .to_owned(),
            ),
        );
        stmts.push(
            b.build_schema(
                Index::create()
                    .name("triggers_current___trigger_by_function_id___idx")
                    .table(TriggersCurrent::Table)
                    .col(TriggersCurrent::TriggerByFunctionId)
                    .to_owned(),
            ),
        );

        // 20. Mandatory version bump.
        stmts.push(b.update_db_version(Self::TO_VERSION));

        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            statements = stmts.len(),
            "Migration built"
        );
        stmts
    }
}

/// Build an `INSERT INTO {target} SELECT {cols} FROM (ranked CTE) WHERE rn = 1`
/// to seed a `_current` table from existing data.
fn seed_current_table(
    b: &Backend,
    target: &str,
    source: &str,
    partition_col: &str,
    version_col: &str,
    columns: &[&str],
) -> String {
    let col_aliases: Vec<Alias> = columns.iter().map(|c| Alias::new(*c)).collect();

    let window = WindowStatement::new()
        .partition_by(Alias::new(partition_col))
        .order_by_expr(Expr::col(Alias::new(version_col)), Order::Desc)
        .order_by_expr(Expr::col(Alias::new("id")), Order::Desc)
        .take();

    let mut ranked = Query::select();
    for col in &col_aliases {
        ranked.column(col.clone());
    }
    ranked.from(Alias::new(source)).expr_window_as(
        Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
        window,
        Alias::new("rn"),
    );
    let ranked_sql = b.build_query(ranked.take());

    let cols_csv = columns.join(", ");
    format!("INSERT INTO {target} ({cols_csv}) SELECT {cols_csv} FROM ({ranked_sql}) WHERE rn = 1")
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v6_to_v7() {
        validate_delta::<Migration>();
    }
}
