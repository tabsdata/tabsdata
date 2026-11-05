//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, Iden, Index, IndexCreateStatement, IntoIden, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::{Collections, LastCollection};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::scd2::{closed_version, label_of, last_version_of, open_interval};
use crate::schema::sql::{ImmutableColumnsTriggerDef, Scd2TriggerDef, ViewDef, cols_of_except};
use crate::schema::sql::{id_column, logical_id_column};

#[derive(Iden, Clone, Copy)]
#[iden = "functions__with_names"]
pub struct FunctionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Functions {
    Table,
    Id,
    ProjectId,
    CollectionId,
    Name,
    Description,
    Decorator,
    RuntimeValues,
    FunctionId,
    BasePath,
    StageId,
    StageBasePath,
    StorageVersion,
    BundleId,
    Snippet,
    ValidFrom,
    DefinedById,
    Status,
    Connector,
    Connection,
    Enabled,
    ValidTo,
    FunctionType,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Functions::Table)
                .col(logical_id_column(Functions::Id).not_null().primary_key())
                .col(id_column(Functions::ProjectId).not_null())
                .col(id_column(Functions::CollectionId).not_null())
                .col(ColumnDef::new(Functions::Name).text().not_null())
                .col(ColumnDef::new(Functions::Description).text().not_null())
                .col(ColumnDef::new(Functions::Decorator).text().not_null())
                .col(ColumnDef::new(Functions::RuntimeValues).text().not_null())
                .col(id_column(Functions::FunctionId).not_null())
                // Optional storage-prefix override for this function version's output
                // table data. NULL → inherit the collection base_path at mint time.
                .col(ColumnDef::new(Functions::BasePath).text().null())
                // Stage/checkpoint identity and storage prefix. Stamped at register
                // (prefix = collection base_path then), carried forward across updates
                // so a function's accumulating state stays put when the collection
                // base_path later changes; stage_id is regenerated on --reset-stage.
                .col(id_column(Functions::StageId).not_null())
                .col(ColumnDef::new(Functions::StageBasePath).text().not_null())
                .col(ColumnDef::new(Functions::StorageVersion).text().not_null())
                .col(id_column(Functions::BundleId).not_null())
                .col(ColumnDef::new(Functions::Snippet).text().not_null())
                .col(ColumnDef::new(Functions::ValidFrom).timestamp_with_time_zone().not_null())
                .col(id_column(Functions::DefinedById).not_null())
                .col(ColumnDef::new(Functions::Status).text().not_null())
                .col(ColumnDef::new(Functions::Connector).text().null())
                .col(ColumnDef::new(Functions::Connection).text().null())
                .col(ColumnDef::new(Functions::Enabled).boolean().default(true))
                .col(ColumnDef::new(Functions::ValidTo).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Functions::FunctionType).text().not_null())
                // The target of the project-scoped foreign keys, and keyset
                // pagination: `ORDER BY id` under the tenant predicate.
                .index(Index::create().name("functions___project_id__id___key").unique().col(Functions::ProjectId).col(Functions::Id))
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // `(collection, name)` resolution at a point in time on every
            // function verb; without `name` it is a heap residual over every
            // historical version in the collection.
            Index::create()
                .name("functions___collection_id__name___idx")
                .table(Functions::Table)
                .col(Functions::CollectionId)
                .col(Functions::Name)
                .to_owned(),
            // The at-now twin of the pair above; unique, so a collection's
            // live function names are too.
            Index::create()
                .name("functions___collection_id__name___current_uidx")
                .table(Functions::Table)
                .col(Functions::CollectionId)
                .col(Functions::Name)
                .and_where(open_interval(Functions::ValidTo))
                .unique()
                .to_owned(),
            // As-of reads of the project's topology: the rows still open at the
            // instant, without walking its closed history.
            Index::create()
                .name("functions___project_id__valid_to___idx")
                .table(Functions::Table)
                .col(Functions::ProjectId)
                .col(Functions::ValidTo)
                .to_owned(),
            Index::create()
                .name("functions___function_id__valid_from__id___idx")
                .table(Functions::Table)
                .col(Functions::FunctionId)
                .col(Functions::ValidFrom)
                .col(Functions::Id)
                .to_owned(),
            // One open interval per partition — the SCD2 invariant, and the
            // at-now lookup path.
            Index::create()
                .name("functions___function_id___current_uidx")
                .table(Functions::Table)
                .col(Functions::FunctionId)
                .unique()
                .and_where(open_interval(Functions::ValidTo))
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            FunctionsWithNames,
            Query::select()
                .expr(Expr::col((Functions::Table, Asterisk)))
                .expr_as(label_of(LastCollection, Collections::Name, (Functions::Table, Functions::CollectionId)), Alias::new("collection"))
                .expr_as(closed_version(LastCollection), Alias::new("collection_deleted"))
                .expr_as(Expr::col((LastCollection, Collections::GroupId)), Alias::new("group_id"))
                .expr_as(name_or_id((Users::Table, Users::Name), (Functions::Table, Functions::DefinedById)), Alias::new("defined_by"))
                .from(Functions::Table)
                .join_as(
                    JoinType::LeftJoin,
                    Collections::Table,
                    LastCollection,
                    last_version_of(
                        Collections::Table,
                        LastCollection,
                        Collections::CollectionId,
                        (Functions::Table, Functions::CollectionId),
                    ),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Functions::Table, Functions::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            ImmutableColumnsTriggerDef {
                name: format!("{}__immutable", Functions::Table.to_string()),
                source_table: Functions::Table.into_iden(),
                columns: cols_of_except(Functions::ValidTo),
            }
            .derived(),
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Functions::Table.to_string()),
                source_table: Functions::Table.into_iden(),
                partition_col: Functions::FunctionId.into_iden(),
                valid_from_col: Functions::ValidFrom.into_iden(),
                valid_to_col: Functions::ValidTo.into_iden(),
                pk_col: Functions::Id.into_iden(),
            }
            .derived(),
        ]
    }
}
