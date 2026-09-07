//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{Scd2TriggerDef, ViewDef};

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
    DefinedOn,
    DefinedById,
    Status,
    Connector,
    Connection,
    Enabled,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Functions::Table)
                .col(id_column(Functions::Id).not_null().primary_key())
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
                .col(ColumnDef::new(Functions::DefinedOn).timestamp_with_time_zone().not_null())
                .col(id_column(Functions::DefinedById).not_null())
                .col(ColumnDef::new(Functions::Status).text().not_null())
                .col(ColumnDef::new(Functions::Connector).text().null())
                .col(ColumnDef::new(Functions::Connection).text().null())
                .col(ColumnDef::new(Functions::Enabled).boolean().default(true))
                .col(ColumnDef::new(Functions::ValidTo).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Functions::Table, Functions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
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
            // The at-now twin of the pair above.
            Index::create()
                .name("functions___collection_id__name___current_idx")
                .table(Functions::Table)
                .col(Functions::CollectionId)
                .col(Functions::Name)
                .and_where(open_interval(Functions::ValidTo))
                .to_owned(),
            // The graph recursion's reference set: every open row of the
            // tenant filtered on status, with nothing else to lead on.
            Index::create()
                .name("functions___project_id__status___current_idx")
                .table(Functions::Table)
                .col(Functions::ProjectId)
                .col(Functions::Status)
                .and_where(open_interval(Functions::ValidTo))
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("functions___project_id__id___idx")
                .table(Functions::Table)
                .col(Functions::ProjectId)
                .col(Functions::Id)
                .to_owned(),
            Index::create()
                .name("functions___function_id__defined_on__id___idx")
                .table(Functions::Table)
                .col(Functions::FunctionId)
                .col(Functions::DefinedOn)
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
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                // Group snapshot from the function's collection. Used at LFR/LTDV
                // mint time to stamp `group_id` onto each minted row.
                .expr_as(Expr::col((CollectionsResolved, Collections::GroupId)), Alias::new("group_id"))
                .expr_as(name_or_id((Users::Table, Users::Name), (Functions::Table, Functions::DefinedById)), Alias::new("defined_by"))
                .from(Functions::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Functions::Table, Functions::CollectionId)).equals((CollectionsResolved, Collections::Id)),
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
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Functions::Table.to_string()),
                source_table: Functions::Table.into_iden(),
                partition_col: Functions::FunctionId.into_iden(),
                valid_from_col: Functions::DefinedOn.into_iden(),
                pk_col: Functions::Id.into_iden(),
            }
            .derived(),
        ]
    }
}
