//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::functions::Functions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{Scd2TriggerDef, ViewDef};

#[derive(Iden, Clone, Copy)]
#[iden = "tables__with_names"]
pub struct TablesWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Tables {
    Table,
    Id,
    ProjectId,
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
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Tables::Table)
                .col(id_column(Tables::Id).not_null().primary_key())
                .col(id_column(Tables::ProjectId).not_null())
                .col(id_column(Tables::CollectionId).not_null())
                .col(id_column(Tables::TableId).not_null())
                .col(ColumnDef::new(Tables::Name).text().not_null())
                .col(id_column(Tables::FunctionId).null())
                .col(id_column(Tables::FunctionVersionId).not_null())
                .col(ColumnDef::new(Tables::FunctionParamPos).integer().null())
                .col(ColumnDef::new(Tables::Private).boolean().not_null())
                .col(ColumnDef::new(Tables::Partitioned).boolean().null())
                .col(ColumnDef::new(Tables::DefinedOn).timestamp_with_time_zone().not_null())
                .col(id_column(Tables::DefinedById).not_null())
                .col(ColumnDef::new(Tables::Status).text().not_null())
                .col(ColumnDef::new(Tables::TableType).text().not_null().default("R"))
                .col(ColumnDef::new(Tables::ValidTo).timestamp_with_time_zone().not_null())
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
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // The at-now twin of `(collection_id, name)`, which is
            // non-partial and so carries every historical version.
            Index::create()
                .name("tables___collection_id__name___current_idx")
                .table(Tables::Table)
                .col(Tables::CollectionId)
                .col(Tables::Name)
                .and_where(open_interval(Tables::ValidTo))
                .to_owned(),
            // The graph recursion's reference set: every open row of the
            // tenant filtered on status, with nothing else to lead on.
            Index::create()
                .name("tables___project_id__status___current_idx")
                .table(Tables::Table)
                .col(Tables::ProjectId)
                .col(Tables::Status)
                .and_where(open_interval(Tables::ValidTo))
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create().name("tables___project_id__id___idx").table(Tables::Table).col(Tables::ProjectId).col(Tables::Id).to_owned(),
            // At-time `(collection, name)` resolution on every table read and register;
            // an interval predicate cannot use the open-row partial index.
            Index::create()
                .name("tables___collection_id__name___idx")
                .table(Tables::Table)
                .col(Tables::CollectionId)
                .col(Tables::Name)
                .to_owned(),
            // Enable re-registers every table of a function version.
            Index::create().name("tables___function_version_id___idx").table(Tables::Table).col(Tables::FunctionVersionId).to_owned(),
            Index::create()
                .name("tables___function_id__status___idx")
                .table(Tables::Table)
                .col(Tables::FunctionId)
                .col(Tables::Status)
                .to_owned(),
            Index::create()
                .name("tables___table_id__defined_on__id___idx")
                .table(Tables::Table)
                .col(Tables::TableId)
                .col(Tables::DefinedOn)
                .col(Tables::Id)
                .to_owned(),
            Index::create()
                .name("tables___table_id___current_uidx")
                .table(Tables::Table)
                .col(Tables::TableId)
                .unique()
                .and_where(open_interval(Tables::ValidTo))
                .to_owned(),
            Index::create()
                .name("tables___function_id___current_idx")
                .table(Tables::Table)
                .col(Tables::FunctionId)
                .and_where(open_interval(Tables::ValidTo))
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            TablesWithNames,
            Query::select()
                .expr(Expr::col((Tables::Table, Asterisk)))
                .expr_as(Expr::col((Tables::Table, Tables::FunctionParamPos)).lt(0), Alias::new("system"))
                .expr_as(name_or_id((Users::Table, Users::Name), (Tables::Table, Tables::DefinedById)), Alias::new("defined_by"))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .from(Tables::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Tables::Table, Tables::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Tables::Table, Tables::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                .join(JoinType::LeftJoin, Users::Table, Expr::col((Tables::Table, Tables::DefinedById)).equals((Users::Table, Users::Id)))
                .to_owned(),
        )]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Tables::Table.to_string()),
                source_table: Tables::Table.into_iden(),
                partition_col: Tables::TableId.into_iden(),
                valid_from_col: Tables::DefinedOn.into_iden(),
                pk_col: Tables::Id.into_iden(),
            }
            .derived(),
        ]
    }
}
