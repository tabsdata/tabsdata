//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement,
};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Collections {
    Table,
    Id,
    Name,
    Description,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
    NameWhenDeleted,
}

#[derive(Iden, Clone, Copy)]
#[iden = "collections_active"]
pub struct CollectionsActive;

#[derive(Iden, Clone, Copy)]
#[iden = "collections__with_names"]
pub struct CollectionsWithNames;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Collections::Table)
                .col(ColumnDef::new(Collections::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Collections::Name).text().not_null().unique_key())
                .col(ColumnDef::new(Collections::Description).text().not_null())
                .col(ColumnDef::new(Collections::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Collections::CreatedById).text().not_null())
                .col(ColumnDef::new(Collections::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Collections::ModifiedById).text().not_null())
                .col(ColumnDef::new(Collections::NameWhenDeleted).text().null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![Index::create().name("collections___name___idx").table(Collections::Table).col(Collections::Name).to_owned()]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![
            ViewDef::new(
                CollectionsActive,
                Query::select()
                    .column(Collections::Id)
                    .column(Collections::Name)
                    .column(Collections::Description)
                    .column(Collections::CreatedOn)
                    .column(Collections::CreatedById)
                    .column(Collections::ModifiedOn)
                    .column(Collections::ModifiedById)
                    .from(Collections::Table)
                    .and_where(Expr::col(Collections::NameWhenDeleted).is_null())
                    .to_owned(),
            ),
            collections_with_names(b),
        ]
    }
}

fn collections_with_names(b: &Backend) -> ViewDef {
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");
    ViewDef::new(
        CollectionsWithNames,
        Query::select()
            .expr(Expr::col((CollectionsActive, Asterisk)))
            .expr_as(
                b.name_or_id((created_by_user.clone(), Users::Name), (CollectionsActive, Collections::CreatedById)),
                Alias::new("created_by"),
            )
            .expr_as(
                b.name_or_id((modified_by_user.clone(), Users::Name), (CollectionsActive, Collections::ModifiedById)),
                Alias::new("modified_by"),
            )
            .from(CollectionsActive)
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                created_by_user.clone(),
                Expr::col((CollectionsActive, Collections::CreatedById)).equals((created_by_user, Users::Id)),
            )
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                modified_by_user.clone(),
                Expr::col((CollectionsActive, Collections::ModifiedById)).equals((modified_by_user, Users::Id)),
            )
            .to_owned(),
    )
}
