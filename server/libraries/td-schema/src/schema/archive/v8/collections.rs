//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, InsertStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::users::Users;
use crate::schema::backend::{Backend, ViewDef};

#[derive(Iden)]
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

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(Collections::Table)
            .col(
                ColumnDef::new(Collections::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Collections::Name)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(ColumnDef::new(Collections::Description).text().not_null())
            .col(
                ColumnDef::new(Collections::CreatedOn)
                    .timestamp()
                    .not_null(),
            )
            .col(ColumnDef::new(Collections::CreatedById).text().not_null())
            .col(
                ColumnDef::new(Collections::ModifiedOn)
                    .timestamp()
                    .not_null(),
            )
            .col(ColumnDef::new(Collections::ModifiedById).text().not_null())
            .col(ColumnDef::new(Collections::NameWhenDeleted).text().null())
            .to_owned(),
    ]
}

pub fn views(b: &Backend) -> Vec<ViewDef> {
    let collections_active = Alias::new("collections_active");
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");

    vec![
        // collections_active
        ViewDef::new(
            "collections_active",
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
        // collections__with_names (users joined twice → need aliases)
        ViewDef::new(
            "collections__with_names",
            Query::select()
                .expr(Expr::col((collections_active.clone(), Asterisk)))
                .expr_as(
                    b.name_or_id("created_by_user.name", "collections_active.created_by_id"),
                    Alias::new("created_by"),
                )
                .expr_as(
                    b.name_or_id("modified_by_user.name", "collections_active.modified_by_id"),
                    Alias::new("modified_by"),
                )
                .from(collections_active.clone())
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    created_by_user,
                    Expr::col((collections_active.clone(), Collections::CreatedById))
                        .equals((Alias::new("created_by_user"), Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    modified_by_user,
                    Expr::col((collections_active, Collections::ModifiedById))
                        .equals((Alias::new("modified_by_user"), Users::Id)),
                )
                .to_owned(),
        ),
    ]
}

pub fn seed() -> Vec<InsertStatement> {
    vec![]
}
