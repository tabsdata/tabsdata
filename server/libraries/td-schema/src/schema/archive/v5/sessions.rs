//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, InsertStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::roles::Roles;
use super::users::Users;
use crate::schema::backend::{Backend, ViewDef};

#[derive(Iden)]
pub enum Sessions {
    Table,
    AccessTokenId,
    RefreshTokenId,
    UserId,
    RoleId,
    CreatedOn,
    ExpiresOn,
    StatusChangeOn,
    Status,
}

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(Sessions::Table)
            .col(
                ColumnDef::new(Sessions::AccessTokenId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Sessions::RefreshTokenId)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(ColumnDef::new(Sessions::UserId).text().not_null())
            .col(ColumnDef::new(Sessions::RoleId).text().not_null())
            .col(ColumnDef::new(Sessions::CreatedOn).timestamp().not_null())
            .col(ColumnDef::new(Sessions::ExpiresOn).timestamp().not_null())
            .col(
                ColumnDef::new(Sessions::StatusChangeOn)
                    .timestamp()
                    .not_null(),
            )
            .col(ColumnDef::new(Sessions::Status).text().not_null())
            .to_owned(),
    ]
}

pub fn views(_b: &Backend) -> Vec<ViewDef> {
    vec![ViewDef::new(
        "sessions__with_names",
        Query::select()
            .expr(Expr::col((Sessions::Table, Asterisk)))
            .expr_as(
                Expr::col((Users::Table, Users::Name)),
                Alias::new("user_name"),
            )
            .expr_as(
                Expr::col((Roles::Table, Roles::Name)),
                Alias::new("role_name"),
            )
            .from(Sessions::Table)
            .join(
                JoinType::Join,
                Users::Table,
                Expr::col((Sessions::Table, Sessions::UserId)).equals((Users::Table, Users::Id)),
            )
            .join(
                JoinType::Join,
                Roles::Table,
                Expr::col((Sessions::Table, Sessions::RoleId)).equals((Roles::Table, Roles::Id)),
            )
            .to_owned(),
    )]
}

pub fn seed() -> Vec<InsertStatement> {
    vec![]
}
