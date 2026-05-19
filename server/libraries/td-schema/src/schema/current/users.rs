//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, InsertStatement, JoinType, Keyword, Query,
    Table, TableCreateStatement,
};

use crate::schema::backend::{Backend, ViewDef};

#[derive(Iden)]
pub enum Users {
    Table,
    Id,
    Name,
    FullName,
    Email,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
    PasswordHash,
    PasswordSetOn,
    PasswordMustChange,
    Enabled,
}

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(Users::Table)
            .col(ColumnDef::new(Users::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Users::Name).text().not_null().unique_key())
            .col(ColumnDef::new(Users::FullName).text().not_null())
            .col(ColumnDef::new(Users::Email).text().unique_key().null())
            .col(ColumnDef::new(Users::CreatedOn).timestamp_with_time_zone().not_null())
            .col(ColumnDef::new(Users::CreatedById).text().not_null())
            .col(ColumnDef::new(Users::ModifiedOn).timestamp_with_time_zone().not_null())
            .col(ColumnDef::new(Users::ModifiedById).text().not_null())
            .col(ColumnDef::new(Users::PasswordHash).text().not_null())
            .col(ColumnDef::new(Users::PasswordSetOn).timestamp_with_time_zone().not_null())
            .col(
                ColumnDef::new(Users::PasswordMustChange)
                    .boolean()
                    .not_null(),
            )
            .col(ColumnDef::new(Users::Enabled).boolean().not_null())
            .to_owned(),
    ]
}

pub fn views(b: &Backend) -> Vec<ViewDef> {
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");

    vec![ViewDef::new(
        "users__with_names",
        Query::select()
            .expr(Expr::col((Users::Table, Asterisk)))
            .expr_as(
                b.name_or_id("created_by_user.name", "users.created_by_id"),
                Alias::new("created_by"),
            )
            .expr_as(
                b.name_or_id("modified_by_user.name", "users.modified_by_id"),
                Alias::new("modified_by"),
            )
            .from(Users::Table)
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                created_by_user,
                Expr::col((Users::Table, Users::CreatedById))
                    .equals((Alias::new("created_by_user"), Users::Id)),
            )
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                modified_by_user,
                Expr::col((Users::Table, Users::ModifiedById))
                    .equals((Alias::new("modified_by_user"), Users::Id)),
            )
            .to_owned(),
    )]
}

pub fn seed() -> Vec<InsertStatement> {
    vec![
        Query::insert()
            .into_table(Users::Table)
            .columns([
                Users::Id,
                Users::Name,
                Users::FullName,
                Users::Email,
                Users::CreatedOn,
                Users::CreatedById,
                Users::ModifiedOn,
                Users::ModifiedById,
                Users::PasswordHash,
                Users::PasswordSetOn,
                Users::PasswordMustChange,
                Users::Enabled,
            ])
            .values_panic([
                "00000000000000000000000004".into(),
                "admin".into(),
                "Administrator".into(),
                "-".into(),
                Keyword::CurrentTimestamp.into(),
                "00000000000000000000000000".into(),
                Keyword::CurrentTimestamp.into(),
                "00000000000000000000000000".into(),
                "$argon2id$v=19$m=19456,t=2,p=1$ULTV//eju00aMbzZXAORyg$4+7GgsoT3e7lHElgWA2v0OYQHr5SBQzYY3pcUy0/YDM".into(),
                Keyword::CurrentTimestamp.into(),
                false.into(),
                true.into(),
            ])
            .to_owned(),
    ]
}
