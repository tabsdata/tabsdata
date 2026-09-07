//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, InsertStatement, JoinType, Keyword, Query, Table,
    TableCreateStatement,
};

use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "users__with_names"]
pub struct UsersWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
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
    System,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
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
                .col(ColumnDef::new(Users::PasswordMustChange).boolean().not_null())
                .col(ColumnDef::new(Users::Enabled).boolean().not_null())
                .col(ColumnDef::new(Users::System).boolean().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create().name("users___enabled___idx").table(Users::Table).col(Users::Enabled).to_owned(),
            Index::create().name("users___system___idx").table(Users::Table).col(Users::System).to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        let created_by_user = Alias::new("created_by_user");
        let modified_by_user = Alias::new("modified_by_user");
        vec![ViewDef::new(
            UsersWithNames,
            Query::select()
                .expr(Expr::col((Users::Table, Asterisk)))
                .expr_as(name_or_id((created_by_user.clone(), Users::Name), (Users::Table, Users::CreatedById)), Alias::new("created_by"))
                .expr_as(
                    name_or_id((modified_by_user.clone(), Users::Name), (Users::Table, Users::ModifiedById)),
                    Alias::new("modified_by"),
                )
                .from(Users::Table)
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    created_by_user.clone(),
                    Expr::col((Users::Table, Users::CreatedById)).equals((created_by_user, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    modified_by_user.clone(),
                    Expr::col((Users::Table, Users::ModifiedById)).equals((modified_by_user, Users::Id)),
                )
                .to_owned(),
        )]
    }

    fn seeds(&self) -> Vec<InsertStatement> {
        let user_columns = [
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
            Users::System,
        ];
        vec![
            Query::insert()
                .into_table(Users::Table)
                .columns(user_columns)
                .values_panic([
                    "00000000000000000000000000".into(),
                    "system".into(),
                    "System".into(),
                    Keyword::Null.into(),
                    Keyword::CurrentTimestamp.into(),
                    "00000000000000000000000000".into(),
                    Keyword::CurrentTimestamp.into(),
                    "00000000000000000000000000".into(),
                    "!".into(),
                    Keyword::CurrentTimestamp.into(),
                    false.into(),
                    false.into(),
                    true.into(),
                ])
                .to_owned(),
            Query::insert()
                .into_table(Users::Table)
                .columns(user_columns)
                .values_panic([
                    "00000000000000000000000004".into(),
                    "admin".into(),
                    "Administrator".into(),
                    sea_query::Value::String(None).into(),
                    Keyword::CurrentTimestamp.into(),
                    "00000000000000000000000000".into(),
                    Keyword::CurrentTimestamp.into(),
                    "00000000000000000000000000".into(),
                    "$argon2id$v=19$m=19456,t=2,p=1$ULTV//eju00aMbzZXAORyg$4+7GgsoT3e7lHElgWA2v0OYQHr5SBQzYY3pcUy0/YDM".into(),
                    Keyword::CurrentTimestamp.into(),
                    false.into(),
                    true.into(),
                    false.into(),
                ])
                .to_owned(),
        ]
    }
}
