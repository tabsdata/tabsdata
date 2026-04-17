//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, InsertStatement,
    JoinType, Keyword, Query, Table, TableCreateStatement,
};

use super::users::Users;
use crate::schema::backend::{Backend, ViewDef};

#[derive(Iden)]
pub enum Roles {
    Table,
    Id,
    Name,
    Description,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
    Fixed,
}

#[derive(Iden)]
pub enum UsersRoles {
    Table,
    Id,
    UserId,
    RoleId,
    AddedOn,
    AddedById,
    Fixed,
}

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(Roles::Table)
            .col(ColumnDef::new(Roles::Id).text().not_null().primary_key())
            .col(ColumnDef::new(Roles::Name).text().not_null().unique_key())
            .col(ColumnDef::new(Roles::Description).text().not_null())
            .col(ColumnDef::new(Roles::CreatedOn).timestamp().not_null())
            .col(ColumnDef::new(Roles::CreatedById).text().not_null())
            .col(ColumnDef::new(Roles::ModifiedOn).timestamp().not_null())
            .col(ColumnDef::new(Roles::ModifiedById).text().not_null())
            .col(ColumnDef::new(Roles::Fixed).boolean().not_null())
            .to_owned(),
        Table::create()
            .table(UsersRoles::Table)
            .col(
                ColumnDef::new(UsersRoles::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(UsersRoles::UserId).text().not_null())
            .col(ColumnDef::new(UsersRoles::RoleId).text().not_null())
            .col(ColumnDef::new(UsersRoles::AddedOn).timestamp().not_null())
            .col(ColumnDef::new(UsersRoles::AddedById).text().not_null())
            .col(ColumnDef::new(UsersRoles::Fixed).boolean().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(UsersRoles::Table, UsersRoles::RoleId)
                    .to(Roles::Table, Roles::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

pub fn views(b: &Backend) -> Vec<ViewDef> {
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");

    vec![
        // roles__with_names (users joined twice → need aliases)
        ViewDef::new(
            "roles__with_names",
            Query::select()
                .expr(Expr::col((Roles::Table, Roles::Id)))
                .expr(Expr::col((Roles::Table, Roles::Name)))
                .expr(Expr::col((Roles::Table, Roles::Description)))
                .expr(Expr::col((Roles::Table, Roles::CreatedOn)))
                .expr(Expr::col((Roles::Table, Roles::CreatedById)))
                .expr_as(
                    b.name_or_id("created_by_user.name", "roles.created_by_id"),
                    Alias::new("created_by"),
                )
                .expr(Expr::col((Roles::Table, Roles::ModifiedOn)))
                .expr(Expr::col((Roles::Table, Roles::ModifiedById)))
                .expr_as(
                    b.name_or_id("modified_by_user.name", "roles.modified_by_id"),
                    Alias::new("modified_by"),
                )
                .expr(Expr::col((Roles::Table, Roles::Fixed)))
                .from(Roles::Table)
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    created_by_user,
                    Expr::col((Roles::Table, Roles::CreatedById))
                        .equals((Alias::new("created_by_user"), Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    modified_by_user,
                    Expr::col((Roles::Table, Roles::ModifiedById))
                        .equals((Alias::new("modified_by_user"), Users::Id)),
                )
                .to_owned(),
        ),
        // users_roles__with_names (users joined twice → alias the added_by join)
        {
            let added_by_user = Alias::new("added_by_user");

            ViewDef::new(
                "users_roles__with_names",
                Query::select()
                    .expr(Expr::col((UsersRoles::Table, UsersRoles::Id)))
                    .expr(Expr::col((UsersRoles::Table, UsersRoles::UserId)))
                    .expr_as(
                        b.name_or_id("users.name", "users_roles.user_id"),
                        Alias::new("user"),
                    )
                    .expr(Expr::col((UsersRoles::Table, UsersRoles::RoleId)))
                    .expr_as(
                        b.name_or_id("roles.name", "users_roles.role_id"),
                        Alias::new("role"),
                    )
                    .expr(Expr::col((UsersRoles::Table, UsersRoles::AddedOn)))
                    .expr(Expr::col((UsersRoles::Table, UsersRoles::AddedById)))
                    .expr_as(
                        b.name_or_id("added_by_user.name", "users_roles.added_by_id"),
                        Alias::new("added_by"),
                    )
                    .expr(Expr::col((UsersRoles::Table, UsersRoles::Fixed)))
                    .from(UsersRoles::Table)
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((UsersRoles::Table, UsersRoles::UserId))
                            .equals((Users::Table, Users::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Roles::Table,
                        Expr::col((UsersRoles::Table, UsersRoles::RoleId))
                            .equals((Roles::Table, Roles::Id)),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        Users::Table,
                        added_by_user,
                        Expr::col((UsersRoles::Table, UsersRoles::AddedById))
                            .equals((Alias::new("added_by_user"), Users::Id)),
                    )
                    .to_owned(),
            )
        },
    ]
}

const SYSTEM_USER_ID: &str = "00000000000000000000000000";
const ADMIN_USER_NAME: &str = "admin";

pub fn seed() -> Vec<InsertStatement> {
    let role_inserts = [
        (
            "00000000000000000000000008",
            "sys_admin",
            "System Administrator Role",
        ),
        (
            "0000000000000000000000000C",
            "sec_admin",
            "Security Administrator Role",
        ),
        ("0000000000000000000000000G", "user", "User Role"),
    ];

    role_inserts
        .iter()
        .map(|(id, name, desc)| {
            Query::insert()
                .into_table(Roles::Table)
                .columns([
                    Roles::Id,
                    Roles::Name,
                    Roles::Description,
                    Roles::CreatedOn,
                    Roles::CreatedById,
                    Roles::ModifiedOn,
                    Roles::ModifiedById,
                    Roles::Fixed,
                ])
                .values_panic([
                    (*id).into(),
                    (*name).into(),
                    (*desc).into(),
                    Keyword::CurrentTimestamp.into(),
                    SYSTEM_USER_ID.into(),
                    Keyword::CurrentTimestamp.into(),
                    SYSTEM_USER_ID.into(),
                    true.into(),
                ])
                .to_owned()
        })
        .collect()
}

/// Seed data using INSERT...SELECT to join against users and roles tables.
pub fn seed_insert_select() -> Vec<InsertStatement> {
    let assignments = [
        ("0000000000000000000000000K", ADMIN_USER_NAME, "sys_admin"),
        ("0000000000000000000000000O", ADMIN_USER_NAME, "sec_admin"),
        ("0000000000000000000000000S", ADMIN_USER_NAME, "user"),
    ];

    assignments
        .iter()
        .map(|(id, user_name, role_name)| {
            Query::insert()
                .into_table(UsersRoles::Table)
                .columns([
                    UsersRoles::Id,
                    UsersRoles::UserId,
                    UsersRoles::RoleId,
                    UsersRoles::AddedOn,
                    UsersRoles::AddedById,
                    UsersRoles::Fixed,
                ])
                .select_from(
                    Query::select()
                        .expr(Expr::val(*id))
                        .expr(Expr::col((Users::Table, Users::Id)))
                        .expr(Expr::col((Roles::Table, Roles::Id)))
                        .expr(Keyword::CurrentTimestamp)
                        .expr(Expr::val(SYSTEM_USER_ID))
                        .expr(Expr::val(true))
                        .from(Users::Table)
                        .from(Roles::Table)
                        .and_where(Expr::col((Users::Table, Users::Name)).eq(*user_name))
                        .and_where(Expr::col((Roles::Table, Roles::Name)).eq(*role_name))
                        .to_owned(),
                )
                .expect("select_from on seed InsertStatement")
                .to_owned()
        })
        .collect()
}
