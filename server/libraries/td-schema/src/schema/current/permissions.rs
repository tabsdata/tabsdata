//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, JoinType, Keyword, Query, Table, TableCreateStatement,
};

use super::collections::Collections;
use super::roles::Roles;
use super::users::Users;
use crate::schema::backend::{Backend, ViewDef};

#[derive(Iden)]
pub enum Permissions {
    Table,
    Id,
    RoleId,
    PermissionType,
    EntityType,
    EntityId,
    GrantedById,
    GrantedOn,
    Fixed,
}

#[derive(Iden)]
pub enum InterCollectionPermissions {
    Table,
    Id,
    FromCollectionId,
    ToCollectionId,
    GrantedOn,
    GrantedById,
}

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(Permissions::Table)
            .col(
                ColumnDef::new(Permissions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Permissions::RoleId).text().not_null())
            .col(
                ColumnDef::new(Permissions::PermissionType)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Permissions::EntityType).text().not_null())
            .col(ColumnDef::new(Permissions::EntityId).text().not_null())
            .col(ColumnDef::new(Permissions::GrantedById).text().not_null())
            .col(
                ColumnDef::new(Permissions::GrantedOn)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(ColumnDef::new(Permissions::Fixed).boolean().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(Permissions::Table, Permissions::RoleId)
                    .to(Roles::Table, Roles::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
        Table::create()
            .table(InterCollectionPermissions::Table)
            .col(
                ColumnDef::new(InterCollectionPermissions::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(InterCollectionPermissions::FromCollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(InterCollectionPermissions::ToCollectionId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(InterCollectionPermissions::GrantedOn)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(InterCollectionPermissions::GrantedById)
                    .text()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        InterCollectionPermissions::Table,
                        InterCollectionPermissions::FromCollectionId,
                    )
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        InterCollectionPermissions::Table,
                        InterCollectionPermissions::ToCollectionId,
                    )
                    .to(Collections::Table, Collections::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            )
            .to_owned(),
    ]
}

pub fn views(b: &Backend) -> Vec<ViewDef> {
    vec![
        // permissions__with_names (all unique tables).
        //
        // We list the source columns explicitly instead of `permissions.*`
        // because the view re-projects `entity_id` through a CASE expression
        // (masking the system-collection sentinel). With `permissions.*` the
        // original `entity_id` column would also appear, and Postgres rejects
        // views with duplicate column names (SQLite accepts them).
        ViewDef::new(
            "permissions__with_names",
            Query::select()
                .expr(Expr::col((Permissions::Table, Permissions::Id)))
                .expr(Expr::col((Permissions::Table, Permissions::RoleId)))
                .expr(Expr::col((Permissions::Table, Permissions::PermissionType)))
                .expr(Expr::col((Permissions::Table, Permissions::EntityType)))
                .expr(Expr::col((Permissions::Table, Permissions::GrantedById)))
                .expr(Expr::col((Permissions::Table, Permissions::GrantedOn)))
                .expr(Expr::col((Permissions::Table, Permissions::Fixed)))
                .expr_as(
                    b.name_or_id("users.name", "permissions.granted_by_id"),
                    Alias::new("granted_by"),
                )
                .expr_as(
                    Expr::case(
                        Expr::col((Permissions::Table, Permissions::EntityId))
                            .eq("00000000000000000000000204"),
                        Expr::val(Option::<String>::None),
                    )
                    .finally(Expr::col((Permissions::Table, Permissions::EntityId))),
                    Alias::new("entity_id"),
                )
                .expr_as(Expr::col((Roles::Table, Roles::Name)), Alias::new("role"))
                .expr_as(
                    Expr::col((Collections::Table, Collections::Name)),
                    Alias::new("entity"),
                )
                .from(Permissions::Table)
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Permissions::Table, Permissions::GrantedById))
                        .equals((Users::Table, Users::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Roles::Table,
                    Expr::col((Permissions::Table, Permissions::RoleId))
                        .equals((Roles::Table, Roles::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Permissions::Table, Permissions::EntityId))
                        .equals((Collections::Table, Collections::Id)),
                )
                .to_owned(),
        ),
        // inter_collection_permissions__with_names (collections joined twice → need aliases)
        {
            let from_collection = Alias::new("from_collection");
            let to_collection = Alias::new("to_collection");

            ViewDef::new(
                "inter_collection_permissions__with_names",
                Query::select()
                    .expr(Expr::col((InterCollectionPermissions::Table, Asterisk)))
                    .expr_as(
                        b.name_or_id("users.name", "inter_collection_permissions.granted_by_id"),
                        Alias::new("granted_by"),
                    )
                    .expr_as(
                        Expr::col((from_collection.clone(), Collections::Name)),
                        Alias::new("from_collection"),
                    )
                    .expr_as(
                        Expr::col((to_collection.clone(), Collections::Name)),
                        Alias::new("to_collection"),
                    )
                    .from(InterCollectionPermissions::Table)
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((
                            InterCollectionPermissions::Table,
                            InterCollectionPermissions::GrantedById,
                        ))
                        .equals((Users::Table, Users::Id)),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        Collections::Table,
                        from_collection,
                        Expr::col((
                            InterCollectionPermissions::Table,
                            InterCollectionPermissions::FromCollectionId,
                        ))
                        .equals((Alias::new("from_collection"), Collections::Id)),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        Collections::Table,
                        to_collection,
                        Expr::col((
                            InterCollectionPermissions::Table,
                            InterCollectionPermissions::ToCollectionId,
                        ))
                        .equals((Alias::new("to_collection"), Collections::Id)),
                    )
                    .to_owned(),
            )
        },
    ]
}

pub fn seed() -> Vec<InsertStatement> {
    vec![]
}

const SYSTEM_USER_ID: &str = "00000000000000000000000000";
const ALL_ENTITY_ID: &str = "00000000000000000000000204";

/// Seed data using INSERT...SELECT against the roles table.
pub fn seed_insert_select() -> Vec<InsertStatement> {
    // (permission_id, role_name, permission_type, entity_type, fixed)
    let assignments: &[(&str, &str, &str, &str, bool)] = &[
        // sys_admin gets all permissions
        ("00000000000000000000000010", "sys_admin", "sa", "s", true),
        ("00000000000000000000000014", "sys_admin", "ss", "s", false),
        ("00000000000000000000000100", "sys_admin", "ca", "c", false),
        ("00000000000000000000000104", "sys_admin", "cd", "c", false),
        ("00000000000000000000000108", "sys_admin", "cx", "c", false),
        ("0000000000000000000000010G", "sys_admin", "cr", "c", false),
        // sec_admin permissions
        ("0000000000000000000000010K", "sec_admin", "ss", "s", true),
        ("0000000000000000000000010O", "sec_admin", "ca", "c", true),
        // user permissions
        ("0000000000000000000000010S", "user", "cd", "c", false),
        ("00000000000000000000000110", "user", "cx", "c", false),
        ("00000000000000000000000200", "user", "cr", "c", false),
    ];

    assignments
        .iter()
        .map(|(id, role_name, perm_type, entity_type, fixed)| {
            Query::insert()
                .into_table(Permissions::Table)
                .columns([
                    Permissions::Id,
                    Permissions::RoleId,
                    Permissions::PermissionType,
                    Permissions::EntityType,
                    Permissions::EntityId,
                    Permissions::GrantedById,
                    Permissions::GrantedOn,
                    Permissions::Fixed,
                ])
                .select_from(
                    Query::select()
                        .expr(Expr::val(*id))
                        .expr(Expr::col((Roles::Table, Roles::Id)))
                        .expr(Expr::val(*perm_type))
                        .expr(Expr::val(*entity_type))
                        .expr(Expr::val(ALL_ENTITY_ID))
                        .expr(Expr::val(SYSTEM_USER_ID))
                        .expr(Keyword::CurrentTimestamp)
                        .expr(Expr::val(*fixed))
                        .from(Roles::Table)
                        .and_where(Expr::col((Roles::Table, Roles::Name)).eq(*role_name))
                        .to_owned(),
                )
                .expect("select_from on seed InsertStatement")
                .to_owned()
        })
        .collect()
}
