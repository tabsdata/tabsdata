//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, InsertStatement, JoinType, Keyword,
    Query, Table, TableCreateStatement,
};

use super::projects::{Projects, ProjectsResolved};
use super::roles::Roles;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "permissions__with_names"]
pub struct PermissionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
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

use td_security::ENCODED_ID_ALL_ENTITIES as ALL_ENTITY_ID;
use td_security::ENCODED_ID_SYSTEM as SYSTEM_USER_ID;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Permissions::Table)
                .col(ColumnDef::new(Permissions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Permissions::RoleId).text().not_null())
                .col(ColumnDef::new(Permissions::PermissionType).text().not_null())
                .col(ColumnDef::new(Permissions::EntityType).text().not_null())
                .col(ColumnDef::new(Permissions::EntityId).text().not_null())
                .col(ColumnDef::new(Permissions::GrantedById).text().not_null())
                .col(ColumnDef::new(Permissions::GrantedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Permissions::Fixed).boolean().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Permissions::Table, Permissions::RoleId)
                        .to(Roles::Table, Roles::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("permissions___role_id__permission_type__entity___idx")
                .table(Permissions::Table)
                .col(Permissions::RoleId)
                .col(Permissions::PermissionType)
                .col(Permissions::EntityType)
                .col(Permissions::EntityId)
                .unique()
                .to_owned(),
            // The Simple facade's `list --project` filters rows by entity_id
            // (the composite index above leads with role_id, so it can't serve
            // an entity_id-only lookup).
            Index::create().name("permissions___entity_id___idx").table(Permissions::Table).col(Permissions::EntityId).to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![
            // permissions__with_names (all unique tables).
            //
            // We list the source columns explicitly instead of `permissions.*`
            // because the view re-projects `entity_id` through a CASE expression
            // (masking the system-collection sentinel). With `permissions.*` the
            // original `entity_id` column would also appear, and Postgres rejects
            ViewDef::new(
                PermissionsWithNames,
                Query::select()
                    .expr(Expr::col((Permissions::Table, Permissions::Id)))
                    .expr(Expr::col((Permissions::Table, Permissions::RoleId)))
                    .expr(Expr::col((Permissions::Table, Permissions::PermissionType)))
                    .expr(Expr::col((Permissions::Table, Permissions::EntityType)))
                    .expr(Expr::col((Permissions::Table, Permissions::GrantedById)))
                    .expr(Expr::col((Permissions::Table, Permissions::GrantedOn)))
                    .expr(Expr::col((Permissions::Table, Permissions::Fixed)))
                    .expr_as(
                        name_or_id((Users::Table, Users::Name), (Permissions::Table, Permissions::GrantedById)),
                        Alias::new("granted_by"),
                    )
                    .expr_as(
                        Expr::case(
                            Expr::col((Permissions::Table, Permissions::EntityId)).eq("00000000000000000000000204"),
                            Expr::val(Option::<String>::None),
                        )
                        .finally(Expr::col((Permissions::Table, Permissions::EntityId))),
                        Alias::new("entity_id"),
                    )
                    .expr_as(Expr::col((Roles::Table, Roles::Name)), Alias::new("role"))
                    .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("entity"))
                    .from(Permissions::Table)
                    .join(
                        JoinType::LeftJoin,
                        Users::Table,
                        Expr::col((Permissions::Table, Permissions::GrantedById)).equals((Users::Table, Users::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        Roles::Table,
                        Expr::col((Permissions::Table, Permissions::RoleId)).equals((Roles::Table, Roles::Id)),
                    )
                    .join(
                        JoinType::LeftJoin,
                        ProjectsResolved,
                        Expr::col((Permissions::Table, Permissions::EntityId)).equals((ProjectsResolved, Projects::Id)),
                    )
                    .to_owned(),
            ),
        ]
    }

    fn seeds(&self) -> Vec<InsertStatement> {
        // (permission_id, role_name, permission_type, entity_type, fixed)
        //
        // entity_id is ALL_ENTITY_ID (`__all__` = 00000000000000000000000204) for
        // every row: system rows are disambiguated by entity_type='system'; the
        // project wildcard `(project:__all__, manage_project)` reuses the same
        // sentinel.
        let assignments: &[(&str, &str, &str, &str, bool)] = &[
            // sys_admin: all three system permissions + manage on every project.
            ("00000000000000000000000010", "sys_admin", "manage_access", "system", true),
            ("00000000000000000000000014", "sys_admin", "create_projects", "system", true),
            ("00000000000000000000000100", "sys_admin", "manage_roles", "system", true),
            ("00000000000000000000000104", "sys_admin", "manage_project", "project", true),
            // default role: every user can create projects.
            ("00000000000000000000000108", "default", "create_projects", "system", false),
            // project_creator role: confers exactly create_projects on system (system-managed).
            ("0000000000000000000000020s", "project_creator", "create_projects", "system", true),
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
}
