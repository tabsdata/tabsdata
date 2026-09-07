//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, InsertStatement, JoinType, Keyword,
    Query, Table, TableCreateStatement,
};

use super::roles::Roles;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "users_roles__with_names"]
pub struct UsersRolesWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum UsersRoles {
    Table,
    Id,
    UserId,
    RoleId,
    AddedOn,
    AddedById,
    Fixed,
}

use td_security::ADMIN_USER as ADMIN_USER_NAME;
use td_security::ENCODED_ID_SYSTEM as SYSTEM_USER_ID;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(UsersRoles::Table)
                .col(ColumnDef::new(UsersRoles::Id).text().not_null().primary_key())
                .col(ColumnDef::new(UsersRoles::UserId).text().not_null())
                .col(ColumnDef::new(UsersRoles::RoleId).text().not_null())
                .col(ColumnDef::new(UsersRoles::AddedOn).timestamp_with_time_zone().not_null())
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

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // `role_id` is only a suffix of `(user_id, role_id)`: the
            // membership list and the role/project delete cascades filter on it
            // and order by id.
            Index::create()
                .name("users_roles___role_id__id___idx")
                .table(UsersRoles::Table)
                .col(UsersRoles::RoleId)
                .col(UsersRoles::Id)
                .to_owned(),
            Index::create()
                .name("roles___user_id__role_id___idx")
                .table(UsersRoles::Table)
                .col(UsersRoles::UserId)
                .col(UsersRoles::RoleId)
                .unique()
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        let added_by_user = Alias::new("added_by_user");
        vec![ViewDef::new(
            UsersRolesWithNames,
            Query::select()
                .expr(Expr::col((UsersRoles::Table, UsersRoles::Id)))
                .expr(Expr::col((UsersRoles::Table, UsersRoles::UserId)))
                .expr_as(name_or_id((Users::Table, Users::Name), (UsersRoles::Table, UsersRoles::UserId)), Alias::new("user"))
                .expr(Expr::col((UsersRoles::Table, UsersRoles::RoleId)))
                .expr_as(name_or_id((Roles::Table, Roles::Name), (UsersRoles::Table, UsersRoles::RoleId)), Alias::new("role"))
                .expr(Expr::col((UsersRoles::Table, UsersRoles::AddedOn)))
                .expr(Expr::col((UsersRoles::Table, UsersRoles::AddedById)))
                .expr_as(
                    name_or_id((added_by_user.clone(), Users::Name), (UsersRoles::Table, UsersRoles::AddedById)),
                    Alias::new("added_by"),
                )
                .expr(Expr::col((UsersRoles::Table, UsersRoles::Fixed)))
                .from(UsersRoles::Table)
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((UsersRoles::Table, UsersRoles::UserId)).equals((Users::Table, Users::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Roles::Table,
                    Expr::col((UsersRoles::Table, UsersRoles::RoleId)).equals((Roles::Table, Roles::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    added_by_user.clone(),
                    Expr::col((UsersRoles::Table, UsersRoles::AddedById)).equals((added_by_user, Users::Id)),
                )
                .to_owned(),
        )]
    }

    fn seeds(&self) -> Vec<InsertStatement> {
        // The admin user is a member of BOTH built-in system roles: sys_admin
        // (all system + manage-all-projects) and default (auto-assigned to every
        // user). Built-in system roles are additive, not mutually exclusive.
        let assignments = [
            ("0000000000000000000000000k", ADMIN_USER_NAME, "sys_admin"),
            ("0000000000000000000000000o", ADMIN_USER_NAME, "default"),
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
}
