//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, InsertStatement, JoinType, Keyword, Query, Table,
    TableCreateStatement,
};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "roles__with_names"]
pub struct RolesWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
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

const SYSTEM_USER_ID: &str = "00000000000000000000000000";

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Roles::Table)
                .col(ColumnDef::new(Roles::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Roles::Name).text().not_null().unique_key())
                .col(ColumnDef::new(Roles::Description).text().not_null())
                .col(ColumnDef::new(Roles::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Roles::CreatedById).text().not_null())
                .col(ColumnDef::new(Roles::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Roles::ModifiedById).text().not_null())
                .col(ColumnDef::new(Roles::Fixed).boolean().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![Index::create().name("roles___name___idx").table(Roles::Table).col(Roles::Name).unique().to_owned()]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        let created_by_user = Alias::new("created_by_user");
        let modified_by_user = Alias::new("modified_by_user");
        vec![ViewDef::new(
            RolesWithNames,
            Query::select()
                .expr(Expr::col((Roles::Table, Roles::Id)))
                .expr(Expr::col((Roles::Table, Roles::Name)))
                .expr(Expr::col((Roles::Table, Roles::Description)))
                .expr(Expr::col((Roles::Table, Roles::CreatedOn)))
                .expr(Expr::col((Roles::Table, Roles::CreatedById)))
                .expr_as(
                    b.name_or_id((created_by_user.clone(), Users::Name), (Roles::Table, Roles::CreatedById)),
                    Alias::new("created_by"),
                )
                .expr(Expr::col((Roles::Table, Roles::ModifiedOn)))
                .expr(Expr::col((Roles::Table, Roles::ModifiedById)))
                .expr_as(
                    b.name_or_id((modified_by_user.clone(), Users::Name), (Roles::Table, Roles::ModifiedById)),
                    Alias::new("modified_by"),
                )
                .expr(Expr::col((Roles::Table, Roles::Fixed)))
                .from(Roles::Table)
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    created_by_user.clone(),
                    Expr::col((Roles::Table, Roles::CreatedById)).equals((created_by_user, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    modified_by_user.clone(),
                    Expr::col((Roles::Table, Roles::ModifiedById)).equals((modified_by_user, Users::Id)),
                )
                .to_owned(),
        )]
    }

    fn seeds(&self) -> Vec<InsertStatement> {
        let role_inserts = [
            ("00000000000000000000000008", "sys_admin", "System Administrator Role"),
            ("0000000000000000000000000C", "sec_admin", "Security Administrator Role"),
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
}
