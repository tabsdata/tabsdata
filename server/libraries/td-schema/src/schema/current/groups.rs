//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{Alias, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "groups__with_names"]
pub struct GroupsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Groups {
    Table,
    Id,
    ProjectId,
    Name,
    Description,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
    System,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Groups::Table)
                // Composite PK (project_id, id): system groups share the same
                // well-known id in every project. Joins on groups.id from
                // project-scoped tables must also match project_id.
                .col(ColumnDef::new(Groups::Id).text().not_null())
                // Tenant tag. No base default — fail-closed on every backend: a
                // write that omits it hits NOT NULL. On postgres the RLS phase
                // sets the column default to read `app.project_id` (the request's
                // project); the no-RLS backends (sqlite/mysql) keep
                // no default at all, so writes there must set `project_id`.
                .col(ColumnDef::new(Groups::ProjectId).text().not_null())
                .primary_key(Index::create().col(Groups::ProjectId).col(Groups::Id))
                .col(ColumnDef::new(Groups::Name).text().not_null())
                .col(ColumnDef::new(Groups::Description).text().not_null())
                .col(ColumnDef::new(Groups::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Groups::CreatedById).text().not_null())
                .col(ColumnDef::new(Groups::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Groups::ModifiedById).text().not_null())
                .col(ColumnDef::new(Groups::System).boolean().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        // Leads with project_id so it serves the RLS predicate; unique makes
        // group names unique within a project, not globally.
        vec![
            Index::create()
                .name("groups___project_id__name___idx")
                .table(Groups::Table)
                .col(Groups::ProjectId)
                .col(Groups::Name)
                .unique()
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        let created_by_user = Alias::new("created_by_user");
        let modified_by_user = Alias::new("modified_by_user");
        vec![ViewDef::new(
            GroupsWithNames,
            Query::select()
                .expr(Expr::col((Groups::Table, Groups::Id)))
                .expr(Expr::col((Groups::Table, Groups::ProjectId)))
                .expr(Expr::col((Groups::Table, Groups::Name)))
                .expr(Expr::col((Groups::Table, Groups::Description)))
                .expr(Expr::col((Groups::Table, Groups::CreatedOn)))
                .expr(Expr::col((Groups::Table, Groups::CreatedById)))
                .expr_as(
                    b.name_or_id((created_by_user.clone(), Users::Name), (Groups::Table, Groups::CreatedById)),
                    Alias::new("created_by"),
                )
                .expr(Expr::col((Groups::Table, Groups::ModifiedOn)))
                .expr(Expr::col((Groups::Table, Groups::ModifiedById)))
                .expr_as(
                    b.name_or_id((modified_by_user.clone(), Users::Name), (Groups::Table, Groups::ModifiedById)),
                    Alias::new("modified_by"),
                )
                .expr(Expr::col((Groups::Table, Groups::System)))
                .from(Groups::Table)
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    created_by_user.clone(),
                    Expr::col((Groups::Table, Groups::CreatedById)).equals((created_by_user, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Users::Table,
                    modified_by_user.clone(),
                    Expr::col((Groups::Table, Groups::ModifiedById)).equals((modified_by_user, Users::Id)),
                )
                .to_owned(),
        )]
    }
}
