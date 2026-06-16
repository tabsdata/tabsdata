//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::groups::Groups;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Collections {
    Table,
    Id,
    ProjectId,
    Name,
    Description,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
    NameWhenDeleted,
    GroupId,
    BasePath,
}

#[derive(Iden, Clone, Copy)]
pub struct CollectionsActive;

#[derive(Iden, Clone, Copy)]
#[iden = "collections__with_names"]
pub struct CollectionsWithNames;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Collections::Table)
                .col(ColumnDef::new(Collections::Id).text().not_null().primary_key())
                // Tenant tag. No base default — fail-closed on every backend: a
                // write that omits it hits NOT NULL. On postgres the RLS phase
                // sets the column default to read `app.project_id` (the request's
                // project); the no-RLS single-project backends (sqlite/mysql) keep
                // no default at all, so writes there must set `project_id`.
                .col(ColumnDef::new(Collections::ProjectId).text().not_null())
                .col(ColumnDef::new(Collections::Name).text().not_null())
                .col(ColumnDef::new(Collections::Description).text().not_null())
                .col(ColumnDef::new(Collections::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Collections::CreatedById).text().not_null())
                .col(ColumnDef::new(Collections::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Collections::ModifiedById).text().not_null())
                .col(ColumnDef::new(Collections::NameWhenDeleted).text().null())
                .col(ColumnDef::new(Collections::GroupId).text().not_null())
                // Logical storage prefix for the collection's artifacts. Mandatory;
                // the `/` default is applied by the BasePath Rust newtype, not here.
                // Mutable; physical backend is selected by mounts.
                .col(ColumnDef::new(Collections::BasePath).text().not_null())
                // Group ids are only unique per project (same pattern as the
                // composite FKs on transactions/LTDV/LFR).
                .foreign_key(
                    ForeignKey::create()
                        .from(Collections::Table, (Collections::ProjectId, Collections::GroupId))
                        .to(Groups::Table, (Groups::ProjectId, Groups::Id))
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // Leads with project_id so it serves the RLS predicate; unique makes
            // collection names unique within a project, not globally. This is the
            // one table with a project-wide scan path (`By::<ProjectId>` in the
            // project-delete emptiness guard), so it earns a project_id-leading
            // index. The other project-scoped tables deliberately get *no*
            // standalone project_id index: they're only ever read by their own
            // keys (execution_id, transaction_id, function_id, …), where the RLS
            // `project_id = GUC` check is a low-selectivity residual the planner
            // would not use such an index for — it would be write-amplifying
            // dead weight. Revisit per-table only if profiling shows a real
            // project-scoped scan there.
            Index::create()
                .name("collections___project_id__name___idx")
                .table(Collections::Table)
                .col(Collections::ProjectId)
                .col(Collections::Name)
                .unique()
                .to_owned(),
            Index::create().name("collections___group_id___idx").table(Collections::Table).col(Collections::GroupId).to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![
            ViewDef::new(
                CollectionsActive,
                Query::select()
                    .column(Collections::Id)
                    .column(Collections::ProjectId)
                    .column(Collections::Name)
                    .column(Collections::Description)
                    .column(Collections::CreatedOn)
                    .column(Collections::CreatedById)
                    .column(Collections::ModifiedOn)
                    .column(Collections::ModifiedById)
                    .column(Collections::GroupId)
                    .column(Collections::BasePath)
                    .from(Collections::Table)
                    .and_where(Expr::col(Collections::NameWhenDeleted).is_null())
                    .to_owned(),
            ),
            collections_with_names(b),
        ]
    }
}

fn collections_with_names(b: &Backend) -> ViewDef {
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");
    let groups_alias = Alias::new("groups_for_collection");
    ViewDef::new(
        CollectionsWithNames,
        Query::select()
            .expr(Expr::col((CollectionsActive, Asterisk)))
            .expr_as(
                b.name_or_id((created_by_user.clone(), Users::Name), (CollectionsActive, Collections::CreatedById)),
                Alias::new("created_by"),
            )
            .expr_as(
                b.name_or_id((modified_by_user.clone(), Users::Name), (CollectionsActive, Collections::ModifiedById)),
                Alias::new("modified_by"),
            )
            .expr_as(Expr::col((groups_alias.clone(), Groups::Name)), Alias::new("group"))
            .from(CollectionsActive)
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                created_by_user.clone(),
                Expr::col((CollectionsActive, Collections::CreatedById)).equals((created_by_user, Users::Id)),
            )
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                modified_by_user.clone(),
                Expr::col((CollectionsActive, Collections::ModifiedById)).equals((modified_by_user, Users::Id)),
            )
            .join_as(
                JoinType::LeftJoin,
                Groups::Table,
                groups_alias.clone(),
                // Group ids are only unique per project; matching project_id too
                // keeps the join exact under RLS-bypass (global) sessions.
                Expr::col((CollectionsActive, Collections::GroupId))
                    .equals((groups_alias.clone(), Groups::Id))
                    .and(Expr::col((CollectionsActive, Collections::ProjectId)).equals((groups_alias, Groups::ProjectId))),
            )
            .to_owned(),
    )
}
