//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Func, Iden, Index, IndexCreateStatement, JoinType, Query,
    Table, TableCreateStatement,
};

use super::connections::Connections;
use super::groups::Groups;
use super::projects::{Projects, ProjectsResolved};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;
use crate::schema::sql::id_column;

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
    CollectionType,
    ConnectionId,
}

#[derive(Iden, Clone, Copy)]
pub struct CollectionsResolved;

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
                .col(id_column(Collections::Id).not_null().primary_key())
                // Tenant tag. No base default — fail-closed on every backend: a
                // write that omits it hits NOT NULL. On postgres the RLS phase
                // sets the column default to read `app.project_id` (the request's
                .col(id_column(Collections::ProjectId).not_null())
                .col(ColumnDef::new(Collections::Name).text().not_null())
                .col(ColumnDef::new(Collections::Description).text().not_null())
                .col(ColumnDef::new(Collections::CreatedOn).timestamp_with_time_zone().not_null())
                .col(id_column(Collections::CreatedById).not_null())
                .col(ColumnDef::new(Collections::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(id_column(Collections::ModifiedById).not_null())
                .col(ColumnDef::new(Collections::NameWhenDeleted).text().null())
                .col(id_column(Collections::GroupId).not_null())
                // Logical storage prefix for the collection's artifacts. Optional:
                // NULL means "inherit the owning project's base_path" (resolved at
                // stamp time via COALESCE(function, collection, project)). Mutable;
                // physical backend is selected by mounts.
                .col(ColumnDef::new(Collections::BasePath).text().null())
                // Collection flavour (DataFactory/Source/Destination), single-char code.
                .col(ColumnDef::new(Collections::CollectionType).text().not_null())
                // The collection's connection (source/destination only); set on the
                // first connection upsert.
                .col(id_column(Collections::ConnectionId).null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Collections::Table, Collections::GroupId)
                        .to(Groups::Table, Groups::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Collections::Table, Collections::ConnectionId)
                        .to(Connections::Table, Connections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // Resolved-name lookup, as on `projects`. `(project_id, name)`
            // cannot serve it — the name read is the expression, not the
            // column — so this is its own index, project-led like the rest.
            Index::create()
                .name("collections___project_id__resolved_name___idx")
                .table(Collections::Table)
                .col(Collections::ProjectId)
                .col(Expr::cust("COALESCE(\"name_when_deleted\", \"name\")"))
                .to_owned(),
            // The group-delete RESTRICT FK probe; project_id trails so the
            // tenant-scoped group filter is served too.
            Index::create()
                .name("collections___group_id__project_id___idx")
                .table(Collections::Table)
                .col(Collections::GroupId)
                .col(Collections::ProjectId)
                .to_owned(),
            // Leads with project_id so it serves the RLS predicate; unique makes
            // collection names unique within a project, not globally.
            Index::create()
                .name("collections___project_id__name___idx")
                .table(Collections::Table)
                .col(Collections::ProjectId)
                .col(Collections::Name)
                .unique()
                .to_owned(),
            // Keyset pagination: `ORDER BY id` under the tenant predicate.
            Index::create()
                .name("collections___project_id__id___idx")
                .table(Collections::Table)
                .col(Collections::ProjectId)
                .col(Collections::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![
            // Every row, including soft-deleted, with the real name resolved via
            // COALESCE(name_when_deleted, name) and a `deleted` flag. The canonical
            // join surface: the base row is always present, so a foreign-key
            // resolution never misses. `effective_base_path` joins the base
            // `projects` table (not `projects_active`) so a deleted collection
            // under a deleted project still resolves a prefix.
            ViewDef::new(
                CollectionsResolved,
                Query::select()
                    .column((Collections::Table, Collections::Id))
                    .column((Collections::Table, Collections::ProjectId))
                    .expr_as(
                        Func::coalesce([
                            Expr::col((Collections::Table, Collections::NameWhenDeleted)),
                            Expr::col((Collections::Table, Collections::Name)),
                        ]),
                        Alias::new("name"),
                    )
                    .column((Collections::Table, Collections::Description))
                    .column((Collections::Table, Collections::CreatedOn))
                    .column((Collections::Table, Collections::CreatedById))
                    .column((Collections::Table, Collections::ModifiedOn))
                    .column((Collections::Table, Collections::ModifiedById))
                    .column((Collections::Table, Collections::NameWhenDeleted))
                    .column((Collections::Table, Collections::GroupId))
                    .column((Collections::Table, Collections::BasePath))
                    .column((Collections::Table, Collections::CollectionType))
                    .column((Collections::Table, Collections::ConnectionId))
                    .expr_as(
                        Func::coalesce([
                            Expr::col((Collections::Table, Collections::BasePath)),
                            Expr::col((Projects::Table, Projects::BasePath)),
                        ]),
                        Alias::new("effective_base_path"),
                    )
                    .expr_as(Expr::col((Collections::Table, Collections::NameWhenDeleted)).is_not_null(), Alias::new("deleted"))
                    .from(Collections::Table)
                    .join(
                        JoinType::LeftJoin,
                        Projects::Table,
                        Expr::col((Collections::Table, Collections::ProjectId)).equals((Projects::Table, Projects::Id)),
                    )
                    .to_owned(),
            ),
            // Non-soft-deleted subset of the resolved view.
            ViewDef::new(
                CollectionsActive,
                Query::select()
                    .column((CollectionsResolved, Collections::Id))
                    .column((CollectionsResolved, Collections::ProjectId))
                    .column((CollectionsResolved, Collections::Name))
                    .column((CollectionsResolved, Collections::Description))
                    .column((CollectionsResolved, Collections::CreatedOn))
                    .column((CollectionsResolved, Collections::CreatedById))
                    .column((CollectionsResolved, Collections::ModifiedOn))
                    .column((CollectionsResolved, Collections::ModifiedById))
                    .column((CollectionsResolved, Collections::GroupId))
                    .column((CollectionsResolved, Collections::BasePath))
                    .column((CollectionsResolved, Collections::CollectionType))
                    .column((CollectionsResolved, Collections::ConnectionId))
                    .expr(Expr::col((CollectionsResolved, Alias::new("effective_base_path"))))
                    .from(CollectionsResolved)
                    .and_where(Expr::col((CollectionsResolved, Collections::NameWhenDeleted)).is_null())
                    .to_owned(),
            ),
            collections_with_names(),
        ]
    }
}

fn collections_with_names() -> ViewDef {
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");
    let groups_alias = Alias::new("groups_for_collection");
    ViewDef::new(
        CollectionsWithNames,
        Query::select()
            .expr(Expr::col((CollectionsActive, Asterisk)))
            .expr_as(
                name_or_id((created_by_user.clone(), Users::Name), (CollectionsActive, Collections::CreatedById)),
                Alias::new("created_by"),
            )
            .expr_as(
                name_or_id((modified_by_user.clone(), Users::Name), (CollectionsActive, Collections::ModifiedById)),
                Alias::new("modified_by"),
            )
            .expr_as(Expr::col((groups_alias.clone(), Groups::Name)), Alias::new("group"))
            .expr_as(Expr::col((ProjectsResolved, Projects::Name)), Alias::new("project"))
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
                Expr::col((CollectionsActive, Collections::GroupId)).equals((groups_alias, Groups::Id)),
            )
            // Safe as an inner join: projects_resolved holds every project
            // (soft-deleted included), so it never drops a collection row.
            .join(
                JoinType::InnerJoin,
                ProjectsResolved,
                Expr::col((CollectionsActive, Collections::ProjectId)).equals((ProjectsResolved, Projects::Id)),
            )
            .to_owned(),
    )
}
