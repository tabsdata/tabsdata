//
// Copyright 2026 Tabs Data Inc.
//

//! Project registry.
//!
//! A project is the tenant boundary for PostgreSQL row-level security: every
//! tenant-scoped table carries a `project_id` and an RLS policy filters it
//! against the `app.project_id` session variable set by the connection layer.
//!
//! This table is the authoritative list of projects. It is *not* referenced by
//! a foreign key from the project-scoped tables: those carry a bare `project_id`
//! tag set from the request GUC, and the apiserver's resolver is what guarantees
//! the GUC only ever holds an id that exists here (it 404s an unknown
//! `{project}` before any scoped table is touched). The active project for a
//! request thus comes from the request, validated against this table by the
//! resolver — not from a DB-level FK. Deletes are soft, so a tagged row's
//! project id keeps resolving here even after the project is deleted.

use sea_query::{Alias, ColumnDef, Expr, ExprTrait, Iden, JoinType, Query, Table, TableCreateStatement};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Projects {
    Table,
    Id,
    Name,
    Description,
    BasePath,
    CreatedOn,
    CreatedById,
    ModifiedOn,
    ModifiedById,
    NameWhenDeleted,
}

#[derive(Iden, Clone, Copy)]
pub struct ProjectsActive;

#[derive(Iden, Clone, Copy)]
#[iden = "projects__with_names"]
pub struct ProjectsWithNames;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Projects::Table)
                .col(ColumnDef::new(Projects::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Projects::Name).text().not_null().unique_key())
                .col(ColumnDef::new(Projects::Description).text().not_null())
                // Logical storage prefix for the project's artifacts and the
                // default inherited by its collections (a collection with a NULL
                // base_path resolves to this). Mandatory; the `/` default is
                // applied by the BasePath Rust newtype, not here. Create-only for
                // now (project "move" deferred); physical backend selected by mounts.
                .col(ColumnDef::new(Projects::BasePath).text().not_null())
                .col(ColumnDef::new(Projects::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Projects::CreatedById).text().not_null())
                .col(ColumnDef::new(Projects::ModifiedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Projects::ModifiedById).text().not_null())
                .col(ColumnDef::new(Projects::NameWhenDeleted).text().null())
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![
            // Active projects: non-soft-deleted rows. `ProjectDB` reads from
            // this view; soft-deletes set `name_when_deleted` and rename the
            // row out of the active set.
            ViewDef::new(
                ProjectsActive,
                Query::select()
                    .column(Projects::Id)
                    .column(Projects::Name)
                    .column(Projects::Description)
                    .column(Projects::BasePath)
                    .column(Projects::CreatedOn)
                    .column(Projects::CreatedById)
                    .column(Projects::ModifiedOn)
                    .column(Projects::ModifiedById)
                    .from(Projects::Table)
                    .and_where(Expr::col(Projects::NameWhenDeleted).is_null())
                    .to_owned(),
            ),
            projects_with_names(b),
        ]
    }
}

/// View that joins active-projects rows to `users` for `created_by` /
/// `modified_by` display names, falling back to the encoded user id when the
/// user row is gone (`name_or_id`).
fn projects_with_names(b: &Backend) -> ViewDef {
    let created_by_user = Alias::new("created_by_user");
    let modified_by_user = Alias::new("modified_by_user");
    ViewDef::new(
        ProjectsWithNames,
        Query::select()
            .expr(Expr::col((ProjectsActive, Projects::Id)))
            .expr(Expr::col((ProjectsActive, Projects::Name)))
            .expr(Expr::col((ProjectsActive, Projects::Description)))
            .expr(Expr::col((ProjectsActive, Projects::BasePath)))
            .expr(Expr::col((ProjectsActive, Projects::CreatedOn)))
            .expr(Expr::col((ProjectsActive, Projects::CreatedById)))
            .expr_as(
                b.name_or_id((created_by_user.clone(), Users::Name), (ProjectsActive, Projects::CreatedById)),
                Alias::new("created_by"),
            )
            .expr(Expr::col((ProjectsActive, Projects::ModifiedOn)))
            .expr(Expr::col((ProjectsActive, Projects::ModifiedById)))
            .expr_as(
                b.name_or_id((modified_by_user.clone(), Users::Name), (ProjectsActive, Projects::ModifiedById)),
                Alias::new("modified_by"),
            )
            .from(ProjectsActive)
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                created_by_user.clone(),
                Expr::col((ProjectsActive, Projects::CreatedById)).equals((created_by_user, Users::Id)),
            )
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                modified_by_user.clone(),
                Expr::col((ProjectsActive, Projects::ModifiedById)).equals((modified_by_user, Users::Id)),
            )
            .to_owned(),
    )
}
