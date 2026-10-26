//
// Copyright 2026 Tabsdata Inc.
//

//! Project registry.
//!
//! A project is the tenant boundary for PostgreSQL row-level security: every
//! tenant-scoped table carries a `project_id` and an RLS policy filters it
//! against the `app.project_id` session variable set by the connection layer.
//!
//! `projects` is the SCD2 timeline of every project: `id` is a version,
//! `project_id` the project. A delete closes the timeline; a tagged row of a
//! deleted project then labels it by its id.
//!
//! Referrers read it through their `*__with_names` views, which join its last
//! version: every read is current. An as-of read needs the instant in the
//! join, which a view cannot take; when reads move to as-of, these views give
//! way to parameterized reads.
//!
//! No project-scoped table references it by a foreign key: those carry a bare
//! `project_id` tag set from the request GUC, and the apiserver's resolver is
//! what guarantees the GUC only ever holds an id with an open version here (it
//! 404s an unknown `{project}` before any scoped table is touched). It is
//! global, not under RLS: the resolver reads it before any project is entered.

use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ExprTrait, Iden, Index, IndexCreateStatement, IntoIden, JoinType, Query, Table,
    TableCreateStatement,
};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{ImmutableColumnsTriggerDef, Scd2TriggerDef, ViewDef, cols_of, cols_of_except, id_column};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Projects {
    Table,
    Id,
    ProjectId,
    Name,
    Description,
    BasePath,
    ValidFrom,
    ValidTo,
    DefinedById,
}

/// A referrer's join to its project's open version.
#[derive(Iden, Clone, Copy)]
pub struct LastProject;

#[derive(Iden, Clone, Copy)]
#[iden = "projects__with_names"]
pub struct ProjectsWithNames;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Projects::Table)
                .col(id_column(Projects::Id).not_null().primary_key())
                .col(id_column(Projects::ProjectId).not_null())
                .col(ColumnDef::new(Projects::Name).text().not_null())
                .col(ColumnDef::new(Projects::Description).text().not_null())
                // Logical storage prefix for the project's artifacts and the
                // default inherited by its collections (a collection with a NULL
                // base_path resolves to this). Mandatory; the `/` default is
                // applied by the BasePath Rust newtype, not here. Fixed for the
                // project's life (project "move" deferred); physical backend
                // selected by mounts.
                .col(ColumnDef::new(Projects::BasePath).text().not_null())
                .col(ColumnDef::new(Projects::ValidFrom).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Projects::ValidTo).timestamp_with_time_zone().not_null())
                .col(id_column(Projects::DefinedById).not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("projects___project_id___current_uidx")
                .table(Projects::Table)
                .col(Projects::ProjectId)
                .unique()
                .and_where(open_interval(Projects::ValidTo))
                .to_owned(),
            // Live names are unique; the project resolver reads by it on every
            // request.
            Index::create()
                .name("projects___name___current_uidx")
                .table(Projects::Table)
                .col(Projects::Name)
                .unique()
                .and_where(open_interval(Projects::ValidTo))
                .to_owned(),
            // The latest version of a timeline, open or closed: what a referrer
            // joins for its label and the entity's fixed columns.
            Index::create()
                .name("projects___project_id__valid_from__id___idx")
                .table(Projects::Table)
                .col(Projects::ProjectId)
                .col(Projects::ValidFrom)
                .col(Projects::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![projects_with_names()]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            ImmutableColumnsTriggerDef {
                name: format!("{}__immutable", Projects::Table.to_string()),
                source_table: Projects::Table.into_iden(),
                columns: cols_of_except(Projects::ValidTo),
            }
            .derived(),
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Projects::Table.to_string()),
                source_table: Projects::Table.into_iden(),
                partition_col: Projects::ProjectId.into_iden(),
                valid_from_col: Projects::ValidFrom.into_iden(),
                valid_to_col: Projects::ValidTo.into_iden(),
                pk_col: Projects::Id.into_iden(),
            }
            .derived(),
        ]
    }
}

/// Every version of every project with its display names; a read of its open
/// rows is the live projects.
fn projects_with_names() -> ViewDef {
    let defined_by_user = Alias::new("defined_by_user");
    ViewDef::new(
        ProjectsWithNames,
        Query::select()
            .columns(cols_of::<Projects>().into_iter().map(|c| (Projects::Table.into_iden(), c)))
            .expr_as(name_or_id((defined_by_user.clone(), Users::Name), (Projects::Table, Projects::DefinedById)), Alias::new("defined_by"))
            .from(Projects::Table)
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                defined_by_user.clone(),
                Expr::col((Projects::Table, Projects::DefinedById)).equals((defined_by_user, Users::Id)),
            )
            .to_owned(),
    )
}
