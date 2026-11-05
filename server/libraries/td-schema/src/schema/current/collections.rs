//
// Copyright 2026 Tabsdata Inc.
//

//! `collections` is the SCD2 timeline of every collection: `id` is a version,
//! `collection_id` the collection every function, table and run points at.
//! Its project, group, type and connection are fixed for the collection's
//! life; a delete closes the timeline.
//!
//! Referrers read it through their `*__with_names` views, which join its last
//! version: every read is current. An as-of read needs the instant in the
//! join, which a view cannot take; when reads move to as-of, these views give
//! way to parameterized reads.

use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Func, FunctionCall, Iden, Index,
    IndexCreateStatement, IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::connections::Connections;
use super::groups::{Groups, LastGroup};
use super::projects::{LastProject, Projects};
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::scd2::{label_of, last_version_of, open_interval};
use crate::schema::sql::{ImmutableColumnsTriggerDef, Scd2TriggerDef, ViewDef, cols_of, cols_of_except, id_column};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Collections {
    Table,
    Id,
    ProjectId,
    CollectionId,
    Name,
    Description,
    BasePath,
    GroupId,
    CollectionType,
    ConnectionId,
    ValidFrom,
    ValidTo,
    DefinedById,
}

/// A referrer's join to its collection's open version.
#[derive(Iden, Clone, Copy)]
pub struct LastCollection;

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
                .col(id_column(Collections::CollectionId).not_null())
                .col(ColumnDef::new(Collections::Name).text().not_null())
                .col(ColumnDef::new(Collections::Description).text().not_null())
                // Logical storage prefix for the collection's artifacts. Optional:
                // NULL means "inherit the owning project's base_path" (resolved at
                // stamp time via COALESCE(function, collection, project));
                // physical backend is selected by mounts.
                .col(ColumnDef::new(Collections::BasePath).text().null())
                .col(id_column(Collections::GroupId).not_null())
                // Collection flavour (DataFactory/Source/Destination), single-char code.
                .col(ColumnDef::new(Collections::CollectionType).text().not_null())
                // The collection's connection (source/destination only), minted
                // with the collection.
                .col(id_column(Collections::ConnectionId).null())
                .col(ColumnDef::new(Collections::ValidFrom).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Collections::ValidTo).timestamp_with_time_zone().not_null())
                .col(id_column(Collections::DefinedById).not_null())
                .foreign_key(
                    ForeignKey::create()
                        .name("collections___connection_id___fkey")
                        .from(Collections::Table, (Collections::ProjectId, Collections::ConnectionId))
                        .to(Connections::Table, (Connections::ProjectId, Connections::Id))
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("collections___collection_id___current_uidx")
                .table(Collections::Table)
                .col(Collections::CollectionId)
                .unique()
                .and_where(open_interval(Collections::ValidTo))
                .to_owned(),
            // Leads with project_id so it serves the RLS predicate; unique makes
            // live collection names unique within a project, not globally.
            Index::create()
                .name("collections___project_id__name___current_uidx")
                .table(Collections::Table)
                .col(Collections::ProjectId)
                .col(Collections::Name)
                .unique()
                .and_where(open_interval(Collections::ValidTo))
                .to_owned(),
            // The latest version of a timeline, open or closed: what a referrer
            // joins for its label and the entity's fixed columns.
            Index::create()
                .name("collections___collection_id__valid_from__id___idx")
                .table(Collections::Table)
                .col(Collections::CollectionId)
                .col(Collections::ValidFrom)
                .col(Collections::Id)
                .to_owned(),
            // The group-delete probe for a live collection in the group;
            // project_id trails so the tenant-scoped group filter is served too.
            Index::create()
                .name("collections___group_id__project_id___idx")
                .table(Collections::Table)
                .col(Collections::GroupId)
                .col(Collections::ProjectId)
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
        vec![collections_with_names()]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            ImmutableColumnsTriggerDef {
                name: format!("{}__immutable", Collections::Table.to_string()),
                source_table: Collections::Table.into_iden(),
                columns: cols_of_except(Collections::ValidTo),
            }
            .derived(),
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Collections::Table.to_string()),
                source_table: Collections::Table.into_iden(),
                partition_col: Collections::CollectionId.into_iden(),
                valid_from_col: Collections::ValidFrom.into_iden(),
                valid_to_col: Collections::ValidTo.into_iden(),
                pk_col: Collections::Id.into_iden(),
            }
            .derived(),
        ]
    }
}

/// Every version of every collection with its resolved prefix and labels; a
/// read of its open rows is the live collections.
fn collections_with_names() -> ViewDef {
    let effective_base_path: FunctionCall = Func::coalesce([
        Expr::col((Collections::Table, Collections::BasePath)),
        Expr::col((LastProject, Projects::BasePath)),
    ]);
    ViewDef::new(
        CollectionsWithNames,
        Query::select()
            .columns(cols_of::<Collections>().into_iter().map(|c| (Collections::Table.into_iden(), c)))
            .expr_as(effective_base_path, Alias::new("effective_base_path"))
            .expr_as(label_of(LastProject, Projects::Name, (Collections::Table, Collections::ProjectId)), Alias::new("project"))
            .expr_as(name_or_id((Users::Table, Users::Name), (Collections::Table, Collections::DefinedById)), Alias::new("defined_by"))
            .expr_as(label_of(LastGroup, Groups::Name, (Collections::Table, Collections::GroupId)), Alias::new("group"))
            .from(Collections::Table)
            .join_as(
                JoinType::LeftJoin,
                Projects::Table,
                LastProject,
                last_version_of(Projects::Table, LastProject, Projects::ProjectId, (Collections::Table, Collections::ProjectId)),
            )
            .join_as(
                JoinType::LeftJoin,
                Groups::Table,
                LastGroup,
                last_version_of(Groups::Table, LastGroup, Groups::GroupId, (Collections::Table, Collections::GroupId)),
            )
            .join(
                JoinType::LeftJoin,
                Users::Table,
                Expr::col((Collections::Table, Collections::DefinedById)).equals((Users::Table, Users::Id)),
            )
            .to_owned(),
    )
}
