//
// Copyright 2026 Tabsdata Inc.
//

//! `groups` is the SCD2 timeline of every group: `id` is a version,
//! `group_id` the group. A delete closes the timeline; the runs and
//! collections that point at the group then label it by its id.
//!
//! Referrers read it through their `*__with_names` views, which join its last
//! version: every read is current. An as-of read needs the instant in the
//! join, which a view cannot take; when reads move to as-of, these views give
//! way to parameterized reads.

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
pub enum Groups {
    Table,
    Id,
    ProjectId,
    GroupId,
    Name,
    Description,
    System,
    Kind,
    ValidFrom,
    ValidTo,
    DefinedById,
}

/// A referrer's join to its group's open version.
#[derive(Iden, Clone, Copy)]
pub struct LastGroup;

#[derive(Iden, Clone, Copy)]
#[iden = "groups__with_names"]
pub struct GroupsWithNames;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Groups::Table)
                .col(id_column(Groups::Id).not_null().primary_key())
                // Tenant tag. No base default — fail-closed on every backend: a
                // write that omits it hits NOT NULL. On postgres the RLS phase
                // sets the column default to read `app.project_id` (the request's
                .col(id_column(Groups::ProjectId).not_null())
                .col(id_column(Groups::GroupId).not_null())
                .col(ColumnDef::new(Groups::Name).text().not_null())
                .col(ColumnDef::new(Groups::Description).text().not_null())
                .col(ColumnDef::new(Groups::System).boolean().not_null())
                .col(ColumnDef::new(Groups::Kind).text().not_null())
                .col(ColumnDef::new(Groups::ValidFrom).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Groups::ValidTo).timestamp_with_time_zone().not_null())
                .col(id_column(Groups::DefinedById).not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("groups___group_id___current_uidx")
                .table(Groups::Table)
                .col(Groups::GroupId)
                .unique()
                .and_where(open_interval(Groups::ValidTo))
                .to_owned(),
            // RLS supplies `project_id` through an OR, so the unique index's
            // leading column is never an index qual; a read by name needs its own.
            Index::create().name("groups___name___idx").table(Groups::Table).col(Groups::Name).to_owned(),
            // Live names are unique within a project, not globally.
            Index::create()
                .name("groups___project_id__name___current_uidx")
                .table(Groups::Table)
                .col(Groups::ProjectId)
                .col(Groups::Name)
                .unique()
                .and_where(open_interval(Groups::ValidTo))
                .to_owned(),
            // The latest version of a timeline, open or closed: what a referrer
            // joins for its label and the entity's fixed columns.
            Index::create()
                .name("groups___group_id__valid_from__id___idx")
                .table(Groups::Table)
                .col(Groups::GroupId)
                .col(Groups::ValidFrom)
                .col(Groups::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![groups_with_names()]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            ImmutableColumnsTriggerDef {
                name: format!("{}__immutable", Groups::Table.to_string()),
                source_table: Groups::Table.into_iden(),
                columns: cols_of_except(Groups::ValidTo),
            }
            .derived(),
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Groups::Table.to_string()),
                source_table: Groups::Table.into_iden(),
                partition_col: Groups::GroupId.into_iden(),
                valid_from_col: Groups::ValidFrom.into_iden(),
                valid_to_col: Groups::ValidTo.into_iden(),
                pk_col: Groups::Id.into_iden(),
            }
            .derived(),
        ]
    }
}

/// Every version of every group with its display names; a read of its open
/// rows is the live groups.
fn groups_with_names() -> ViewDef {
    let defined_by_user = Alias::new("defined_by_user");
    ViewDef::new(
        GroupsWithNames,
        Query::select()
            .columns(cols_of::<Groups>().into_iter().map(|c| (Groups::Table.into_iden(), c)))
            .expr_as(name_or_id((defined_by_user.clone(), Users::Name), (Groups::Table, Groups::DefinedById)), Alias::new("defined_by"))
            .from(Groups::Table)
            .join_as(
                JoinType::LeftJoin,
                Users::Table,
                defined_by_user.clone(),
                Expr::col((Groups::Table, Groups::DefinedById)).equals((defined_by_user, Users::Id)),
            )
            .to_owned(),
    )
}
