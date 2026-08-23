//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::{Collections, CollectionsResolved};
use super::functions_current::FunctionsCurrent;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::{SeedCurrent, TriggerDef, TriggerSpec, ViewDef, cols_of};

/// `FunctionStatus::Deleted` discriminant, hoisted as a `&str` because
/// td-schema sits below td-objects in the dep graph.
pub(super) const FUNCTION_STATUS_DELETED: &str = "D";

#[derive(Iden, Clone, Copy)]
#[iden = "functions__with_names"]
pub struct FunctionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Functions {
    Table,
    Id,
    ProjectId,
    CollectionId,
    Name,
    Description,
    Decorator,
    RuntimeValues,
    FunctionId,
    BasePath,
    StageId,
    StageBasePath,
    StorageVersion,
    BundleId,
    Snippet,
    DefinedOn,
    DefinedById,
    Status,
    Connector,
    Connection,
    Enabled,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Functions::Table)
                .col(ColumnDef::new(Functions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Functions::ProjectId).text().not_null())
                .col(ColumnDef::new(Functions::CollectionId).text().not_null())
                .col(ColumnDef::new(Functions::Name).text().not_null())
                .col(ColumnDef::new(Functions::Description).text().not_null())
                .col(ColumnDef::new(Functions::Decorator).text().not_null())
                .col(ColumnDef::new(Functions::RuntimeValues).text().not_null())
                .col(ColumnDef::new(Functions::FunctionId).text().not_null())
                // Optional storage-prefix override for this function version's output
                // table data. NULL → inherit the collection base_path at mint time.
                .col(ColumnDef::new(Functions::BasePath).text().null())
                // Stage/checkpoint identity and storage prefix. Stamped at register
                // (prefix = collection base_path then), carried forward across updates
                // so a function's accumulating state stays put when the collection
                // base_path later changes; stage_id is regenerated on --reset-stage.
                .col(ColumnDef::new(Functions::StageId).text().not_null())
                .col(ColumnDef::new(Functions::StageBasePath).text().not_null())
                .col(ColumnDef::new(Functions::StorageVersion).text().not_null())
                .col(ColumnDef::new(Functions::BundleId).text().not_null())
                .col(ColumnDef::new(Functions::Snippet).text().not_null())
                .col(ColumnDef::new(Functions::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Functions::DefinedById).text().not_null())
                .col(ColumnDef::new(Functions::Status).text().not_null())
                .col(ColumnDef::new(Functions::Connector).text().null())
                .col(ColumnDef::new(Functions::Connection).text().null())
                .col(ColumnDef::new(Functions::Enabled).boolean().default(true))
                .foreign_key(
                    ForeignKey::create()
                        .from(Functions::Table, Functions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create().name("functions___collection_id___idx").table(Functions::Table).col(Functions::CollectionId).to_owned(),
            // Composite index for versioned queries:
            // PARTITION BY function_id ORDER BY defined_on DESC, id DESC
            Index::create()
                .name("functions___function_id__defined_on__id___idx")
                .table(Functions::Table)
                .col(Functions::FunctionId)
                .col(Functions::DefinedOn)
                .col(Functions::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![ViewDef::new(
            FunctionsWithNames,
            Query::select()
                .expr(Expr::col((Functions::Table, Asterisk)))
                .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((CollectionsResolved, Alias::new("deleted"))), Alias::new("collection_deleted"))
                // Group snapshot from the function's collection. Used at LFR/LTDV
                // mint time to stamp `group_id` onto each minted row.
                .expr_as(Expr::col((CollectionsResolved, Collections::GroupId)), Alias::new("group_id"))
                .expr_as(b.name_or_id((Users::Table, Users::Name), (Functions::Table, Functions::DefinedById)), Alias::new("defined_by"))
                .from(Functions::Table)
                .join(
                    JoinType::LeftJoin,
                    CollectionsResolved,
                    Expr::col((Functions::Table, Functions::CollectionId)).equals((CollectionsResolved, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Functions::Table, Functions::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![TriggerSpec::MaintainCurrent(TriggerDef {
            name: format!("{}__maintain_current", Functions::Table.to_string()),
            source_table: Functions::Table.into_iden(),
            target_table: FunctionsCurrent::Table.into_iden(),
            partition_col: Functions::FunctionId.into_iden(),
            columns: cols_of::<Functions>(),
        })]
    }

    fn seed_current(&self) -> Vec<SeedCurrent> {
        vec![SeedCurrent {
            target: FunctionsCurrent::Table.into_iden(),
            source: Functions::Table.into_iden(),
            partition_col: Functions::FunctionId.into_iden(),
            version_col: Functions::DefinedOn.into_iden(),
            columns: cols_of::<Functions>(),
        }]
    }
}
