//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, IntoIden, JoinType,
    Query, Table, TableCreateStatement,
};

use super::collections::Collections;
use super::functions::Functions;
use super::tables_current::TablesCurrent;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::{SeedCurrent, TriggerDef, TriggerSpec, ViewDef, cols_of};

#[derive(Iden, Clone, Copy)]
#[iden = "tables__with_names"]
pub struct TablesWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Tables {
    Table,
    ProjectId,
    Id,
    CollectionId,
    TableId,
    Name,
    FunctionId,
    FunctionVersionId,
    FunctionParamPos,
    Private,
    Partitioned,
    DefinedOn,
    DefinedById,
    Status,
    TableType,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Tables::Table)
                .col(ColumnDef::new(Tables::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Tables::CollectionId).text().not_null())
                .col(ColumnDef::new(Tables::TableId).text().not_null())
                .col(ColumnDef::new(Tables::Name).text().not_null())
                .col(ColumnDef::new(Tables::FunctionId).text().null())
                .col(ColumnDef::new(Tables::FunctionVersionId).text().not_null())
                .col(ColumnDef::new(Tables::FunctionParamPos).integer().null())
                .col(ColumnDef::new(Tables::Private).boolean().not_null())
                .col(ColumnDef::new(Tables::Partitioned).boolean().null())
                .col(ColumnDef::new(Tables::DefinedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Tables::DefinedById).text().not_null())
                .col(ColumnDef::new(Tables::Status).text().not_null())
                .col(ColumnDef::new(Tables::TableType).text().not_null().default("R"))
                .foreign_key(
                    ForeignKey::create()
                        .from(Tables::Table, Tables::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Tables::Table, Tables::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .col(ColumnDef::new(Tables::ProjectId).text().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("tables___function_id__status___idx")
                .table(Tables::Table)
                .col(Tables::FunctionId)
                .col(Tables::Status)
                .to_owned(),
            Index::create().name("tables___status___idx").table(Tables::Table).col(Tables::Status).to_owned(),
            // Composite index for versioned queries:
            // PARTITION BY table_id ORDER BY defined_on DESC, id DESC
            Index::create()
                .name("tables___table_id__defined_on__id___idx")
                .table(Tables::Table)
                .col(Tables::TableId)
                .col(Tables::DefinedOn)
                .col(Tables::Id)
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![ViewDef::new(
            TablesWithNames,
            Query::select()
                .expr(Expr::col((Tables::Table, Asterisk)))
                .expr_as(Expr::col((Tables::Table, Tables::FunctionParamPos)).lt(0), Alias::new("system"))
                .expr_as(b.name_or_id((Users::Table, Users::Name), (Tables::Table, Tables::DefinedById)), Alias::new("defined_by"))
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .from(Tables::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Tables::Table, Tables::CollectionId)).equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Tables::Table, Tables::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                .join(JoinType::LeftJoin, Users::Table, Expr::col((Tables::Table, Tables::DefinedById)).equals((Users::Table, Users::Id)))
                .to_owned(),
        )]
    }

    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![TriggerSpec::MaintainCurrent(TriggerDef {
            name: format!("{}__maintain_current", Tables::Table.to_string()),
            source_table: Tables::Table.into_iden(),
            target_table: TablesCurrent::Table.into_iden(),
            partition_col: Tables::TableId.into_iden(),
            columns: cols_of::<Tables>(),
        })]
    }

    fn seed_current(&self) -> Vec<SeedCurrent> {
        vec![SeedCurrent {
            target: TablesCurrent::Table.into_iden(),
            source: Tables::Table.into_iden(),
            partition_col: Tables::TableId.into_iden(),
            version_col: Tables::DefinedOn.into_iden(),
            columns: cols_of::<Tables>(),
        }]
    }
}
