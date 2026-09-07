//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IntoIden, JoinType, Query, Table, TableCreateStatement,
};

use super::functions::Functions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::source::Derived;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{ImmutableColumnsTriggerDef, Scd2TriggerDef, ViewDef, cols_of_except};

/// `function_patches` — code-only overrides of a parent
/// `FunctionVersionId`. Lives in its own table so every "find latest
/// function version" query against `functions` is patch-blind by
/// construction.
#[derive(Iden, Clone, Copy)]
#[iden = "function_patches__with_names"]
pub struct FunctionPatchesWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum FunctionPatches {
    Table,
    Id,
    ProjectId,
    ParentFunctionVersionId,
    FunctionId,
    CollectionId,
    Snippet,
    BundleId,
    RuntimeValues,
    Description,
    ValidFrom,
    DefinedById,
    Status,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(FunctionPatches::Table)
                .col(id_column(FunctionPatches::Id).not_null().primary_key())
                .col(id_column(FunctionPatches::ProjectId).not_null())
                .col(id_column(FunctionPatches::ParentFunctionVersionId).not_null())
                .col(id_column(FunctionPatches::FunctionId).not_null())
                .col(id_column(FunctionPatches::CollectionId).not_null())
                .col(ColumnDef::new(FunctionPatches::Snippet).text().not_null())
                .col(id_column(FunctionPatches::BundleId).not_null())
                .col(ColumnDef::new(FunctionPatches::RuntimeValues).text().not_null())
                .col(ColumnDef::new(FunctionPatches::Description).text().not_null())
                .col(ColumnDef::new(FunctionPatches::ValidFrom).timestamp_with_time_zone().not_null())
                .col(id_column(FunctionPatches::DefinedById).not_null())
                .col(ColumnDef::new(FunctionPatches::Status).text().not_null())
                .col(ColumnDef::new(FunctionPatches::ValidTo).timestamp_with_time_zone().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(FunctionPatches::Table, FunctionPatches::ParentFunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // List-patches scans by `(function_id, valid_from DESC)`.
            Index::create()
                .name("function_patches___function_id__valid_from___idx")
                .table(FunctionPatches::Table)
                .col(FunctionPatches::FunctionId)
                .col(FunctionPatches::ValidFrom)
                .to_owned(),
            // Mint (plan build / replay) reads at-time within
            // `parent_function_version_id` partitions.
            Index::create()
                .name("function_patches___parent_fv_id__valid_from__id___idx")
                .table(FunctionPatches::Table)
                .col(FunctionPatches::ParentFunctionVersionId)
                .col(FunctionPatches::ValidFrom)
                .col(FunctionPatches::Id)
                .to_owned(),
            Index::create()
                .name("function_patches___parent_function_version_id___current_uidx")
                .table(FunctionPatches::Table)
                .col(FunctionPatches::ParentFunctionVersionId)
                .unique()
                .and_where(open_interval(FunctionPatches::ValidTo))
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            FunctionPatchesWithNames,
            Query::select()
                .expr(Expr::col((FunctionPatches::Table, Asterisk)))
                .expr_as(
                    name_or_id((Users::Table, Users::Name), (FunctionPatches::Table, FunctionPatches::DefinedById)),
                    Alias::new("defined_by"),
                )
                .from(FunctionPatches::Table)
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((FunctionPatches::Table, FunctionPatches::DefinedById)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", FunctionPatches::Table.to_string()),
                source_table: FunctionPatches::Table.into_iden(),
                partition_col: FunctionPatches::ParentFunctionVersionId.into_iden(),
                valid_from_col: FunctionPatches::ValidFrom.into_iden(),
                valid_to_col: FunctionPatches::ValidTo.into_iden(),
                pk_col: FunctionPatches::Id.into_iden(),
            }
            .derived(),
            // Patches are an event log of code amendments — never updated or deleted.
            ImmutableColumnsTriggerDef {
                name: format!("{}__immutable", FunctionPatches::Table.to_string()),
                source_table: FunctionPatches::Table.into_iden(),
                columns: cols_of_except(FunctionPatches::ValidTo),
            }
            .derived(),
        ]
    }
}
