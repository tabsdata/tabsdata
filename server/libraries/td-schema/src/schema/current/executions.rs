//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::Collections;
use super::functions::Functions;
use super::projects::Projects;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

/// `executions__with_names` view name. Re-used cross-file by
/// `with_status_views` to build `executions__with_status`.
#[derive(Iden, Clone, Copy)]
#[iden = "executions__with_names"]
pub struct ExecutionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Executions {
    Table,
    Id,
    ProjectId,
    Name,
    CollectionId,
    FunctionVersionId,
    TriggeredOn,
    TriggeredById,
    RecursionOptions,
    FunctionId,
    TriggerType,
    ExternalUid,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Executions::Table)
                .col(ColumnDef::new(Executions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(Executions::ProjectId).text().not_null())
                .col(ColumnDef::new(Executions::Name).text().null())
                .col(ColumnDef::new(Executions::CollectionId).text().not_null())
                .col(ColumnDef::new(Executions::FunctionVersionId).text().not_null())
                .col(ColumnDef::new(Executions::TriggeredOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Executions::TriggeredById).text().not_null())
                // `Option<Json<RecursionOptions>>` in the DAO; sqlx encodes
                // `Json<T>` as JSONB in Postgres, so the column must be JSONB
                // (MySQL JSON, SQLite TEXT) — not plain TEXT, which PG rejects at
                // decode time with a "mismatched types" error.
                .col(ColumnDef::new(Executions::RecursionOptions).json_binary().null())
                .col(ColumnDef::new(Executions::FunctionId).text().not_null().default(""))
                .col(ColumnDef::new(Executions::TriggerType).text().not_null().default("M"))
                .col(ColumnDef::new(Executions::ExternalUid).text().null())
                .foreign_key(
                    ForeignKey::create()
                        .from(Executions::Table, Executions::CollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(Executions::Table, Executions::FunctionVersionId)
                        .to(Functions::Table, Functions::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create().name("executions___collection_id___idx").table(Executions::Table).col(Executions::CollectionId).to_owned(),
            Index::create()
                .name("executions___function_version_id___idx")
                .table(Executions::Table)
                .col(Executions::FunctionVersionId)
                .to_owned(),
            Index::create().name("executions___triggered_by_id___idx").table(Executions::Table).col(Executions::TriggeredById).to_owned(),
            Index::create().name("executions___triggered_on___idx").table(Executions::Table).col(Executions::TriggeredOn).to_owned(),
            // Dedup key: a non-NULL `external_uid` is unique per function.
            // NULLs are distinct on all backends, so untagged triggers are
            // never deduplicated.
            Index::create()
                .name("executions___function_id__external_uid___idx")
                .table(Executions::Table)
                .col(Executions::FunctionId)
                .col(Executions::ExternalUid)
                .unique()
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        vec![ViewDef::new(
            ExecutionsWithNames,
            Query::select()
                .expr(Expr::col((Executions::Table, Asterisk)))
                .expr_as(Expr::col((Collections::Table, Collections::Name)), Alias::new("collection"))
                .expr_as(Expr::col((Functions::Table, Functions::Name)), Alias::new("function"))
                .expr_as(
                    b.name_or_id((Users::Table, Users::Name), (Executions::Table, Executions::TriggeredById)),
                    Alias::new("triggered_by"),
                )
                .expr_as(Expr::col((Projects::Table, Projects::Name)), Alias::new("project"))
                .from(Executions::Table)
                .join(
                    JoinType::LeftJoin,
                    Collections::Table,
                    Expr::col((Executions::Table, Executions::CollectionId)).equals((Collections::Table, Collections::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Functions::Table,
                    Expr::col((Executions::Table, Executions::FunctionVersionId)).equals((Functions::Table, Functions::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((Executions::Table, Executions::TriggeredById)).equals((Users::Table, Users::Id)),
                )
                .join(
                    JoinType::LeftJoin,
                    Projects::Table,
                    Expr::col((Executions::Table, Executions::ProjectId)).equals((Projects::Table, Projects::Id)),
                )
                .to_owned(),
        )]
    }
}
