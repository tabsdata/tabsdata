//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::collections::Collections;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::backend::Backend;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "inter_collection_permissions__with_names"]
pub struct InterCollectionPermissionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum InterCollectionPermissions {
    Table,
    Id,
    ProjectId,
    FromCollectionId,
    ToCollectionId,
    GrantedOn,
    GrantedById,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(InterCollectionPermissions::Table)
                .col(ColumnDef::new(InterCollectionPermissions::Id).text().not_null().primary_key())
                .col(ColumnDef::new(InterCollectionPermissions::ProjectId).text().not_null())
                .col(ColumnDef::new(InterCollectionPermissions::FromCollectionId).text().not_null())
                .col(ColumnDef::new(InterCollectionPermissions::ToCollectionId).text().not_null())
                .col(ColumnDef::new(InterCollectionPermissions::GrantedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(InterCollectionPermissions::GrantedById).text().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(InterCollectionPermissions::Table, InterCollectionPermissions::FromCollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .from(InterCollectionPermissions::Table, InterCollectionPermissions::ToCollectionId)
                        .to(Collections::Table, Collections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("inter_collection_permissions___from_collection_id__to_collection_id__idx")
                .table(InterCollectionPermissions::Table)
                .col(InterCollectionPermissions::FromCollectionId)
                .col(InterCollectionPermissions::ToCollectionId)
                .unique()
                .to_owned(),
        ]
    }

    fn create_views(&self, b: &Backend) -> Vec<ViewDef> {
        let from_collection = Alias::new("from_collection");
        let to_collection = Alias::new("to_collection");
        vec![ViewDef::new(
            InterCollectionPermissionsWithNames,
            Query::select()
                .expr(Expr::col((InterCollectionPermissions::Table, Asterisk)))
                .expr_as(
                    b.name_or_id((Users::Table, Users::Name), (InterCollectionPermissions::Table, InterCollectionPermissions::GrantedById)),
                    Alias::new("granted_by"),
                )
                .expr_as(Expr::col((from_collection.clone(), Collections::Name)), Alias::new("from_collection"))
                .expr_as(Expr::col((to_collection.clone(), Collections::Name)), Alias::new("to_collection"))
                .from(InterCollectionPermissions::Table)
                .join(
                    JoinType::LeftJoin,
                    Users::Table,
                    Expr::col((InterCollectionPermissions::Table, InterCollectionPermissions::GrantedById))
                        .equals((Users::Table, Users::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Collections::Table,
                    from_collection.clone(),
                    Expr::col((InterCollectionPermissions::Table, InterCollectionPermissions::FromCollectionId))
                        .equals((from_collection, Collections::Id)),
                )
                .join_as(
                    JoinType::LeftJoin,
                    Collections::Table,
                    to_collection.clone(),
                    Expr::col((InterCollectionPermissions::Table, InterCollectionPermissions::ToCollectionId))
                        .equals((to_collection, Collections::Id)),
                )
                .to_owned(),
        )]
    }
}
