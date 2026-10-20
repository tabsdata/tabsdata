//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, JoinType, Query, Table,
    TableCreateStatement,
};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::sql::ViewDef;
use crate::schema::sql::id_column;

#[derive(Iden, Clone, Copy)]
#[iden = "service_tokens__with_names"]
pub struct ServiceTokensWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum ServiceTokens {
    Table,
    Id,
    UserId,
    Slot,
    Digest,
    Hint,
    Comment,
    CreatedOn,
    ExpiresOn,
    LastUsedOn,
    Scope,
    System,
}

/// Two, so a rotation always has a free slot.
pub const SLOTS: [i32; 2] = [1, 2];

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(ServiceTokens::Table)
                .col(id_column(ServiceTokens::Id).not_null().primary_key())
                .col(id_column(ServiceTokens::UserId).not_null())
                .col(ColumnDef::new(ServiceTokens::Slot).integer().not_null().check(Expr::col(ServiceTokens::Slot).is_in(SLOTS)))
                .col(ColumnDef::new(ServiceTokens::Digest).text().not_null().unique_key())
                .col(ColumnDef::new(ServiceTokens::Hint).text().not_null())
                .col(ColumnDef::new(ServiceTokens::Comment).text().null())
                .col(ColumnDef::new(ServiceTokens::CreatedOn).timestamp_with_time_zone().not_null())
                // `infinity` = never expires; never NULL.
                .col(ColumnDef::new(ServiceTokens::ExpiresOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(ServiceTokens::LastUsedOn).timestamp_with_time_zone().null())
                .col(ColumnDef::new(ServiceTokens::Scope).text().not_null())
                .col(ColumnDef::new(ServiceTokens::System).boolean().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(ServiceTokens::Table, ServiceTokens::UserId)
                        .to(Users::Table, Users::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("service_tokens___user_id__slot___idx")
                .table(ServiceTokens::Table)
                .col(ServiceTokens::UserId)
                .col(ServiceTokens::Slot)
                .unique()
                .to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            ServiceTokensWithNames,
            Query::select()
                .expr(Expr::col((ServiceTokens::Table, Asterisk)))
                .expr_as(Expr::col((Users::Table, Users::Name)), Alias::new("user_name"))
                .expr_as(Expr::col((Users::Table, Users::Enabled)), Alias::new("user_enabled"))
                .from(ServiceTokens::Table)
                .join(
                    JoinType::Join,
                    Users::Table,
                    Expr::col((ServiceTokens::Table, ServiceTokens::UserId)).equals((Users::Table, Users::Id)),
                )
                .to_owned(),
        )]
    }
}
