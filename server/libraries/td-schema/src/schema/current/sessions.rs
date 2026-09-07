//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, Iden, Index, IndexCreateStatement, JoinType, Query, Table, TableCreateStatement,
};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "sessions__with_names"]
pub struct SessionsWithNames;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Sessions {
    Table,
    AccessTokenId,
    RefreshTokenId,
    UserId,
    CreatedOn,
    ExpiresOn,
    StatusChangeOn,
    Status,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Sessions::Table)
                .col(ColumnDef::new(Sessions::AccessTokenId).text().not_null().primary_key())
                .col(ColumnDef::new(Sessions::RefreshTokenId).text().not_null().unique_key())
                .col(ColumnDef::new(Sessions::UserId).text().not_null())
                .col(ColumnDef::new(Sessions::CreatedOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Sessions::ExpiresOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Sessions::StatusChangeOn).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(Sessions::Status).text().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // For session purge: DELETE WHERE expires_on < now
            Index::create().name("sessions___expires_on___idx").table(Sessions::Table).col(Sessions::ExpiresOn).to_owned(),
            // For session lookup by user during refresh
            Index::create().name("sessions___user_id___idx").table(Sessions::Table).col(Sessions::UserId).to_owned(),
        ]
    }

    fn create_views(&self) -> Vec<ViewDef> {
        vec![ViewDef::new(
            SessionsWithNames,
            Query::select()
                .expr(Expr::col((Sessions::Table, Asterisk)))
                .expr_as(Expr::col((Users::Table, Users::Name)), Alias::new("user_name"))
                .from(Sessions::Table)
                .join(JoinType::Join, Users::Table, Expr::col((Sessions::Table, Sessions::UserId)).equals((Users::Table, Users::Id)))
                .to_owned(),
        )]
    }
}
