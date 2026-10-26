//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ExprTrait, Iden, Index, IndexCreateStatement, IntoIden, IntoIndexColumn, Table,
    TableCreateStatement,
};

use crate::schema::Entity;
use crate::schema::source::Derived;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;
use crate::schema::sql::{ImmutableColumnsTriggerDef, Scd2TriggerDef, cols_of_except};

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum Tags {
    Table,
    Id,
    TagId,
    ProjectId,
    EntityType,
    EntityId,
    Kind,
    Key,
    Value,
    ValidFrom,
    DefinedById,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(Tags::Table)
                .col(id_column(Tags::Id).not_null().primary_key())
                .col(id_column(Tags::TagId).not_null())
                .col(id_column(Tags::ProjectId).not_null())
                .col(ColumnDef::new(Tags::EntityType).text().not_null())
                // Polymorphic over `entity_type`, so no FK.
                .col(id_column(Tags::EntityId).not_null())
                .col(ColumnDef::new(Tags::Kind).text().not_null())
                .col(ColumnDef::new(Tags::Key).text().not_null())
                .col(ColumnDef::new(Tags::Value).text().not_null())
                .col(ColumnDef::new(Tags::ValidFrom).timestamp_with_time_zone().not_null())
                .col(id_column(Tags::DefinedById).not_null())
                .col(ColumnDef::new(Tags::ValidTo).timestamp_with_time_zone().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            // The as-of topology load: the project's topology tags open at an
            // instant, without walking user tags or closed history.
            Index::create()
                .name("tags___project_id__valid_to___topology_idx")
                .table(Tags::Table)
                .col(Tags::ProjectId)
                .col(Tags::ValidTo)
                .and_where(Expr::col(Tags::Kind).eq("T"))
                .to_owned(),
            Index::create()
                .name("tags___tag_id__valid_from__id___idx")
                .table(Tags::Table)
                .col(Tags::TagId)
                .col(Tags::ValidFrom)
                .col(Tags::Id)
                .to_owned(),
            Index::create()
                .name("tags___tag_id___current_uidx")
                .table(Tags::Table)
                .col(Tags::TagId)
                .unique()
                .and_where(open_interval(Tags::ValidTo))
                .to_owned(),
            Index::create()
                .name("tags___entity_type__entity_id__key___current_uidx")
                .table(Tags::Table)
                .col(Tags::EntityType)
                .col(Tags::EntityId)
                .col(Tags::Key)
                .unique()
                .and_where(open_interval(Tags::ValidTo))
                .to_owned(),
            // `text_pattern_ops` on the value so a `<entity>.<key>:lk:<prefix>*`
            // filter and a facet prefix are a range scan; `=` still uses it.
            Index::create()
                .name("tags___key__value__entity_type___current_idx")
                .table(Tags::Table)
                .col(Tags::Key)
                .col(Tags::Value.into_index_column().with_operator_class(Alias::new("text_pattern_ops")))
                .col(Tags::EntityType)
                .and_where(open_interval(Tags::ValidTo))
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            ImmutableColumnsTriggerDef {
                name: format!("{}__immutable", Tags::Table.to_string()),
                source_table: Tags::Table.into_iden(),
                columns: cols_of_except(Tags::ValidTo),
            }
            .derived(),
            Scd2TriggerDef {
                name: format!("{}__scd2_close", Tags::Table.to_string()),
                source_table: Tags::Table.into_iden(),
                partition_col: Tags::TagId.into_iden(),
                valid_from_col: Tags::ValidFrom.into_iden(),
                valid_to_col: Tags::ValidTo.into_iden(),
                pk_col: Tags::Id.into_iden(),
            }
            .derived(),
        ]
    }
}
