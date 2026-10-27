//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ConditionalStatement, Iden, Index, IndexCreateStatement, IntoIden, Table, TableCreateStatement};

use crate::schema::Entity;
use crate::schema::source::Derived;
use crate::schema::sql::Scd2TriggerDef;
use crate::schema::sql::id_column;
use crate::schema::sql::scd2::open_interval;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum ProjectTopologies {
    Table,
    Id,
    ProjectId,
    ValidFrom,
    ValidTo,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(ProjectTopologies::Table)
                .col(id_column(ProjectTopologies::Id).not_null().primary_key())
                .col(id_column(ProjectTopologies::ProjectId).not_null())
                .col(ColumnDef::new(ProjectTopologies::ValidFrom).timestamp_with_time_zone().not_null())
                .col(ColumnDef::new(ProjectTopologies::ValidTo).timestamp_with_time_zone().not_null())
                .to_owned(),
        ]
    }

    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("project_topologies___project_id__valid_from___uidx")
                .table(ProjectTopologies::Table)
                .col(ProjectTopologies::ProjectId)
                .col(ProjectTopologies::ValidFrom)
                .unique()
                .to_owned(),
            // One open version per project.
            Index::create()
                .name("project_topologies___project_id___current_uidx")
                .table(ProjectTopologies::Table)
                .col(ProjectTopologies::ProjectId)
                .unique()
                .and_where(open_interval(ProjectTopologies::ValidTo))
                .to_owned(),
        ]
    }

    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![
            Scd2TriggerDef {
                name: format!("{}__scd2_close", ProjectTopologies::Table.to_string()),
                source_table: ProjectTopologies::Table.into_iden(),
                partition_col: ProjectTopologies::ProjectId.into_iden(),
                valid_from_col: ProjectTopologies::ValidFrom.into_iden(),
                valid_to_col: ProjectTopologies::ValidTo.into_iden(),
                pk_col: ProjectTopologies::Id.into_iden(),
            }
            .derived(),
        ]
    }
}
