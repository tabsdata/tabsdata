//
// Copyright 2026 Tabsdata Inc.
//

//! `CREATE STATISTICS`: without it the planner multiplies per-column
//! distinct counts and picks a spilling sort over a hash for correlated
//! grouping columns.

use sea_query::{DynIden, IntoIden};

use crate::schema::source::Derived;

pub(crate) struct StatisticsDef {
    name: &'static str,
    table: DynIden,
    columns: Vec<DynIden>,
}

impl StatisticsDef {
    pub(crate) fn n_distinct<T, C>(name: &'static str, table: T, columns: impl IntoIterator<Item = C>) -> Self
    where
        T: IntoIden,
        C: IntoIden,
    {
        Self {
            name,
            table: table.into_iden(),
            columns: columns.into_iter().map(IntoIden::into_iden).collect(),
        }
    }

    pub(crate) fn render(&self) -> Derived {
        let columns = self.columns.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
        Derived {
            create: vec![format!(
                "CREATE STATISTICS \"{}\" (ndistinct) ON {columns} FROM \"{}\"",
                self.name, self.table
            )],
            drop: vec![format!("DROP STATISTICS IF EXISTS \"{}\"", self.name)],
        }
    }
}
