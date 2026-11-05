//
// Copyright 2026 Tabsdata Inc.
//

//! `CREATE STATISTICS`: without it the planner multiplies per-column
//! distinct counts and picks a spilling sort over a hash for correlated
//! grouping columns, and multiplies a key's filter by the tenant filter it
//! already implies.

use sea_query::{Alias, DynIden, IntoIden};

use crate::schema::source::Derived;

pub struct StatisticsDef {
    name: String,
    kind: &'static str,
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
            name: name.to_string(),
            kind: "ndistinct",
            table: table.into_iden(),
            columns: columns.into_iter().map(IntoIden::into_iden).collect(),
        }
    }

    pub(crate) fn project_dependency(name: String, table: DynIden, column: DynIden) -> Self {
        Self {
            name,
            kind: "dependencies",
            table,
            columns: vec![column, Alias::new("project_id").into_iden()],
        }
    }

    pub(crate) fn render(&self) -> Derived {
        let columns = self.columns.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
        Derived {
            create: vec![format!(
                "CREATE STATISTICS \"{}\" ({}) ON {columns} FROM \"{}\"",
                self.name, self.kind, self.table
            )],
            drop: vec![format!("DROP STATISTICS IF EXISTS \"{}\"", self.name)],
        }
    }
}
