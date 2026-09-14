//
// Copyright 2026 Tabsdata Inc.
//

//! `CREATE INDEX … WHERE`: sea_query's `IndexCreateStatement` carries no
//! predicate, so partial indexes are rendered here.

use sea_query::{DynIden, IntoIden};

use crate::schema::source::Derived;

pub(crate) struct PartialIndexDef {
    name: &'static str,
    table: DynIden,
    columns: Vec<DynIden>,
    predicate: String,
}

impl PartialIndexDef {
    pub(crate) fn new<T, C>(name: &'static str, table: T, columns: impl IntoIterator<Item = C>, predicate: impl Into<String>) -> Self
    where
        T: IntoIden,
        C: IntoIden,
    {
        Self {
            name,
            table: table.into_iden(),
            columns: columns.into_iter().map(IntoIden::into_iden).collect(),
            predicate: predicate.into(),
        }
    }

    pub(crate) fn render(&self) -> Derived {
        let columns = self.columns.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
        Derived {
            create: vec![format!(
                "CREATE INDEX \"{}\" ON \"{}\" ({columns}) WHERE {}",
                self.name, self.table, self.predicate
            )],
            drop: vec![format!("DROP INDEX IF EXISTS \"{}\"", self.name)],
        }
    }
}
