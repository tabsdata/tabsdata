//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{DynIden, IntoIden, SelectStatement};

use crate::schema::backend::Backend;

/// A view definition: name (typed via [`Iden`](sea_query::Iden)) + SELECT
/// statement. Names are passed in as `Iden` structs so the view's identifier
/// lives in one place — the struct definition — and can be referenced from
/// other views without re-typing the string.
pub(crate) struct ViewDef {
    pub name: DynIden,
    pub select: SelectStatement,
}

impl ViewDef {
    pub(crate) fn new(name: impl IntoIden, select: SelectStatement) -> Self {
        Self {
            name: name.into_iden(),
            select,
        }
    }

    /// Render this view to a `CREATE VIEW name AS …` statement for `b`.
    pub(crate) fn render(&self, b: &Backend) -> String {
        format!("CREATE VIEW {} AS {}", self.name.to_string(), b.build_query(self.select.clone()))
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, Asterisk, Expr, Iden, IntoIden, Query};

    use super::*;

    #[derive(Iden, Clone, Copy)]
    #[iden = "my_view"]
    struct MyView;

    fn sample() -> ViewDef {
        ViewDef::new(MyView, Query::select().expr(Expr::col(Asterisk)).from(Alias::new("source")).to_owned())
    }

    #[test]
    fn render_emits_create_view_with_name_and_body() {
        let sql = sample().render(&Backend::Sqlite);
        assert!(sql.starts_with("CREATE VIEW my_view AS"), "{sql}");
        assert!(sql.contains("FROM \"source\""), "{sql}");
    }

    #[test]
    fn render_differs_by_backend() {
        let v = sample();
        let sqlite = v.render(&Backend::Sqlite);
        let mysql = v.render(&Backend::MySql);
        // SQLite/Postgres quote with `"`, MySQL with backticks. Body MUST differ
        // because the quoting style changes.
        assert!(sqlite.contains("\"source\""), "{sqlite}");
        assert!(mysql.contains("`source`"), "{mysql}");
    }

    #[test]
    fn name_is_typed_iden() {
        // Verify a non-derive Iden also works (sanity check on the API surface).
        let v =
            ViewDef::new(Alias::new("alias_view").into_iden(), Query::select().expr(Expr::col(Asterisk)).from(Alias::new("t")).to_owned());
        assert!(v.render(&Backend::Sqlite).starts_with("CREATE VIEW alias_view AS"));
    }
}
