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
    ///
    /// On PostgreSQL the view is created `WITH (security_invoker = true)` so it
    /// runs with the querying role's row-level security, not the view owner's.
    /// Without this a view over an RLS table would be evaluated as its owner and
    /// bypass project isolation. The option is inert on views over non-RLS
    /// tables, so it is applied uniformly. (PostgreSQL 15+.)
    pub(crate) fn render(&self, b: &Backend) -> String {
        let body = b.build_query(self.select.clone());
        match b {
            Backend::Postgres => format!("CREATE VIEW {} WITH (security_invoker = true) AS {}", self.name, body),
            _ => format!("CREATE VIEW {} AS {}", self.name, body),
        }
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, Asterisk, Expr, Iden, IntoIden, Query};

    use super::*;

    #[derive(Iden, Clone, Copy)]
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
