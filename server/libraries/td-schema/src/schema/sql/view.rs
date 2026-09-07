//
// Copyright 2026 Tabsdata Inc.
//

use crate::schema::render::render_query;
use sea_query::{DynIden, IntoIden, SelectStatement};

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

    /// Render this view to a `CREATE VIEW name AS …` statement.
    ///
    /// The view is created `WITH (security_invoker = true)` so it runs with the
    /// querying role's row-level security, not the view owner's. Without this a
    /// view over an RLS table would be evaluated as its owner and bypass project
    /// isolation. The option is inert on views over non-RLS tables, so it is
    /// applied uniformly. (PostgreSQL 15+.)
    pub(crate) fn render(&self) -> String {
        format!("CREATE VIEW {} WITH (security_invoker = true) AS {}", self.name, render_query(self.select.clone()))
    }

    /// No `CASCADE`: an unexpected dependent must fail loudly, not vanish.
    pub(crate) fn render_drop(&self) -> String {
        format!("DROP VIEW IF EXISTS {}", self.name)
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
    fn render_emits_security_invoker_view_with_name_and_body() {
        let sql = sample().render();
        assert!(sql.starts_with("CREATE VIEW my_view WITH (security_invoker = true) AS"), "{sql}");
        assert!(sql.contains("FROM \"source\""), "{sql}");
    }

    #[test]
    fn name_is_typed_iden() {
        // Verify a non-derive Iden also works (sanity check on the API surface).
        let v =
            ViewDef::new(Alias::new("alias_view").into_iden(), Query::select().expr(Expr::col(Asterisk)).from(Alias::new("t")).to_owned());
        assert!(v.render().starts_with("CREATE VIEW alias_view WITH (security_invoker = true) AS"));
    }

    #[test]
    fn drop_is_not_cascading() {
        assert_eq!(sample().render_drop(), "DROP VIEW IF EXISTS my_view");
    }
}
