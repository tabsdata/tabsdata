//
// Copyright 2026 Tabsdata Inc.
//

//! The migration invariant, against a real database:
//! `archive(N) + deltas(N+1..=CURRENT) ≡ current(CURRENT)`. td-schema's guards only
//! prove an archive and a delta exist and render non-empty SQL.
//!
//! No-op until the first in-major bump; the [`DestructiveFixture`] test keeps
//! the pool's drop-ladder-rebuild ordering exercised regardless.

use crate::components::{ApiserverComponent, SchemaComponent};
use crate::pool::TdPool;
use crate::sea_query::builder::SeaQueryExt;
use crate::test_utils::TestDb;
use sea_query::{
    Alias, Asterisk, ColumnDef, Expr, ExprTrait, JoinType, Order, Query, SelectStatement, Table,
};
use td_schema::schema::Derived;
use td_schema::schema::archive;
use td_schema::schema::render::{render_query, render_schema};
use td_schema::{CURRENT_VERSION, CurrentSource, SchemaSource};

fn current_schema() -> Expr {
    Expr::cust("current_schema()")
}

fn postgres_fingerprint() -> Vec<SelectStatement> {
    vec![
        Query::select()
            .expr(Expr::cust(
                "'column ' || table_name || '.' || column_name || ':' || data_type \
                 || ':nullable=' || is_nullable || ':default=' || COALESCE(column_default, '')",
            ))
            .from((Alias::new("information_schema"), Alias::new("columns")))
            .and_where(Expr::col(Alias::new("table_schema")).eq(current_schema()))
            .order_by(Alias::new("table_name"), Order::Asc)
            .order_by(Alias::new("column_name"), Order::Asc)
            .to_owned(),
        Query::select()
            .expr(Expr::cust(
                "'view ' || viewname || ' ' || replace(definition, current_schema() || '.', '')",
            ))
            .from(Alias::new("pg_views"))
            .and_where(Expr::col(Alias::new("schemaname")).eq(current_schema()))
            .order_by(Alias::new("viewname"), Order::Asc)
            .to_owned(),
        Query::select()
            .expr(Expr::cust(
                "'index ' || indexname || ' ' || replace(indexdef, current_schema() || '.', '')",
            ))
            .from(Alias::new("pg_indexes"))
            .and_where(Expr::col(Alias::new("schemaname")).eq(current_schema()))
            .order_by(Alias::new("indexname"), Order::Asc)
            .to_owned(),
        // `pg_get_triggerdef` stops at `EXECUTE FUNCTION x_fn()`, so a trigger
        // body that pins a stale column list is invisible without the function.
        Query::select()
            .expr(Expr::cust(
                "'function ' || p.proname || ' ' \
                 || replace(pg_get_functiondef(p.oid), current_schema() || '.', '')",
            ))
            .from_as(Alias::new("pg_proc"), Alias::new("p"))
            .join_as(
                JoinType::Join,
                Alias::new("pg_namespace"),
                Alias::new("n"),
                Expr::cust("n.oid = p.pronamespace"),
            )
            .and_where(Expr::cust("n.nspname").eq(current_schema()))
            .order_by((Alias::new("p"), Alias::new("proname")), Order::Asc)
            .to_owned(),
        Query::select()
            .expr(Expr::cust(
                "'trigger ' || c.relname || '.' || t.tgname || ' ' \
                 || replace(pg_get_triggerdef(t.oid), current_schema() || '.', '')",
            ))
            .from_as(Alias::new("pg_trigger"), Alias::new("t"))
            .join_as(
                JoinType::Join,
                Alias::new("pg_class"),
                Alias::new("c"),
                Expr::cust("c.oid = t.tgrelid"),
            )
            .join_as(
                JoinType::Join,
                Alias::new("pg_namespace"),
                Alias::new("n"),
                Expr::cust("n.oid = c.relnamespace"),
            )
            .and_where(Expr::cust("n.nspname").eq(current_schema()))
            .and_where(Expr::cust("NOT t.tgisinternal"))
            .order_by((Alias::new("c"), Alias::new("relname")), Order::Asc)
            .order_by((Alias::new("t"), Alias::new("tgname")), Order::Asc)
            .to_owned(),
        // A project-scoped table added without its policy is a cross-tenant leak.
        Query::select()
            .expr(Expr::cust(
                "'rls ' || c.relname || ':enabled=' || c.relrowsecurity \
                 || ':forced=' || c.relforcerowsecurity",
            ))
            .from_as(Alias::new("pg_class"), Alias::new("c"))
            .join_as(
                JoinType::Join,
                Alias::new("pg_namespace"),
                Alias::new("n"),
                Expr::cust("n.oid = c.relnamespace"),
            )
            .and_where(Expr::cust("n.nspname").eq(current_schema()))
            // Bound as text, `relkind`'s "char" type has no matching operator.
            .and_where(Expr::cust("c.relkind = 'r'"))
            .order_by((Alias::new("c"), Alias::new("relname")), Order::Asc)
            .to_owned(),
        Query::select()
            .expr(Expr::cust(
                "'policy ' || tablename || '.' || policyname || ' ' || cmd || ' ' \
                 || replace(COALESCE(qual, ''), current_schema() || '.', '') || ' ' \
                 || replace(COALESCE(with_check, ''), current_schema() || '.', '')",
            ))
            .from(Alias::new("pg_policies"))
            .and_where(Expr::col(Alias::new("schemaname")).eq(current_schema()))
            .order_by(Alias::new("tablename"), Order::Asc)
            .order_by(Alias::new("policyname"), Order::Asc)
            .to_owned(),
    ]
}

async fn fingerprint(db: &TdPool) -> Vec<String> {
    let queries = postgres_fingerprint();
    let mut out = Vec::new();
    for (i, mut query) in queries.into_iter().enumerate() {
        let rows: Vec<String> = query.build_query_scalar().fetch_all(db).await.unwrap();
        // A query that silently matches nothing would make the comparison vacuous.
        assert!(!rows.is_empty(), "fingerprint query {i} returned no rows");
        out.extend(rows);
    }
    out
}

#[allow(clippy::reversed_empty_ranges)] // empty until the first bump
async fn assert_ladder_matches_fresh_install(new_db: impl AsyncFn() -> TestDb) {
    for from in 1..CURRENT_VERSION {
        let migrated = new_db().await;
        migrated
            .admin
            .upgrade_source(archive::source(from))
            .await
            .unwrap_or_else(|e| panic!("installing archive v{from}: {e:?}"));
        assert_eq!(migrated.admin.db_version().await.unwrap(), Some(from));

        migrated
            .admin
            .upgrade(&ApiserverComponent)
            .await
            .unwrap_or_else(|e| panic!("upgrading v{from} -> v{CURRENT_VERSION}: {e:?}"));
        assert_eq!(
            migrated.admin.db_version().await.unwrap(),
            Some(CurrentSource.current_version())
        );

        let fresh = new_db().await;
        fresh.upgrade().await.unwrap();

        let (migrated_fingerprint, fresh_fingerprint) = (
            fingerprint(&migrated.serving).await,
            fingerprint(&fresh.serving).await,
        );
        let only_migrated: Vec<_> = migrated_fingerprint
            .iter()
            .filter(|o| !fresh_fingerprint.contains(o))
            .collect();
        let only_fresh: Vec<_> = fresh_fingerprint
            .iter()
            .filter(|o| !migrated_fingerprint.contains(o))
            .collect();
        assert!(
            only_migrated.is_empty() && only_fresh.is_empty(),
            "schema migrated from v{from} differs from a fresh v{CURRENT_VERSION} install.\n\
             only in the migrated database: {only_migrated:#?}\n\
             only in the fresh database: {only_fresh:#?}"
        );

        ApiserverComponent
            .check(&migrated.serving)
            .await
            .unwrap_or_else(|e| panic!("check after upgrade from v{from}: {e:?}"));
    }
}

#[tokio::test]
async fn migrated_schema_matches_fresh_install_postgres() {
    assert_ladder_matches_fresh_install(async || crate::test_utils::postgres::pool().await).await;
}

/// A throwaway v1 -> v2 bump whose delta drops a column under a standing
/// `SELECT *` view — only passes on PostgreSQL because the pool drops derived
/// objects before the ladder.
struct DestructiveFixture(usize);

impl SchemaSource for DestructiveFixture {
    fn current_version(&self) -> usize {
        self.0
    }

    fn tables(&self) -> Vec<String> {
        let mut table = Table::create();
        table
            .table(Alias::new("fixture"))
            .col(ColumnDef::new(Alias::new("a")).text());
        if self.0 == 1 {
            table.col(ColumnDef::new(Alias::new("b")).text());
        }
        vec![render_schema(table)]
    }

    fn delta(&self, target: usize) -> Vec<String> {
        assert_eq!(target, 2);
        vec![render_schema(
            Table::alter()
                .table(Alias::new("fixture"))
                .drop_column(Alias::new("b"))
                .to_owned(),
        )]
    }

    fn views(&self) -> Vec<Derived> {
        let select = Query::select()
            .expr(Expr::col(Asterisk))
            .from(Alias::new("fixture"))
            .to_owned();
        vec![Derived {
            create: vec![format!(
                "CREATE VIEW fixture_view AS {}",
                render_query(select)
            )],
            drop: vec!["DROP VIEW IF EXISTS fixture_view".to_string()],
        }]
    }
}

async fn view_columns(db: &TdPool, view: &str) -> Vec<String> {
    // `pg_catalog`, not `information_schema`, which filters by privilege.
    let sql = format!(
        "SELECT a.attname FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = current_schema() AND c.relname = '{view}' \
         AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attname"
    );
    super::scalars(db, &sql).await
}

async fn assert_destructive_delta_clears_standing_views(new_db: impl AsyncFn() -> TestDb) {
    let db = new_db().await;
    // `upgrade_source` also grants the app role: without the schema `USAGE`
    // grant, serving's `current_schema()` is NULL.
    db.admin
        .upgrade_source(&DestructiveFixture(1))
        .await
        .unwrap();
    assert_eq!(view_columns(&db.serving, "fixture_view").await, ["a", "b"]);

    db.admin
        .upgrade_source(&DestructiveFixture(2))
        .await
        .unwrap();
    assert_eq!(db.admin.db_version().await.unwrap(), Some(2));
    assert_eq!(view_columns(&db.serving, "fixture_view").await, ["a"]);
}

/// Two upgraders racing the same fresh schema: the `tabsdata_system` lock
/// makes the loser wait, re-read `db_version`, and no-op — both must succeed.
#[tokio::test]
async fn concurrent_upgrades_serialize_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    let (a, b) = (db.admin.clone(), db.admin.clone());
    let (ra, rb) = tokio::join!(
        tokio::spawn(async move { a.upgrade_source(&DestructiveFixture(2)).await }),
        tokio::spawn(async move { b.upgrade_source(&DestructiveFixture(2)).await }),
    );
    ra.unwrap().unwrap();
    rb.unwrap().unwrap();
    assert_eq!(db.admin.db_version().await.unwrap(), Some(2));
    assert_eq!(view_columns(&db.serving, "fixture_view").await, ["a"]);
}

/// A v1 -> v3 ladder whose second hop is invalid: the failure must undo the
/// first hop *and* the derived-object drop the ladder needed, leaving the
/// schema exactly as it was.
struct FailingFixture;

impl SchemaSource for FailingFixture {
    fn current_version(&self) -> usize {
        3
    }

    fn tables(&self) -> Vec<String> {
        DestructiveFixture(3).tables()
    }

    fn views(&self) -> Vec<Derived> {
        DestructiveFixture(3).views()
    }

    fn delta(&self, target: usize) -> Vec<String> {
        match target {
            2 => DestructiveFixture(2).delta(2),
            3 => vec![render_schema(
                Table::alter()
                    .table(Alias::new("fixture"))
                    .drop_column(Alias::new("absent"))
                    .to_owned(),
            )],
            other => panic!("unexpected target {other}"),
        }
    }
}

async fn assert_failed_ladder_rolls_back(new_db: impl AsyncFn() -> TestDb) {
    let db = new_db().await;
    db.admin
        .upgrade_source(&DestructiveFixture(1))
        .await
        .unwrap();
    assert_eq!(view_columns(&db.serving, "fixture_view").await, ["a", "b"]);

    db.admin
        .upgrade_source(&FailingFixture)
        .await
        .expect_err("the second hop must fail");

    assert_eq!(db.admin.db_version().await.unwrap(), Some(1));
    assert_eq!(view_columns(&db.serving, "fixture_view").await, ["a", "b"]);
}

#[tokio::test]
async fn failed_ladder_rolls_back_postgres() {
    assert_failed_ladder_rolls_back(async || crate::test_utils::postgres::pool().await).await;
}

#[tokio::test]
async fn destructive_delta_clears_standing_views_postgres() {
    assert_destructive_delta_clears_standing_views(async || {
        crate::test_utils::postgres::pool().await
    })
    .await;
}
