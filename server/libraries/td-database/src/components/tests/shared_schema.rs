//
// Copyright 2026 Tabsdata Inc.
//

//! The pool's **shared** schema lifecycle: the `tabsdata_system` ledger is created
//! by shared logic (not each schema's own content), and `db_version` is written
//! and advanced by shared logic — driven from a component's [`SchemaSource`] /
//! `SchemaDelta`. The test source declares only its own content, never the ledger
//! or the version row, proving those are the pool's responsibility.
//!
//! SQLite-only: the mechanism is backend-agnostic, so one backend exercises it.

use crate::dynamic::DynamicConnection;
use crate::test_utils::TestDb;
use crate::test_utils::sqlite::pool;
use td_schema::SchemaSource;
use td_schema::schema::SchemaDelta;
use td_schema::schema::backend::Backend;

/// The v1→v2 migration of the component's own content. Real deltas bump
/// `db_version` themselves; this one omits it so the assertions below prove the
/// pool's own write records the version.
struct WidgetV1ToV2;

impl SchemaDelta for WidgetV1ToV2 {
    const FROM_VERSION: usize = 1;
    const TO_VERSION: usize = 2;
    fn up(_backend: Backend) -> Vec<String> {
        vec!["ALTER TABLE widget ADD COLUMN note TEXT".to_string()]
    }
}

/// A component's own content — just its table. Crucially it does **not**
/// create `tabsdata_system` nor seed `db_version`; the pool owns both.
struct WidgetSource {
    version: usize,
}

impl SchemaSource for WidgetSource {
    fn current_version(&self) -> usize {
        self.version
    }
    fn tables(&self, _backend: Backend) -> Vec<String> {
        vec!["CREATE TABLE widget (id TEXT PRIMARY KEY)".to_string()]
    }
    fn delta(&self, target: usize, backend: Backend) -> Vec<String> {
        assert_eq!(
            target,
            WidgetV1ToV2::TO_VERSION,
            "only the v2 delta is defined"
        );
        WidgetV1ToV2::up(backend)
    }
}

/// Read a scalar `i64` on the serving pool (same sqlite file as the system pool).
async fn scalar(db: &TestDb, sql: &str) -> i64 {
    let mut conn = db.serving.acquire().await.unwrap();
    let DynamicConnection::Sqlite(h) = &mut conn else {
        unreachable!("sqlite test")
    };
    sqlx::query_scalar(sql).fetch_one(&mut **h).await.unwrap()
}

/// On a fresh install the pool creates the `tabsdata_system` ledger and records
/// `db_version`, even though the source's `build` does neither.
#[tokio::test]
async fn ledger_and_version_created_by_shared_logic() {
    // The source's own install script never touches the shared ledger.
    let build =
        td_schema::schema::install_script(&WidgetSource { version: 1 }, Backend::Sqlite).join("\n");
    assert!(
        !build.contains("tabsdata_system") && !build.contains("db_version"),
        "the source must declare only its own content, got: {build}"
    );

    let db = pool().await;
    let system = &db.admin;
    system
        .upgrade_source(&WidgetSource { version: 1 })
        .await
        .unwrap();

    // The pool created the ledger and wrote the version row...
    assert_eq!(system.db_version().await.unwrap(), Some(1));
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM tabsdata_system WHERE name = 'db_version'"
        )
        .await,
        1,
        "the pool created the shared ledger and seeded db_version"
    );
    // ...and the source's own table is present.
    assert_eq!(scalar(&db, "SELECT count(*) FROM widget").await, 0);
}

/// An upgrade applies the `SchemaDelta` and advances `db_version` through the
/// shared logic — the delta never touches the version row itself.
#[tokio::test]
async fn upgrade_advances_version_via_delta() {
    let db = pool().await;
    let system = &db.admin;

    system
        .upgrade_source(&WidgetSource { version: 1 })
        .await
        .unwrap();
    assert_eq!(system.db_version().await.unwrap(), Some(1));

    // v1 → v2: shared logic runs the delta and bumps db_version.
    system
        .upgrade_source(&WidgetSource { version: 2 })
        .await
        .unwrap();
    assert_eq!(system.db_version().await.unwrap(), Some(2));

    // The delta actually ran: the column it adds now exists.
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM pragma_table_info('widget') WHERE name = 'note'"
        )
        .await,
        1,
        "the v1->v2 delta added the `note` column"
    );

    // Idempotent: re-running at the current version is a no-op.
    system
        .upgrade_source(&WidgetSource { version: 2 })
        .await
        .unwrap();
    assert_eq!(system.db_version().await.unwrap(), Some(2));
}
