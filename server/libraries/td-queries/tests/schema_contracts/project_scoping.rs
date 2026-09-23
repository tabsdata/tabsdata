//
// Copyright 2026 Tabsdata Inc.
//

//! Checks every DAO's `#[dao(project_scoped)]` claim against the policies the
//! database actually carries: a relation the `project_isolation` policy
//! governs must be marked, and a marked one must be governed.

use std::collections::{HashMap, HashSet};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::registered_daos;

/// Tables the policy governs directly, plus every view that reads one —
/// `security_invoker` views inherit their base tables' RLS.
async fn scoped_relations(db: &TdPool) -> HashSet<String> {
    let mut conn = db.begin().await.expect("a connection");
    let policed: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_policies \
         WHERE policyname = 'project_isolation' AND schemaname = current_schema()",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("pg_policies is readable");

    // pg_depend, not information_schema: the latter shows only views the
    // current user owns, and the test role is not their owner.
    let usage: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT v.relname, t.relname \
         FROM pg_depend d \
         JOIN pg_rewrite r ON r.oid = d.objid \
         JOIN pg_class v ON v.oid = r.ev_class \
         JOIN pg_class t ON t.oid = d.refobjid \
         WHERE d.classid = 'pg_rewrite'::regclass \
           AND d.refclassid = 'pg_class'::regclass \
           AND v.relkind = 'v' AND t.oid <> v.oid \
           AND v.relnamespace = current_schema()::regnamespace",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("pg_depend is readable");

    let mut reads: HashMap<String, Vec<String>> = HashMap::new();
    for (view, base) in usage {
        reads.entry(view).or_default().push(base);
    }

    let mut scoped: HashSet<String> = policed.into_iter().map(|(t,)| t).collect();
    loop {
        let grown: Vec<String> = reads
            .iter()
            .filter(|(v, bases)| !scoped.contains(*v) && bases.iter().any(|b| scoped.contains(b)))
            .map(|(v, _)| v.clone())
            .collect();
        if grown.is_empty() {
            break;
        }
        scoped.extend(grown);
    }
    scoped
}

#[td_test::test(db)]
async fn every_dao_scoping_matches_its_relations_policies(db: TdPool) -> Result<(), TdError> {
    let scoped = scoped_relations(&db).await;
    assert!(
        scoped.len() >= 35,
        "only {} project-scoped relations found; the policy query may be broken",
        scoped.len(),
    );

    let mut wrong = Vec::new();
    for dao in registered_daos() {
        let proj = (dao.describe)();
        let governed = scoped.contains(&proj.table);
        if governed != proj.project_scoped {
            wrong.push(format!(
                "`{}` (relation `{}`): marked project_scoped={}, policy says {}",
                proj.name, proj.table, proj.project_scoped, governed,
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "DAO project scoping disagrees with the schema:\n  {}",
        wrong.join("\n  "),
    );
    Ok(())
}
