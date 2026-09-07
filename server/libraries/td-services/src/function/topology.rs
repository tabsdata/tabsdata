//
// Copyright 2026 Tabsdata Inc.
//

//! Function-version topology helpers.
//!
//! Patches share the topology of their parent by construction
//! (patches don't own `tables` / `dependencies` rows), so resolving
//! "which fvid to run" is purely a head-vs-latest-patch choice.

use std::collections::{HashMap, HashSet};
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::function::{FunctionDB, FunctionPatchDB};
use td_objects::types::basic::{AtTime, FunctionVersionId, Parent, Patch};
use td_queries::Queries;

/// Resolve a fvid to its canonical head: try `functions` first, fall
/// back to `function_patches`. Returns the parent fvid for patches and
/// the row's own id for canonicals.
pub async fn head_of<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    fvid: &FunctionVersionId,
) -> Result<FunctionVersionId, TdError> {
    if let Some(row) = conn.fetch_optional::<FunctionDB>(fvid).await? {
        return Ok(row.base.id);
    }
    let patch: FunctionPatchDB = conn.fetch_one::<FunctionPatchDB>(fvid).await?;
    Ok(patch.base.parent_function_version_id.into_inner())
}

/// The pair of ids needed to mint an FR/TDV: the head fvid (FK-valid
/// against `functions`) and an optional patch id (FK-valid against
/// `function_patches`) chosen by the latest-patch-or-head resolver.
#[derive(Debug, Clone)]
pub struct RunnableVersion {
    pub function_version_id: FunctionVersionId,
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
}

impl RunnableVersion {
    /// The id that actually drives the worker's bundle_id/snippet —
    /// patch if set, else head.
    pub fn effective(&self) -> &FunctionVersionId {
        self.function_version_patch_id
            .as_deref()
            .unwrap_or(&self.function_version_id)
    }
}

/// Resolve `fvid` to the head fvid + an optional patch id, pinned to
/// time `at` (at most one row per head partition: the latest `Active`
/// patch with `defined_on <= at`). Plan-build and replay each pass
/// their own `at`, so patches registered later don't backfill into
/// earlier mints.
pub async fn latest_patch_or_head<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    fvid: &FunctionVersionId,
    at: &AtTime,
) -> Result<RunnableVersion, TdError> {
    let head_fvid = head_of(conn, fvid).await?;
    let latest: Option<FunctionPatchDB> = conn
        .as_of(at)
        .fetch_optional(and!(
            &Parent::from(head_fvid.clone()),
            FunctionPatchDB::active()
        ))
        .await?;
    Ok(RunnableVersion {
        function_version_id: head_fvid,
        function_version_patch_id: latest.map(|v| Patch::from(v.base.id)),
    })
}

/// Batched `latest_patch_or_head` for many fvids in one tick, pinned
/// to time `at`. Returns a map keyed by the *input* fvid so callers
/// can look up the runnable for each LFR they're minting.
pub async fn latest_patch_or_head_batch<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    fvids: Vec<FunctionVersionId>,
    at: &AtTime,
) -> Result<HashMap<FunctionVersionId, RunnableVersion>, TdError> {
    if fvids.is_empty() {
        return Ok(HashMap::new());
    }
    // Any fvid missing from `functions` is presumed to be a patch id (rare
    // on the dispatch path — LFRs are minted with head fvids).
    let canonical: Vec<FunctionDB> = conn
        .history()
        .chunked()
        .fetch_all::<FunctionDB>(&fvids, ())
        .await?;
    let mut canonical_by_id: HashMap<FunctionVersionId, FunctionDB> = canonical
        .into_iter()
        .map(|f| (f.base.id.clone(), f))
        .collect();
    let unresolved: Vec<FunctionVersionId> = fvids
        .iter()
        .filter(|id| !canonical_by_id.contains_key(*id))
        .cloned()
        .collect();
    let patch_lookups: Vec<FunctionPatchDB> = if unresolved.is_empty() {
        Vec::new()
    } else {
        conn.history()
            .chunked()
            .fetch_all::<FunctionPatchDB>(&unresolved, ())
            .await?
    };
    let mut patch_lookup_by_id: HashMap<FunctionVersionId, FunctionPatchDB> = patch_lookups
        .into_iter()
        .map(|p| (p.base.id.clone(), p))
        .collect();

    let mut head_by_input: HashMap<FunctionVersionId, FunctionVersionId> = HashMap::new();
    for fvid in fvids {
        if let Some(row) = canonical_by_id.remove(&fvid) {
            head_by_input.insert(fvid, row.base.id);
        } else if let Some(patch) = patch_lookup_by_id.remove(&fvid) {
            head_by_input.insert(fvid, patch.base.parent_function_version_id.into_inner());
        }
    }

    // One point-in-time read keyed by the distinct head fvids; the partition
    // is `parent_function_version_id`, so each head returns at most one row.
    let mut seen: HashSet<&FunctionVersionId> = HashSet::with_capacity(head_by_input.len());
    let head_fvids: Vec<FunctionVersionId> = head_by_input
        .values()
        .filter(|v| seen.insert(v))
        .cloned()
        .collect();
    let latest_patches: Vec<FunctionPatchDB> = if head_fvids.is_empty() {
        Vec::new()
    } else {
        let head_partitions: Vec<_> = head_fvids.iter().cloned().map(Parent::from).collect();
        conn.as_of(at)
            .chunked()
            .fetch_all(&head_partitions, FunctionPatchDB::active())
            .await?
    };
    let patch_by_head: HashMap<FunctionVersionId, FunctionPatchDB> = latest_patches
        .into_iter()
        .map(|p| ((*p.base.parent_function_version_id).clone(), p))
        .collect();

    let mut out = HashMap::with_capacity(head_by_input.len());
    for (input_fvid, head_fvid) in head_by_input {
        let latest = patch_by_head
            .get(&head_fvid)
            .map(|p| Patch::from(p.base.id.clone()));
        out.insert(
            input_fvid,
            RunnableVersion {
                function_version_id: head_fvid,
                function_version_patch_id: latest,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::access::InProject;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::function::FunctionPatchInsertDB;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, Description, FunctionRuntimeValues, FunctionStatus,
        Patch, ProjectId, RawSnippet, Snippet, TableNameDto, UserId,
    };
    use td_queries::test_utils::SeedEntry;

    async fn seed_head(
        db: &TdPool,
        project_id: ProjectId,
        coll_name: &str,
        fn_name: &str,
    ) -> Result<(ActiveCollectionDB, FunctionDB), TdError> {
        let coll = db
            .scoped(project_id)
            .seed()
            .collection(&CollectionName::try_from(coll_name)?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: fn_name.try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let head = db
            .scoped(project_id)
            .seed()
            .function(&coll, &create)
            .await?;
        Ok((coll, head))
    }

    async fn insert_patch(
        db: &TdPool,
        project_id: ProjectId,
        head: &FunctionDB,
        defined_on: AtTime,
        snippet: &str,
    ) -> Result<FunctionVersionId, TdError> {
        let patch = FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: head.base.id.clone().into(),
            function_id: head.base.function_id,
            collection_id: head.base.collection_id,
            snippet: Snippet::try_from(RawSnippet::try_from(snippet)?)?,
            bundle_id: BundleId::default(),
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            description: Description::try_from("d")?,
            defined_on,
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(project_id).insert(&patch).await?;
        Ok(patch.id)
    }

    /// Resolving a head fvid at time `T1` returns the patch active at `T1`,
    /// ignoring later patches. Catches the regression where mint reads the
    /// freshest patch wall-clock-now instead of pinning to the request time.
    #[td_test::test(db)]
    async fn test_latest_patch_or_head_time_pinning(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_coll, head) = seed_head(&db, p.id, "c0", "f0").await?;

        // Three deterministic points: `before` (epoch — no patches), `t1`
        // (p1 alive), `t2 = t1 + 1ms` (p2 alive).
        let before = AtTime::from(td_common::datetime::Timestamp::try_from(0_i64).unwrap());
        let t1 = AtTime::now();
        let t2 = AtTime::from(td_common::datetime::Timestamp::from(
            **t1 + chrono::Duration::milliseconds(1),
        ));
        let p1 = insert_patch(&db, p.id, &head, t1, "p1").await?;
        let p2 = insert_patch(&db, p.id, &head, t2, "p2").await?;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let at_t1 = latest_patch_or_head(&mut conn, &head.base.id, &t1).await?;
        assert_eq!(at_t1.function_version_id, head.base.id);
        assert_eq!(at_t1.function_version_patch_id, Some(Patch::from(p1)));

        let at_t2 = latest_patch_or_head(&mut conn, &head.base.id, &t2).await?;
        assert_eq!(at_t2.function_version_patch_id, Some(Patch::from(p2)));

        let at_before = latest_patch_or_head(&mut conn, &head.base.id, &before).await?;
        assert_eq!(at_before.function_version_patch_id, None);
        Ok(())
    }

    /// At partition isolation: two head fvids each have their
    /// own patch lineage. Resolving one head must never see the other
    /// head's patches. Catches a regression where `partition_by` drifts
    /// from `parent_function_version_id` (or where the batch resolver
    /// crosses partitions).
    #[td_test::test(db)]
    async fn test_latest_patch_or_head_batch_partition_isolation(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_coll_a, head_a) = seed_head(&db, p.id, "c0", "fa").await?;
        let (_coll_b, head_b) = seed_head(&db, p.id, "c1", "fb").await?;

        let now = AtTime::now();
        let p_a = insert_patch(&db, p.id, &head_a, now, "p-a").await?;
        let p_b = insert_patch(&db, p.id, &head_b, now, "p-b").await?;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        let map = latest_patch_or_head_batch(
            &mut conn,
            vec![head_a.base.id.clone(), head_b.base.id.clone()],
            &now,
        )
        .await?;

        let r_a = map.get(&head_a.base.id).expect("missing head_a");
        let r_b = map.get(&head_b.base.id).expect("missing head_b");
        assert_eq!(r_a.function_version_id, head_a.base.id);
        assert_eq!(r_a.function_version_patch_id, Some(Patch::from(p_a)));
        assert_eq!(r_b.function_version_id, head_b.base.id);
        assert_eq!(r_b.function_version_patch_id, Some(Patch::from(p_b)));
        Ok(())
    }
}
