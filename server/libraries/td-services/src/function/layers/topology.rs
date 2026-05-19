//
// Copyright 2026 Tabs Data Inc.
//

//! Function-version topology helpers.
//!
//! Patches share the topology of their parent by construction
//! (patches don't own `tables` / `dependencies` rows), so resolving
//! "which fvid to run" is purely a head-vs-latest-patch choice.

use std::collections::{HashMap, HashSet};
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::function::{FunctionDB, FunctionPatchDB};
use td_objects::types::basic::{AtTime, FunctionVersionId};
use td_queries::{ChunkedQueries, LiveEntry, VersionedEntry};

/// Resolve a fvid to its canonical head: try `functions` first, fall
/// back to `function_patches`. Returns the parent fvid for patches and
/// the row's own id for canonicals.
pub async fn head_of<'c>(
    conn: &mut DynamicConnection<'c>,
    fvid: &FunctionVersionId,
) -> Result<FunctionVersionId, TdError> {
    if let Some(row) = conn.live().fetch_optional::<FunctionDB>(fvid).await? {
        return Ok(row.id);
    }
    let patch: FunctionPatchDB = conn.live().fetch_one::<FunctionPatchDB>(fvid).await?;
    Ok(patch.parent_function_version_id)
}

/// The pair of ids needed to mint an FR/TDV: the head fvid (FK-valid
/// against `functions`) and an optional patch id (FK-valid against
/// `function_patches`) chosen by the latest-patch-or-head resolver.
#[derive(Debug, Clone)]
pub struct RunnableVersion {
    pub function_version_id: FunctionVersionId,
    pub function_version_patch_id: Option<FunctionVersionId>,
}

impl RunnableVersion {
    /// The id that actually drives the worker's bundle_id/snippet —
    /// patch if set, else head.
    pub fn effective(&self) -> &FunctionVersionId {
        self.function_version_patch_id
            .as_ref()
            .unwrap_or(&self.function_version_id)
    }
}

/// Resolve `fvid` to the head fvid + an optional patch id, pinned to
/// time `at`. Reads `function_patches_current` via the versioned
/// surface: the partition is the head fvid, so the query returns at
/// most one row — the latest `Active` patch with `defined_on <= at`.
/// Plan-build (request time = plan trigger) and replay (request time =
/// replay request) both call this with their own `at`, so patches
/// registered later don't backfill into earlier mints.
pub async fn latest_patch_or_head<'c>(
    conn: &mut DynamicConnection<'c>,
    fvid: &FunctionVersionId,
    at: &AtTime,
) -> Result<RunnableVersion, TdError> {
    let head_fvid = head_of(conn, fvid).await?;
    let latest: Option<FunctionPatchDB> = conn
        .versioned(at)
        .state(FunctionPatchDB::active())
        .fetch_optional(&head_fvid)
        .await?;
    Ok(RunnableVersion {
        function_version_id: head_fvid,
        function_version_patch_id: latest.map(|v| v.id),
    })
}

/// Batched `latest_patch_or_head` for many fvids in one tick, pinned
/// to time `at`. Returns a map keyed by the *input* fvid so callers
/// can look up the runnable for each LFR they're minting.
pub async fn latest_patch_or_head_batch<'c>(
    conn: &mut DynamicConnection<'c>,
    fvids: Vec<FunctionVersionId>,
    at: &AtTime,
) -> Result<HashMap<FunctionVersionId, RunnableVersion>, TdError> {
    if fvids.is_empty() {
        return Ok(HashMap::new());
    }
    // 1. Resolve heads. `functions` covers canonical fvids; any fvid
    //    missing from that result is presumed to be a patch id (rare
    //    on the dispatch path — LFRs are minted with head fvids).
    let canonical: Vec<FunctionDB> = conn
        .fetch_all_chunked::<FunctionDB, FunctionVersionId>(&fvids)
        .await?;
    let mut canonical_by_id: HashMap<FunctionVersionId, FunctionDB> =
        canonical.into_iter().map(|f| (f.id.clone(), f)).collect();
    let unresolved: Vec<FunctionVersionId> = fvids
        .iter()
        .filter(|id| !canonical_by_id.contains_key(*id))
        .cloned()
        .collect();
    let patch_lookups: Vec<FunctionPatchDB> = if unresolved.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<FunctionPatchDB, FunctionVersionId>(&unresolved)
            .await?
    };
    let mut patch_lookup_by_id: HashMap<FunctionVersionId, FunctionPatchDB> = patch_lookups
        .into_iter()
        .map(|p| (p.id.clone(), p))
        .collect();

    let mut head_by_input: HashMap<FunctionVersionId, FunctionVersionId> = HashMap::new();
    for fvid in fvids {
        if let Some(row) = canonical_by_id.remove(&fvid) {
            head_by_input.insert(fvid, row.id);
        } else if let Some(patch) = patch_lookup_by_id.remove(&fvid) {
            head_by_input.insert(fvid, patch.parent_function_version_id);
        }
    }

    // 2. One versioned read against `function_patches_current` keyed
    //    by the distinct head fvids. The partition is
    //    `parent_function_version_id`, so each head fvid returns at
    //    most one row: the latest Active patch with `defined_on <= at`.
    let mut seen: HashSet<&FunctionVersionId> = HashSet::with_capacity(head_by_input.len());
    let head_fvids: Vec<FunctionVersionId> = head_by_input
        .values()
        .filter(|v| seen.insert(v))
        .cloned()
        .collect();
    let latest_patches: Vec<FunctionPatchDB> = if head_fvids.is_empty() {
        Vec::new()
    } else {
        conn.versioned(at)
            .state(FunctionPatchDB::active())
            .fetch_all_chunked(&head_fvids)
            .await?
    };
    let patch_by_head: HashMap<FunctionVersionId, FunctionPatchDB> = latest_patches
        .into_iter()
        .map(|p| (p.parent_function_version_id.clone(), p))
        .collect();

    // 3. Resolve `RunnableVersion` for each input fvid in-memory.
    let mut out = HashMap::with_capacity(head_by_input.len());
    for (input_fvid, head_fvid) in head_by_input {
        let latest = patch_by_head.get(&head_fvid).map(|p| p.id.clone());
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
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::function::{FunctionDB, FunctionPatchDB, FunctionRegister};
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, Description, FunctionRuntimeValues, FunctionStatus,
        Snippet, TableNameDto, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    async fn seed_head(
        db: &TdPool,
        coll_name: &str,
        fn_name: &str,
    ) -> Result<(CollectionDB, FunctionDB), TdError> {
        let coll = db
            .seed()
            .collection(&CollectionName::try_from(coll_name)?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name(fn_name)?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let head = db.seed().function(&coll, &create).await?;
        Ok((coll, head))
    }

    async fn insert_patch(
        db: &TdPool,
        head: &FunctionDB,
        defined_on: AtTime,
        snippet: &str,
    ) -> Result<FunctionVersionId, TdError> {
        let patch = FunctionPatchDB::builder()
            .parent_function_version_id(head.id.clone())
            .function_id(head.function_id)
            .collection_id(head.collection_id)
            .snippet(Snippet::try_from(snippet)?)
            .bundle_id(BundleId::default())
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .description(Description::try_from("d")?)
            .defined_on(defined_on)
            .defined_by_id(UserId::admin())
            .status(FunctionStatus::Active)
            .build()?;
        db.live().insert::<FunctionPatchDB>(&patch).await?;
        Ok(patch.id)
    }

    /// Plan A semantic: resolving a head fvid at time `T1` returns the
    /// patch that was active at `T1`, ignoring any patches registered
    /// after `T1`. Catches the regression where mint reads the freshest
    /// patch wall-clock-now instead of pinning to the request `AtTime`.
    #[td_test::test(db)]
    async fn test_latest_patch_or_head_time_pinning(db: TdPool) -> Result<(), TdError> {
        let (_coll, head) = seed_head(&db, "c0", "f0").await?;

        // Three deterministic points: `before` (epoch — no patches), `t1`
        // (p1 alive), `t2 = t1 + 1ms` (p2 alive).
        let before = AtTime::try_from(td_common::datetime::Timestamp::try_from(0_i64).unwrap())?;
        let t1 = AtTime::now();
        let t2 = AtTime::try_from(td_common::datetime::Timestamp::from(
            **t1 + chrono::Duration::milliseconds(1),
        ))?;
        let p1 = insert_patch(&db, &head, t1, "p1").await?;
        let p2 = insert_patch(&db, &head, t2, "p2").await?;

        let tx = db.begin().await.unwrap();
        let mut conn = tx.into_connection();

        // At T1: p1 exists; p2 does not yet.
        let at_t1 = latest_patch_or_head(&mut conn, &head.id, &t1).await?;
        assert_eq!(at_t1.function_version_id, head.id);
        assert_eq!(at_t1.function_version_patch_id, Some(p1));

        // At T2: p2 is the newest active patch.
        let at_t2 = latest_patch_or_head(&mut conn, &head.id, &t2).await?;
        assert_eq!(at_t2.function_version_patch_id, Some(p2));

        // Before either patch: head only, no patch resolved.
        let at_before = latest_patch_or_head(&mut conn, &head.id, &before).await?;
        assert_eq!(at_before.function_version_patch_id, None);
        Ok(())
    }

    /// Versioned partition isolation: two head fvids each have their
    /// own patch lineage. Resolving one head must never see the other
    /// head's patches. Catches a regression where `partition_by` drifts
    /// from `parent_function_version_id` (or where the batch resolver
    /// crosses partitions).
    #[td_test::test(db)]
    async fn test_latest_patch_or_head_batch_partition_isolation(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (_coll_a, head_a) = seed_head(&db, "c0", "fa").await?;
        let (_coll_b, head_b) = seed_head(&db, "c1", "fb").await?;

        let now = AtTime::now();
        let p_a = insert_patch(&db, &head_a, now, "p-a").await?;
        let p_b = insert_patch(&db, &head_b, now, "p-b").await?;

        let tx = db.begin().await.unwrap();
        let mut conn = tx.into_connection();
        let map =
            latest_patch_or_head_batch(&mut conn, vec![head_a.id.clone(), head_b.id.clone()], &now)
                .await?;

        let r_a = map.get(&head_a.id).expect("missing head_a");
        let r_b = map.get(&head_b.id).expect("missing head_b");
        assert_eq!(r_a.function_version_id, head_a.id);
        assert_eq!(r_a.function_version_patch_id, Some(p_a));
        assert_eq!(r_b.function_version_id, head_b.id);
        assert_eq!(r_b.function_version_patch_id, Some(p_b));
        Ok(())
    }
}
