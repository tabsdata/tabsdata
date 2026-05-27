//
// Copyright 2026 Tabs Data Inc.
//

//! Logical-version resolver — mirrors `VersionResolver` but indexes over
//! the LTDV timeline (`logical_table_data_versions`) rather than the
//! concrete TDV timeline.
//!
//! Plan-build pins logical ids on the dependency-binding rows, so the
//! resolver hands out LTDVs directly. Every concrete TDV has an LTDV by
//! construction: in-plan plan-build mints them eagerly, and the v9→v10
//! eager migration backfilled LTDVs for all pre-existing direct rows.
//!
//! ### Two sources for an LTDV
//!
//! 1. **In-plan** — an LTDV minted earlier in this same pass, not yet
//!    queryable via the DB partition scan. The caller supplies `in_plan`
//!    keyed by `table_id`, which is consulted first for "latest" lookups.
//! 2. **Cross-plan** — the LTDV is in `logical_table_data_versions` from
//!    a prior plan-build (or a v9→v10 mirror row). The
//!    `Version::Fixed(<TDV-id>)` lookup queries
//!    `logical_table_data_version_pointers` for the earliest pointer
//!    event referencing the concrete TDV id and follows it to the LTDV
//!    identity row.
//!
//! ### Versions semantics
//!
//! | Version op            | resolution                                                                  |
//! |-----------------------|-----------------------------------------------------------------------------|
//! | `None`                | latest LTDV at `triggered_on` (in-plan map first, else committed timeline)  |
//! | `Single(Head(0))`     | identical to `None`                                                         |
//! | `Single(Head(-k))`    | (k+1)-th most recent LTDV in the committed timeline                         |
//! | `Single(Initial(k))`  | (k+1)-th oldest LTDV                                                        |
//! | `Single(Fixed(id))`   | LTDV reached via the pointer event log (earliest Bound event for `id`)      |
//! | `Range` / `List`      | composed from the single-version cases (Fixed pre-resolved up front)        |
//! | `New`                 | LTDVs minted after the requesting function's last execution                 |
//!
//! `Version::Fixed(<TDV-id>)` is pre-resolved before calling the
//! data-version machinery: the resolver looks up the LTDV via the pointer
//! event log (earliest event whose `physical_table_data_version_id = <TDV-id>` is
//! the producing/lazy-mint event) and rewrites the bound to a
//! `Head(-offset)` in the LTDV timeline. The `fetch_*_data_versions_at`
//! calls therefore only ever see `Head` / `Initial` bounds —
//! `col_for_type::<PhysicalTableDataVersionId>` on `LogicalTableDataVersionDB`
//! is never invoked. **Do not call `fetch_*_data_versions_at` on LTDV with
//! a `Versions::*(Fixed(_))` shape directly**: the LTDV identity row has
//! no `PhysicalTableDataVersionId` column, and the macro emits a runtime
//! `TypeNotFound` for that resolution path.
//!
//! ### Voided LTDVs are visible to the planner
//!
//! The resolver uses the `All` state filter — Voided LTDVs are returned
//! exactly like Pending and Bound ones. The planner pins whatever is the
//! latest LTDV at `triggered_on` regardless of pointer status; the gate
//! against running a plan with voided inputs lives at dispatch (an LFR
//! cannot flip Pending → Bound while any of its input LTDVs' latest
//! pointer is Voided). This matches A5 / `CancelPropagation::Manual`:
//! operators decide whether to redirect a Voided LTDV to a previous-valid
//! concrete; the planner does not silently substitute older versions.

use derive_builder::Builder;
use itertools::Itertools;
use std::collections::HashMap;
use td_database::dynamic::DynamicConnection;
use td_error::DisplayVec;
use td_error::{TdError, td_error};
use td_objects::dxo::logical::LogicalTableDataVersionDB;
use td_objects::types::basic::{FunctionId, LogicalTableDataVersionId, TableId, TriggeredOn};
use td_objects::types::table_ref::{Version, Versions};
use td_queries::{ChunkedQueries, LfrEntry, LiveEntry, VersionedEntry};

#[td_error]
pub enum VersionResolverError {
    #[error("Could not find the following logical table data versions: [{0}]")]
    FixedTableDataVersionsNotFound(DisplayVec<LogicalTableDataVersionId>) = 0,
    #[error("In a range, the left version must be older than the right version.")]
    InvalidRange(Versions),
}

/// Resolver for logical table-data versions.
///
/// Mirrors the `VersionResolver` API surface. The `in_plan` map carries
/// the LTDVs minted earlier in this same plan-build pass (keyed by
/// `table_id`), which `Versions::None` and `Single(Head(0))` resolution
/// consults before reaching the DB.
#[derive(Builder)]
#[builder(build_fn(error = "TdError"))]
pub struct VersionResolver<'a> {
    function_id: &'a FunctionId,
    table_id: &'a TableId,
    versions: &'a Versions,
    triggered_on: &'a TriggeredOn,
    #[builder(default = "false")]
    error_on_desc_range: bool,
    #[builder(default)]
    in_plan: Option<&'a HashMap<TableId, LogicalTableDataVersionDB>>,
}

impl<'a> VersionResolver<'a> {
    pub fn builder() -> VersionResolverBuilder<'a> {
        VersionResolverBuilder::default()
    }

    /// Resolves the requested versions to LTDV rows.
    ///
    /// Returns one entry per requested version; the entry is `None` when
    /// the requested coordinate doesn't exist (e.g. an `Initial(0)` on an
    /// empty timeline). For `Versions::None` and `Single(Head(0))`, the
    /// in-plan map is consulted first.
    pub async fn resolve(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<Vec<Option<LogicalTableDataVersionDB>>, TdError> {
        let table_id = self.table_id;
        let triggered_on = self.triggered_on;
        let in_plan_latest = self.in_plan.and_then(|m| m.get(table_id).cloned());

        let resolved = match self.versions {
            Versions::None | Versions::Single(Version::Head(0)) => {
                if let Some(ltdv) = in_plan_latest {
                    return Ok(vec![Some(ltdv)]);
                }
                let v = conn
                    .versioned(triggered_on)
                    .data_version(table_id)
                    .fetch_optional::<LogicalTableDataVersionDB>(self.versions)
                    .await?;
                vec![v]
            }
            Versions::Single(Version::Head(_) | Version::Initial(_)) => {
                let v = conn
                    .versioned(triggered_on)
                    .data_version(table_id)
                    .fetch_optional::<LogicalTableDataVersionDB>(self.versions)
                    .await?;
                vec![v]
            }
            Versions::Single(Version::Fixed(ltdv_id)) => {
                let ltdv = self.fetch_ftdv_for_fixed(conn, ltdv_id).await?;
                vec![Some(ltdv)]
            }
            Versions::List(versions) => self.resolve_list(conn, versions).await?,
            Versions::Range(_, _) => self.resolve_range(conn).await?,
            Versions::New => self.resolve_new(conn).await?,
        };

        Ok(resolved)
    }

    /// Fetch the LTDV identity row for a `Version::Fixed(ltdv_id)`. The
    /// id IS the logical identity now, so this is a direct lookup — no
    /// pointer-log traversal. Reports a domain error when the LTDV
    /// doesn't exist so callers (e.g. register-time validation) can match
    /// on it.
    async fn fetch_ftdv_for_fixed(
        &self,
        conn: &mut DynamicConnection<'_>,
        ltdv_id: &LogicalTableDataVersionId,
    ) -> Result<LogicalTableDataVersionDB, TdError> {
        let ltdv = conn
            .live()
            .fetch_optional::<LogicalTableDataVersionDB>(ltdv_id)
            .await?
            .ok_or_else(|| {
                VersionResolverError::FixedTableDataVersionsNotFound(vec![ltdv_id.clone()].into())
            })?;
        Ok(ltdv)
    }

    /// Resolve a Fixed LTDV id to its `Head(-offset)` position in the
    /// partition's timeline at `self.triggered_on`. Indexed lookup via the
    /// ranked CTE — no full timeline materialization.
    async fn fixed_to_head_offset(
        &self,
        conn: &mut DynamicConnection<'_>,
        ltdv_id: &LogicalTableDataVersionId,
    ) -> Result<Version, TdError> {
        let rn = conn
            .versioned(self.triggered_on)
            .data_version(self.table_id)
            .fetch_relative_offset::<LogicalTableDataVersionDB>(&Version::Fixed(ltdv_id.clone()))
            .await?
            .ok_or_else(|| {
                VersionResolverError::FixedTableDataVersionsNotFound(vec![ltdv_id.clone()].into())
            })?;
        Ok(Version::Head(-((rn - 1) as isize)))
    }

    /// `Versions::List` resolution.
    ///
    /// Three-bucket structure (fixed / head / initial), preserving the
    /// original index order in the returned Vec. Fixed entries resolve
    /// via the TDV back-pointer; Head/Initial via the data-version
    /// machinery on the substituted (Fixed-free) bounds.
    async fn resolve_list(
        &self,
        conn: &mut DynamicConnection<'_>,
        versions: &[Version],
    ) -> Result<Vec<Option<LogicalTableDataVersionDB>>, TdError> {
        let table_id = self.table_id;
        let triggered_on = self.triggered_on;

        let (fixed_versions, head_versions, initial_versions) =
            versions.iter().cloned().enumerate().fold(
                (HashMap::new(), HashMap::new(), HashMap::new()),
                |(mut fixed, mut head, mut initial), (index, version)| {
                    match version {
                        Version::Fixed(_) => fixed.insert(index, version),
                        Version::Head(_) => head.insert(index, version),
                        Version::Initial(_) => initial.insert(index, version),
                    };
                    (fixed, head, initial)
                },
            );

        // Fixed entries: one batched fetch by LTDV id list.
        let fixed_versions = if !fixed_versions.is_empty() {
            let ids: Vec<LogicalTableDataVersionId> = fixed_versions
                .values()
                .map(|v| match v {
                    Version::Fixed(id) => id.clone(),
                    _ => unreachable!(),
                })
                .collect();
            let by_id: HashMap<LogicalTableDataVersionId, LogicalTableDataVersionDB> = conn
                .fetch_all_chunked::<LogicalTableDataVersionDB, LogicalTableDataVersionId>(&ids)
                .await?
                .into_iter()
                .map(|f| (f.id.clone(), f))
                .collect();
            let mut out = HashMap::new();
            let mut not_found = Vec::new();
            for (i, v) in fixed_versions {
                let id = match v {
                    Version::Fixed(id) => id,
                    _ => unreachable!(),
                };
                match by_id.get(&id) {
                    Some(ltdv) => {
                        out.insert(i, Some(ltdv.clone()));
                    }
                    None => not_found.push(id),
                }
            }
            if !not_found.is_empty() {
                Err(VersionResolverError::FixedTableDataVersionsNotFound(
                    not_found.into(),
                ))?
            }
            out
        } else {
            HashMap::new()
        };

        // Head and Initial entries: fetch the min..=max range as a
        // contiguous slice so we can map by relative position.
        let mut find_min_max_range = async |versions: HashMap<usize, Version>, reverse: bool| {
            let minmax = versions
                .values()
                .minmax()
                .into_option()
                .map(|(min, max)| (min.clone(), max.clone()));

            let resolved = if let Some((min, max)) = minmax {
                let mut found = conn
                    .versioned(triggered_on)
                    .data_version(table_id)
                    .fetch_all::<LogicalTableDataVersionDB>(&Versions::Range(min, max))
                    .await?;
                if reverse {
                    found.reverse();
                }
                let map: HashMap<_, _> = versions
                    .iter()
                    .map(|(i, v)| {
                        let v = match v {
                            Version::Head(back) => found.get(-back as usize).cloned(),
                            Version::Initial(forward) => found.get(*forward as usize).cloned(),
                            _ => unreachable!(),
                        };
                        (*i, v)
                    })
                    .collect();
                map
            } else {
                HashMap::new()
            };
            Ok::<_, TdError>(resolved)
        };
        let head_versions = find_min_max_range(head_versions, true).await?;
        let initial_versions = find_min_max_range(initial_versions, false).await?;

        // Merge by original index.
        let sorted: Vec<_> = fixed_versions
            .into_iter()
            .chain(head_versions)
            .chain(initial_versions)
            .collect::<HashMap<_, _>>()
            .into_iter()
            .sorted_by_key(|&(index, _)| index)
            .map(|(_, version)| version)
            .collect();
        Ok(sorted)
    }

    /// `Versions::Range` resolution.
    ///
    /// Convert any Fixed boundary to a Head offset via an indexed lookup
    /// on the LTDV timeline, then fetch the contiguous range, padding with
    /// `None` if the underlying timeline is shorter than the requested
    /// range.
    async fn resolve_range(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<Vec<Option<LogicalTableDataVersionDB>>, TdError> {
        let table_id = self.table_id;
        let triggered_on = self.triggered_on;
        let (from, to) = match self.versions {
            Versions::Range(f, t) => (f, t),
            _ => unreachable!(),
        };

        let relative_from = match from {
            Version::Fixed(id) => self.fixed_to_head_offset(conn, id).await?,
            _ => from.clone(),
        };
        let relative_to = match to {
            Version::Fixed(id) => self.fixed_to_head_offset(conn, id).await?,
            _ => to.clone(),
        };

        // Detect descending ranges (e.g. Head(-3)..Head(-5)). Default
        // behaviour is "best-effort" — return the empty list. Callers
        // doing register-time validation flip `error_on_desc_range(true)`
        // to surface this as a hard error instead.
        let is_desc_range = match (&relative_from, &relative_to) {
            (Version::Head(f), Version::Head(t)) => f > t,
            (Version::Initial(f), Version::Initial(t)) => f > t,
            _ => false,
        };
        if is_desc_range {
            if self.error_on_desc_range {
                Err(VersionResolverError::InvalidRange(self.versions.clone()))?
            }
            return Ok(vec![]);
        }

        let substituted = Versions::Range(relative_from.clone(), relative_to.clone());
        let mut found = conn
            .versioned(triggered_on)
            .data_version(table_id)
            .fetch_all::<LogicalTableDataVersionDB>(&substituted)
            .await?
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();

        // Pad with None when the underlying timeline is shorter than the
        // requested range — only meaningful for same-side bounds.
        let found_size = found.len();
        match (&relative_from, &relative_to) {
            (Version::Head(f), Version::Head(t)) => {
                let range_size = (t - f).unsigned_abs() + 1;
                if range_size > found_size {
                    found.resize(range_size, None);
                    found.rotate_right(range_size - found_size);
                }
            }
            (Version::Initial(f), Version::Initial(t)) => {
                let range_size = (t - f).unsigned_abs() + 1;
                if range_size > found_size {
                    found.resize(range_size, None);
                }
            }
            _ => {
                // Mixed Head/Initial bounds don't define a deterministic
                // range size; leave the result as-is.
            }
        }
        Ok(found)
    }

    /// `Versions::New`: all LTDVs minted after the requesting function's
    /// previous run. Cutoff is the prior LFR for `self.function_id` —
    /// LFRs partition on the function actually running, while `executions`
    /// partitions on the plan's trigger function, so only the LFR source
    /// surfaces a downstream consumer's prior runs.
    async fn resolve_new(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<Vec<Option<LogicalTableDataVersionDB>>, TdError> {
        let table_id = self.table_id;
        let triggered_on = self.triggered_on;

        let last_lfr = conn
            .lfr()
            .previous_for_function_at(self.function_id, triggered_on)
            .await?;

        let last_logical = match last_lfr {
            Some(last) => {
                conn.versioned(&last.triggered_on)
                    .data_version(table_id)
                    .fetch_optional::<LogicalTableDataVersionDB>(&Versions::None)
                    .await?
            }
            None => None,
        };

        let resolved = match last_logical {
            Some(boundary) => {
                // Strictly-newer-than boundary: take the boundary's Head
                // offset, then range from one-newer to Head(0).
                let boundary_offset = self.fixed_to_head_offset(conn, &boundary.id).await?;
                let boundary_pos = match boundary_offset {
                    Version::Head(k) => -k as usize,
                    _ => unreachable!(),
                };
                if boundary_pos == 0 {
                    // Boundary is HEAD itself — nothing strictly newer.
                    Vec::new()
                } else {
                    conn.versioned(triggered_on)
                        .data_version(table_id)
                        .fetch_all::<LogicalTableDataVersionDB>(&Versions::Range(
                            Version::Head(-((boundary_pos - 1) as isize)),
                            Version::Head(0),
                        ))
                        .await?
                        .into_iter()
                        .map(Some)
                        .collect::<Vec<_>>()
                }
            }
            None => conn
                .versioned(triggered_on)
                .data_version(table_id)
                .fetch_all::<LogicalTableDataVersionDB>(&Versions::Range(
                    Version::Initial(0),
                    Version::Head(0),
                ))
                .await?
                .into_iter()
                .map(Some)
                .collect(),
        };
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::pool::TdPool;
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::logical::LogicalFunctionRunDB;
    use td_objects::dxo::logical::LogicalPointerDao;
    use td_objects::dxo::logical::LogicalTableDataVersionPointerDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::LogicalFunctionRunStatus;
    use td_objects::types::basic::Trigger;
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, Decorator, ExecutionId, LogicalPointerReason,
        LogicalPointerStatus, PhysicalTableDataVersionId, TableName, TableNameDto, TransactionId,
        TransactionKey, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_queries::{CurrentEntry, LtdvEntry};

    /// Insert an LTDV identity row plus a `Bound` `InitialMaterialization`
    /// pointer event (mirroring lazy-mint, but without back-pointer side
    /// effects on the concrete TDV — these tests don't seed concrete TDVs).
    async fn insert_ftdv_bound(
        db: &TdPool,
        ltdv: &LogicalTableDataVersionDB,
        bound_to: &PhysicalTableDataVersionId,
        triggered_by_id: &UserId,
    ) {
        let tx = db.begin().await.unwrap();
        let mut conn = tx.into_connection();
        conn.live()
            .insert::<LogicalTableDataVersionDB>(ltdv)
            .await
            .unwrap();
        let pointer = LogicalTableDataVersionPointerDB::builder()
            .logical_table_data_version_id(ltdv.id.clone())
            .status(LogicalPointerStatus::Bound)
            .physical_table_data_version_id(Some(bound_to.clone()))
            .set_on(AtTime::now())
            .set_by_id(*triggered_by_id)
            .reason(LogicalPointerReason::InitialMaterialization)
            .build()
            .unwrap();
        conn.live()
            .insert::<LogicalTableDataVersionPointerDB>(&pointer)
            .await
            .unwrap();
        conn.into_transaction().unwrap().commit().await.unwrap();
    }

    /// Seed a collection / function / single table, then insert `n` LTDVs
    /// directly into the logic table. Each LTDV's pointer event log
    /// gets one Bound entry with a fresh synthetic `PhysicalTableDataVersionId`.
    ///
    /// Returns the table_id and the LTDVs in chronological order.
    async fn seed_ftdv_timeline(
        db: &TdPool,
        n: usize,
    ) -> (
        CollectionDB,
        FunctionDB,
        TableId,
        Vec<LogicalTableDataVersionDB>,
    ) {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
            .await
            .unwrap();

        let table_name = TableNameDto::try_from("t0").unwrap();
        let create = FunctionRegister::builder()
            .try_name("f0")
            .unwrap()
            .try_description("d")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("s")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![table_name.clone()])
            .try_runtime_values("rt")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();
        let function = db.seed().function(&collection, &create).await.unwrap();

        let table_name_typed = TableName::try_from(&table_name).unwrap();
        let table = db
            .current()
            .fetch_one::<TableDB>(and!(&collection.id, &table_name_typed))
            .await
            .unwrap();

        let mut ltdvs = Vec::with_capacity(n);
        for _ in 0..n {
            let execution = db.seed().execution(&function).await.unwrap();
            let transaction = db
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
                .await
                .unwrap();

            let synthetic_tdv_id = PhysicalTableDataVersionId::new();
            let ltdv = LogicalTableDataVersionDB::builder()
                .collection_id(collection.id)
                .execution_id(execution.id.clone())
                .transaction_id(transaction.id.clone())
                .table_id(table.table_id)
                .producer_logical_function_run_id(None)
                .function_param_pos(None)
                .triggered_on(execution.triggered_on)
                .triggered_by_id(execution.triggered_by_id)
                .build()
                .unwrap();

            insert_ftdv_bound(db, &ltdv, &synthetic_tdv_id, &execution.triggered_by_id).await;
            ltdvs.push(ltdv);
        }
        (collection, function, table.table_id, ltdvs)
    }

    /// `Versions::None` returns the latest LTDV at `triggered_on`.
    #[td_test::test(db)]
    async fn test_resolve_logical_none(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
        let triggered_on = TriggeredOn::now();

        let mut conn = db.acquire().await.unwrap();
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::None)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
        Ok(())
    }

    /// `Versions::None` consults the in-plan map first when present.
    #[td_test::test(db)]
    async fn test_resolve_logical_none_in_plan_overrides(db: TdPool) -> Result<(), TdError> {
        let (collection, _function, table_id, _ftdvs) = seed_ftdv_timeline(&db, 1).await;

        // Build an in-plan LTDV that is NOT yet inserted in the DB; the
        // resolver should still hand it back for `None` lookups.
        let in_plan_ftdv = LogicalTableDataVersionDB::builder()
            .collection_id(collection.id)
            .execution_id(ExecutionId::new())
            .transaction_id(TransactionId::new())
            .table_id(table_id)
            .producer_logical_function_run_id(None)
            .function_param_pos(None)
            .triggered_on(TriggeredOn::now())
            .triggered_by_id(UserId::admin())
            .build()
            .unwrap();
        let in_plan = HashMap::from([(table_id, in_plan_ftdv.clone())]);

        let mut conn = db.acquire().await.unwrap();
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::None)
            .triggered_on(&TriggeredOn::now())
            .in_plan(Some(&in_plan))
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].as_ref().unwrap().id, in_plan_ftdv.id);
        Ok(())
    }

    /// `Versions::Single(Head)` walks back from the most recent LTDV.
    #[td_test::test(db)]
    async fn test_resolve_logical_single_head(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let cases = [
            (Version::Head(0), Some(&ltdvs[2])),
            (Version::Head(-1), Some(&ltdvs[1])),
            (Version::Head(-2), Some(&ltdvs[0])),
            (Version::Head(-3), None),
        ];
        for (version, expected) in cases {
            let found = VersionResolver::builder()
                .function_id(&FunctionId::default())
                .table_id(&table_id)
                .versions(&Versions::Single(version))
                .triggered_on(&TriggeredOn::now())
                .build()?
                .resolve(&mut conn)
                .await?;
            assert_eq!(found.len(), 1);
            match expected {
                Some(ltdv) => assert_eq!(found[0].as_ref().unwrap().id, ltdv.id),
                None => assert!(found[0].is_none()),
            }
        }
        Ok(())
    }

    /// `Versions::Single(Initial)` walks forward from the oldest LTDV.
    #[td_test::test(db)]
    async fn test_resolve_logical_single_initial(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let cases = [
            (Version::Initial(0), Some(&ltdvs[0])),
            (Version::Initial(1), Some(&ltdvs[1])),
            (Version::Initial(2), Some(&ltdvs[2])),
            (Version::Initial(3), None),
        ];
        for (version, expected) in cases {
            let found = VersionResolver::builder()
                .function_id(&FunctionId::default())
                .table_id(&table_id)
                .versions(&Versions::Single(version))
                .triggered_on(&TriggeredOn::now())
                .build()?
                .resolve(&mut conn)
                .await?;
            assert_eq!(found.len(), 1);
            match expected {
                Some(ltdv) => assert_eq!(found[0].as_ref().unwrap().id, ltdv.id),
                None => assert!(found[0].is_none()),
            }
        }
        Ok(())
    }

    /// `Versions::List` mixing Head + Initial preserves index order and
    /// returns `None` for out-of-range slots.
    #[td_test::test(db)]
    async fn test_resolve_logical_list_relative(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;
        let mut conn = db.acquire().await.unwrap();

        let versions = Versions::List(vec![
            Version::Head(0),
            Version::Head(-1),
            Version::Head(-2),
            Version::Initial(0),
            Version::Initial(1),
            Version::Initial(2),
        ]);
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 6);
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[1].id);
        assert_eq!(found[1].as_ref().unwrap().id, ltdvs[0].id);
        assert!(found[2].is_none());
        assert_eq!(found[3].as_ref().unwrap().id, ltdvs[0].id);
        assert_eq!(found[4].as_ref().unwrap().id, ltdvs[1].id);
        assert!(found[5].is_none());
        Ok(())
    }

    /// `Versions::Range` with same-side bounds returns the contiguous slice
    /// in chronological order, padded with `None` when shorter than asked.
    #[td_test::test(db)]
    async fn test_resolve_logical_range_head(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let versions = Versions::Range(Version::Head(-2), Version::Head(0));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
        assert_eq!(found[1].as_ref().unwrap().id, ltdvs[1].id);
        assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
        Ok(())
    }

    /// `Versions::Range` with descending bounds returns an empty Vec rather
    /// than erroring (best-effort semantics).
    #[td_test::test(db)]
    async fn test_resolve_logical_range_descending(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, _ftdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let versions = Versions::Range(Version::Head(-1), Version::Head(-3));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert!(found.is_empty());
        Ok(())
    }

    /// `Versions::Range(Initial, Initial)` walks forward from the oldest
    /// LTDV in chronological order.
    #[td_test::test(db)]
    async fn test_resolve_logical_range_initial(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let versions = Versions::Range(Version::Initial(0), Version::Initial(2));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
        assert_eq!(found[1].as_ref().unwrap().id, ltdvs[1].id);
        assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
        Ok(())
    }

    /// `Versions::Range(Initial, Head)` resolves to all LTDVs in the
    /// timeline. Mixed-side bounds don't define a deterministic range size,
    /// so the resolver returns the contiguous slice as-is without padding.
    #[td_test::test(db)]
    async fn test_resolve_logical_range_mixed_bounds(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let versions = Versions::Range(Version::Initial(0), Version::Head(0));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
        assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
        Ok(())
    }

    /// `Versions::New` resolves to the LTDVs minted after the requesting
    /// function's previous execution. Exercises the "no prior execution"
    /// branch — using a fresh `function_id` not tied to any seeded
    /// execution falls through to `Versions::Range(Initial(0), Head(0))`,
    /// returning every LTDV in the timeline.
    #[td_test::test(db)]
    async fn test_resolve_logical_new_no_prior_execution(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = db.acquire().await.unwrap();

        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::New)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
        assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
        Ok(())
    }

    /// `Versions::New` for a downstream consumer (never the plan's
    /// trigger) returns just the delta since its prior LFR — not the
    /// whole timeline. Setup: publisher mints v1, v2 in two
    /// publisher-rooted executions; consumer has an LFR aligned with
    /// run 1's execution; resolving for the consumer must yield {v2}.
    #[td_test::test(db)]
    async fn test_resolve_logical_new_downstream_consumer(db: TdPool) -> Result<(), TdError> {
        let (collection, _publisher, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;

        // Decorator is irrelevant — we only need a distinct function_id.
        let consumer_create = FunctionRegister::builder()
            .try_name("c0")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("c0_out")?])
            .try_runtime_values("rt")?
            .reuse_frozen_tables(false)
            .build()?;
        let consumer = db.seed().function(&collection, &consumer_create).await?;

        // Consumer LFR aligned with run 1 — the only cutoff a correct
        // resolver could use.
        let lfr_run1 = LogicalFunctionRunDB::builder()
            .collection_id(collection.id)
            .execution_id(&ltdvs[0].execution_id)
            .transaction_id(&ltdvs[0].transaction_id)
            .function_id(consumer.function_id)
            .function_version_id(&consumer.id)
            .trigger(Trigger::Dependency)
            .triggered_on(ltdvs[0].triggered_on)
            .triggered_by_id(UserId::admin())
            .status(LogicalFunctionRunStatus::Committed)
            .build()?;
        db.live().insert::<LogicalFunctionRunDB>(&lfr_run1).await?;

        let mut conn = db.acquire().await.unwrap();
        let found = VersionResolver::builder()
            .function_id(&consumer.function_id)
            .table_id(&table_id)
            .versions(&Versions::New)
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;

        let found_ids: Vec<_> = found
            .iter()
            .map(|f| f.as_ref().map(|f| f.id.clone()))
            .collect();
        assert_eq!(
            found.len(),
            1,
            "downstream consumer should see only the LTDV minted after its prior run, got {found_ids:?}",
        );
        assert_eq!(found[0].as_ref().unwrap().id, ltdvs[1].id);
        Ok(())
    }

    /// LTDVs partition by `table_id`: a resolver call against table A must
    /// not surface LTDVs minted for table B. Guards against partition
    /// scope leaks.
    #[td_test::test(db)]
    async fn test_resolve_logical_partition_isolation(db: TdPool) -> Result<(), TdError> {
        let (collection, function, table_a_id, ftdvs_a) = seed_ftdv_timeline(&db, 2).await;

        // Mint two LTDVs for an unrelated table B in the same collection.
        let table_b_name = TableNameDto::try_from("table_b")?;
        let table_b_create = FunctionRegister::builder()
            .try_name("f_b")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![table_b_name.clone()])
            .try_runtime_values("rt")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_b = db.seed().function(&collection, &table_b_create).await?;
        let table_b_typed = TableName::try_from(&table_b_name)?;
        let table_b = db
            .current()
            .fetch_one::<TableDB>(and!(&collection.id, &table_b_typed))
            .await?;
        for _ in 0..2 {
            let execution = db.seed().execution(&function_b).await?;
            let transaction = db
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY")?)
                .await?;
            let ltdv = LogicalTableDataVersionDB::builder()
                .collection_id(collection.id)
                .execution_id(execution.id.clone())
                .transaction_id(transaction.id.clone())
                .table_id(table_b.table_id)
                .producer_logical_function_run_id(None)
                .function_param_pos(None)
                .triggered_on(execution.triggered_on)
                .triggered_by_id(execution.triggered_by_id)
                .build()?;
            insert_ftdv_bound(
                &db,
                &ltdv,
                &PhysicalTableDataVersionId::new(),
                &execution.triggered_by_id,
            )
            .await;
        }
        // function_b avoids the unused warning while documenting the seed
        // path; we don't reference it again.
        let _ = function;
        let _ = function_b;

        // Resolve the full timeline for table A — only A's LTDVs come back.
        let mut conn = db.acquire().await.unwrap();
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_a_id)
            .versions(&Versions::Range(Version::Initial(0), Version::Head(0)))
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 2);
        let ids: Vec<_> = found
            .iter()
            .map(|f| f.as_ref().unwrap().id.clone())
            .collect();
        assert!(ids.contains(&ftdvs_a[0].id));
        assert!(ids.contains(&ftdvs_a[1].id));
        for ltdv in found.iter().flatten() {
            assert_eq!(ltdv.table_id, table_a_id);
        }
        Ok(())
    }

    /// Voided LTDVs remain visible to the resolver. The planner pins
    /// whatever is the latest at `triggered_on` regardless of pointer
    /// status; the gate against running on Voided inputs lives at dispatch.
    #[td_test::test(db)]
    async fn test_resolve_logical_voided_visible(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;

        // Void the most recent LTDV by directly appending a Voided
        // pointer event.
        let tx = db.begin().await.unwrap();
        let mut conn = tx.into_connection();
        let void_event = LogicalTableDataVersionPointerDB::build_event(
            &ltdvs[1].id,
            LogicalPointerStatus::Voided,
            None,
            AtTime::now(),
            UserId::admin(),
            LogicalPointerReason::OperatorForceVoid,
        )?;
        conn.live()
            .insert::<LogicalTableDataVersionPointerDB>(&void_event)
            .await?;
        conn.into_transaction().unwrap().commit().await.unwrap();

        // `Head(0)` still returns the voided LTDV's identity row.
        let mut conn = db.acquire().await.unwrap();
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::Single(Version::Head(0)))
            .triggered_on(&TriggeredOn::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        let ltdv = found[0].as_ref().unwrap();
        assert_eq!(ltdv.id, ltdvs[1].id);
        // Confirm the latest pointer event for this LTDV is Voided (no
        // concrete TDV id reachable via the temporal view).
        let latest = conn.ltdv().pointers().concrete(&ltdv.id).await?;
        assert!(latest.is_none());
        Ok(())
    }
}
