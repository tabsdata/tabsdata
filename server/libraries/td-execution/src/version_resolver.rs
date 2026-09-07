//
// Copyright 2026 Tabsdata Inc.
//

//! Logical-version resolver over the LTDV timeline
//! (`logical_table_data_versions`). Plan-build pins logical ids on the
//! dependency-binding rows, so the resolver hands out LTDVs directly.
//! Same-plan minted LTDVs are persisted before inputs are resolved, so
//! they resolve through the same queries — there is no in-memory source.
//!
//! ### Versions semantics
//!
//! | Version op            | resolution                                                                  |
//! |-----------------------|-----------------------------------------------------------------------------|
//! | `None`                | latest LTDV at `triggered_on` in the timeline                               |
//! | `Single(Head(0))`     | identical to `None`                                                         |
//! | `Single(Head(-k))`    | (k+1)-th most recent LTDV in the committed timeline                         |
//! | `Single(Initial(k))`  | (k+1)-th oldest LTDV                                                        |
//! | `Single(Fixed(id))`   | the LTDV identity row for `id` (active view)                                |
//! | `Range` / `List`      | composed from the single-version cases                                      |
//! | `New`                 | LTDVs minted after the requesting function's last execution                 |
//!
//! ### Voided LTDVs are visible; Canceled/Yanked-produced ones disappear
//!
//! Voided is a pointer-layer state and stays visible: the planner pins the
//! latest LTDV regardless of pointer status, and the dispatch gate handles
//! voided inputs. A Canceled/Yanked *producer* produced no data, so its
//! LTDV disappears: all reads go through
//! `logical_table_data_versions__active`, so `Head(0)` falls back to the
//! previous live version and a `Fixed` pin to such an LTDV resolves to
//! `FixedTableDataVersionsNotFound`.

use td_database::access::SeesProjectRows;

use derive_builder::Builder;
use itertools::Itertools;
use std::collections::HashMap;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::DisplayVec;
use td_error::{TdError, td_error};
use td_objects::dxo::logical::ActiveLogicalTableDataVersionDB;
use td_objects::types::basic::{AtTime, FunctionId, LogicalTableDataVersionId, TableId};
use td_objects::types::table_ref::{Version, Versions};
use td_queries::Queries;

#[td_error]
pub enum VersionResolverError {
    #[error("Could not find the following logical table data versions: [{0}]")]
    FixedTableDataVersionsNotFound(DisplayVec<LogicalTableDataVersionId>) = 0,
    #[error("In a range, the left version must be older than the right version.")]
    InvalidRange(Versions),
}

#[derive(Builder)]
#[builder(build_fn(error = "TdError"))]
pub struct VersionResolver<'a> {
    function_id: &'a FunctionId,
    table_id: &'a TableId,
    versions: &'a Versions,
    triggered_on: &'a AtTime,
    #[builder(default = "false")]
    error_on_desc_range: bool,
}

impl<'a> VersionResolver<'a> {
    pub fn builder() -> VersionResolverBuilder<'a> {
        VersionResolverBuilder::default()
    }

    /// Resolves the requested versions to LTDV rows.
    ///
    /// Returns one entry per requested version; the entry is `None` when
    /// the requested coordinate doesn't exist (e.g. an `Initial(0)` on an
    /// empty timeline).
    pub async fn resolve<S: SeesProjectRows>(
        &self,
        conn: &mut DomainConnection<Apiserver, S>,
    ) -> Result<Vec<Option<ActiveLogicalTableDataVersionDB>>, TdError> {
        let table_id = self.table_id;
        let triggered_on = self.triggered_on;

        let resolved = match self.versions {
            Versions::None | Versions::Single(Version::Head(0)) => {
                let v = conn
                    .timeline()
                    .at(triggered_on, table_id)
                    .fetch_optional::<ActiveLogicalTableDataVersionDB>(self.versions)
                    .await?;
                vec![v]
            }
            Versions::Single(Version::Head(_) | Version::Initial(_)) => {
                let v = conn
                    .timeline()
                    .at(triggered_on, table_id)
                    .fetch_optional::<ActiveLogicalTableDataVersionDB>(self.versions)
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

    /// Reads the active view, so a pin to a Canceled/Yanked-produced LTDV
    /// (or a missing id) yields the not-found domain error — callers like
    /// register-time validation match on it.
    async fn fetch_ftdv_for_fixed<S: SeesProjectRows>(
        &self,
        conn: &mut DomainConnection<Apiserver, S>,
        ltdv_id: &LogicalTableDataVersionId,
    ) -> Result<ActiveLogicalTableDataVersionDB, TdError> {
        let ltdv = conn
            .timeline()
            .fetch_optional::<ActiveLogicalTableDataVersionDB>(ltdv_id)
            .await?
            .ok_or_else(|| {
                VersionResolverError::FixedTableDataVersionsNotFound(vec![ltdv_id.clone()].into())
            })?;
        Ok(ltdv)
    }

    /// Resolve a Fixed LTDV id to its `Head(-offset)` position in the
    /// partition's timeline at `self.triggered_on`.
    async fn fixed_to_head_offset<S: SeesProjectRows>(
        &self,
        conn: &mut DomainConnection<Apiserver, S>,
        ltdv_id: &LogicalTableDataVersionId,
    ) -> Result<Version, TdError> {
        let rn = conn
            .timeline()
            .at(self.triggered_on, self.table_id)
            .fetch_relative_offset::<ActiveLogicalTableDataVersionDB>(&Version::Fixed(
                ltdv_id.clone(),
            ))
            .await?
            .ok_or_else(|| {
                VersionResolverError::FixedTableDataVersionsNotFound(vec![ltdv_id.clone()].into())
            })?;
        Ok(Version::Head(-((rn - 1) as isize)))
    }

    async fn resolve_list<S: SeesProjectRows>(
        &self,
        conn: &mut DomainConnection<Apiserver, S>,
        versions: &[Version],
    ) -> Result<Vec<Option<ActiveLogicalTableDataVersionDB>>, TdError> {
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

        let fixed_versions = if !fixed_versions.is_empty() {
            let ids: Vec<LogicalTableDataVersionId> = fixed_versions
                .values()
                .map(|v| match v {
                    Version::Fixed(id) => id.clone(),
                    _ => unreachable!(),
                })
                .collect();
            let by_id: HashMap<_, _> = conn
                .timeline()
                .chunked()
                .fetch_all::<ActiveLogicalTableDataVersionDB>(&ids, ())
                .await?
                .into_iter()
                .map(|f| (f.base.base.id.clone(), f))
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

        let mut find_min_max_range = async |versions: HashMap<usize, Version>, reverse: bool| {
            let minmax = versions
                .values()
                .minmax()
                .into_option()
                .map(|(min, max)| (min.clone(), max.clone()));

            let resolved = if let Some((min, max)) = minmax {
                let mut found: Vec<ActiveLogicalTableDataVersionDB> = conn
                    .timeline()
                    .at(triggered_on, table_id)
                    .fetch_all::<ActiveLogicalTableDataVersionDB>(&Versions::Range(min, max))
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

    /// Fixed boundaries convert to Head offsets first; the result is padded
    /// with `None` when the timeline is shorter than the requested range.
    async fn resolve_range<S: SeesProjectRows>(
        &self,
        conn: &mut DomainConnection<Apiserver, S>,
    ) -> Result<Vec<Option<ActiveLogicalTableDataVersionDB>>, TdError> {
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

        // Descending ranges are best-effort (empty list) unless the caller
        // opted into `error_on_desc_range` (register-time validation).
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
            .timeline()
            .at(triggered_on, table_id)
            .fetch_all::<ActiveLogicalTableDataVersionDB>(&substituted)
            .await?
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();

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
    async fn resolve_new<S: SeesProjectRows>(
        &self,
        conn: &mut DomainConnection<Apiserver, S>,
    ) -> Result<Vec<Option<ActiveLogicalTableDataVersionDB>>, TdError> {
        let table_id = self.table_id;
        let triggered_on = self.triggered_on;

        let last_lfr = conn
            .logical_runs()
            .previous_for_function_at(self.function_id, triggered_on)
            .await?;

        let last_logical = match last_lfr {
            Some(last) => {
                conn.timeline()
                    .at(&last.base.triggered_on, table_id)
                    .fetch_optional::<ActiveLogicalTableDataVersionDB>(&Versions::None)
                    .await?
            }
            None => None,
        };

        let resolved = match last_logical {
            Some(boundary) => {
                // Strictly newer than the boundary: range from one-newer to Head(0).
                let boundary_offset = self
                    .fixed_to_head_offset(conn, &boundary.base.base.id)
                    .await?;
                let boundary_pos = match boundary_offset {
                    Version::Head(k) => -k as usize,
                    _ => unreachable!(),
                };
                if boundary_pos == 0 {
                    Vec::new()
                } else {
                    conn.timeline()
                        .at(triggered_on, table_id)
                        .fetch_all::<ActiveLogicalTableDataVersionDB>(&Versions::Range(
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
                .timeline()
                .at(triggered_on, table_id)
                .fetch_all::<ActiveLogicalTableDataVersionDB>(&Versions::Range(
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
    use td_database::access::InProject;
    use td_database::pool::TdPool;
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::types::basic::{AtTime, TriggeredOn};

    use td_objects::dxo::logical::LogicalFunctionRunInsertDB;

    use td_objects::dxo::logical::LogicalTableDataVersionInsertDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::LogicalFunctionRunStatus;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::Trigger;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, LogicalFunctionRunId, Private, ProjectId, TableName,
        TableNameDto, TransactionKey, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    /// Insert an LTDV identity row. The resolver works over the LTDV
    /// timeline alone, so these tests seed no materializations.
    async fn insert_ftdv(db: &TdPool, project: &ProjectId, ltdv: &LogicalTableDataVersionInsertDB) {
        db.scoped(*project)
            .insert::<LogicalTableDataVersionInsertDB>(ltdv)
            .await
            .unwrap();
    }

    /// Seed a collection / function / single table with `n` LTDVs,
    /// returned in chronological order.
    async fn seed_ftdv_timeline(
        db: &TdPool,
        n: usize,
    ) -> (
        ActiveCollectionDB,
        FunctionDB,
        TableId,
        Vec<LogicalTableDataVersionInsertDB>,
    ) {
        let p = db.seed().test_project().await.unwrap();
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
            .await
            .unwrap();

        let table_name = TableNameDto::try_from("t0").unwrap();
        let create = FunctionRegister {
            name: "f0".try_into().unwrap(),
            description: "d".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "s".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![table_name.clone()].into(),
            table_types: None,
            runtime_values: "rt".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await
            .unwrap();

        let table_name_typed = TableName::try_from(&table_name).unwrap();
        let table = db
            .scd2()
            .fetch_first::<TableDB>(and!(&collection.id, &table_name_typed))
            .await
            .unwrap()
            .expect("row must exist");

        let mut ltdvs = Vec::with_capacity(n);
        for _ in 0..n {
            let execution = db.scoped(p.id).seed().execution(&function).await.unwrap();
            let transaction = db
                .scoped(p.id)
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
                .await
                .unwrap();

            let ltdv = LogicalTableDataVersionInsertDB {
                id: LogicalTableDataVersionId::new(),
                collection_id: collection.id,
                execution_id: execution.base.id.clone(),
                transaction_id: transaction.base.id.clone(),
                table_id: table.base.table_id,
                producer_logical_function_run_id: None,
                function_param_pos: None,
                private: Private::FALSE,
                triggered_on: execution.base.triggered_on,
                triggered_by_id: execution.base.triggered_by_id,
                group_id: collection.group_id,
            };

            insert_ftdv(db, &collection.project_id, &ltdv).await;
            ltdvs.push(ltdv);
        }
        (collection, function, table.base.table_id, ltdvs)
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_none(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
        let triggered_on = AtTime::now();

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::None)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[0].id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_single_head(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

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
                .triggered_on(&AtTime::now())
                .build()?
                .resolve(&mut conn)
                .await?;
            assert_eq!(found.len(), 1);
            match expected {
                Some(ltdv) => assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdv.id),
                None => assert!(found[0].is_none()),
            }
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_single_initial(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

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
                .triggered_on(&AtTime::now())
                .build()?
                .resolve(&mut conn)
                .await?;
            assert_eq!(found.len(), 1);
            match expected {
                Some(ltdv) => assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdv.id),
                None => assert!(found[0].is_none()),
            }
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_list_relative(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

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
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 6);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[1].id);
        assert_eq!(found[1].as_ref().unwrap().base.base.id, ltdvs[0].id);
        assert!(found[2].is_none());
        assert_eq!(found[3].as_ref().unwrap().base.base.id, ltdvs[0].id);
        assert_eq!(found[4].as_ref().unwrap().base.base.id, ltdvs[1].id);
        assert!(found[5].is_none());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_range_head(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let versions = Versions::Range(Version::Head(-2), Version::Head(0));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[0].id);
        assert_eq!(found[1].as_ref().unwrap().base.base.id, ltdvs[1].id);
        assert_eq!(found[2].as_ref().unwrap().base.base.id, ltdvs[2].id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_range_descending(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, _ftdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let versions = Versions::Range(Version::Head(-1), Version::Head(-3));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert!(found.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_range_initial(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let versions = Versions::Range(Version::Initial(0), Version::Initial(2));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[0].id);
        assert_eq!(found[1].as_ref().unwrap().base.base.id, ltdvs[1].id);
        assert_eq!(found[2].as_ref().unwrap().base.base.id, ltdvs[2].id);
        Ok(())
    }

    /// Mixed-side bounds don't define a deterministic range size, so the
    /// resolver returns the contiguous slice as-is without padding.
    #[td_test::test(db)]
    async fn test_resolve_logical_range_mixed_bounds(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let versions = Versions::Range(Version::Initial(0), Version::Head(0));
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[0].id);
        assert_eq!(found[2].as_ref().unwrap().base.base.id, ltdvs[2].id);
        Ok(())
    }

    /// With no prior execution, `Versions::New` returns the whole timeline.
    #[td_test::test(db)]
    async fn test_resolve_logical_new_no_prior_execution(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::New)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 3);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[0].id);
        assert_eq!(found[2].as_ref().unwrap().base.base.id, ltdvs[2].id);
        Ok(())
    }

    /// `Versions::New` for a downstream consumer (never the plan's trigger)
    /// returns just the delta since its prior LFR — not the whole timeline.
    #[td_test::test(db)]
    async fn test_resolve_logical_new_downstream_consumer(db: TdPool) -> Result<(), TdError> {
        let (collection, _publisher, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;

        let consumer_create = FunctionRegister {
            name: "c0".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("c0_out")?].into(),
            table_types: None,
            runtime_values: "rt".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let consumer = db
            .scoped(collection.project_id)
            .seed()
            .function(&collection, &consumer_create)
            .await?;

        // Consumer LFR aligned with run 1 is the resolution cutoff.
        let lfr_run1 = LogicalFunctionRunInsertDB {
            id: LogicalFunctionRunId::new(),
            collection_id: collection.id,
            execution_id: ltdvs[0].execution_id.clone(),
            transaction_id: ltdvs[0].transaction_id.clone(),
            function_id: consumer.base.function_id,
            function_version_id: consumer.base.id,
            trigger: Trigger::Dependency,
            triggered_on: ltdvs[0].triggered_on,
            triggered_by_id: UserId::admin(),
            status: LogicalFunctionRunStatus::Committed,
            group_id: collection.group_id,
        };
        db.scoped(collection.project_id)
            .insert::<LogicalFunctionRunInsertDB>(&lfr_run1)
            .await?;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        let found = VersionResolver::builder()
            .function_id(&consumer.base.function_id)
            .table_id(&table_id)
            .versions(&Versions::New)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;

        let found_ids: Vec<_> = found
            .iter()
            .map(|f| f.as_ref().map(|f| f.base.base.id.clone()))
            .collect();
        assert_eq!(
            found.len(),
            1,
            "downstream consumer should see only the LTDV minted after its prior run, got {found_ids:?}",
        );
        assert_eq!(found[0].as_ref().unwrap().base.base.id, ltdvs[1].id);
        Ok(())
    }

    /// LTDVs partition by `table_id`: a resolver call against table A must
    /// not surface LTDVs minted for table B.
    #[td_test::test(db)]
    async fn test_resolve_logical_partition_isolation(db: TdPool) -> Result<(), TdError> {
        let (collection, _function, table_a_id, ftdvs_a) = seed_ftdv_timeline(&db, 2).await;

        let table_b_name = TableNameDto::try_from("table_b")?;
        let table_b_create = FunctionRegister {
            name: "f_b".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![table_b_name.clone()].into(),
            table_types: None,
            runtime_values: "rt".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function_b = db
            .scoped(collection.project_id)
            .seed()
            .function(&collection, &table_b_create)
            .await?;
        let table_b_typed = TableName::try_from(&table_b_name)?;
        let table_b = db
            .scd2()
            .fetch_first::<TableDB>(and!(&collection.id, &table_b_typed))
            .await?
            .expect("row must exist");
        for _ in 0..2 {
            let execution = db
                .scoped(collection.project_id)
                .seed()
                .execution(&function_b)
                .await?;
            let transaction = db
                .scoped(collection.project_id)
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY")?)
                .await?;
            let ltdv = LogicalTableDataVersionInsertDB {
                id: LogicalTableDataVersionId::new(),
                collection_id: collection.id,
                execution_id: execution.base.id.clone(),
                transaction_id: transaction.base.id.clone(),
                table_id: table_b.base.table_id,
                producer_logical_function_run_id: None,
                function_param_pos: None,
                private: Private::FALSE,
                triggered_on: execution.base.triggered_on,
                triggered_by_id: execution.base.triggered_by_id,
                group_id: collection.group_id,
            };
            insert_ftdv(&db, &collection.project_id, &ltdv).await;
        }

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_a_id)
            .versions(&Versions::Range(Version::Initial(0), Version::Head(0)))
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 2);
        let ids: Vec<_> = found
            .iter()
            .map(|f| f.as_ref().unwrap().base.base.id.clone())
            .collect();
        assert!(ids.contains(&ftdvs_a[0].id));
        assert!(ids.contains(&ftdvs_a[1].id));
        for ltdv in found.iter().flatten() {
            assert_eq!(ltdv.base.base.table_id, table_a_id);
        }
        Ok(())
    }

    /// LTDVs with no materialization remain visible to the resolver. The
    /// planner pins whatever is the latest at `triggered_on`; the gate
    /// against running on unmaterialized inputs lives at dispatch.
    #[td_test::test(db)]
    async fn test_resolve_ignores_materialization_state(db: TdPool) -> Result<(), TdError> {
        let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::Single(Version::Head(0)))
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        let ltdv = found[0].as_ref().unwrap();
        assert_eq!(ltdv.base.base.id, ltdvs[1].id);
        // The resolver pins the latest logical version whether or not a
        // materialization exists for it; translating to one is the
        // caller's step.
        let current: Option<PhysicalTableDataVersionDB> = conn
            .scd2()
            .fetch_optional::<PhysicalTableDataVersionDB>(&ltdv.base.base.id)
            .await?;
        assert!(current.is_none(), "no materialization was seeded");
        Ok(())
    }

    /// Seed a Canceled-producer LTDV for `table_id` — the Canceled producer
    /// is what makes the active view hide the returned LTDV.
    async fn seed_canceled_ltdv(
        db: &TdPool,
        collection: &ActiveCollectionDB,
        function: &FunctionDB,
        table_id: TableId,
    ) -> LogicalTableDataVersionInsertDB {
        let execution = db
            .scoped(collection.project_id)
            .seed()
            .execution(function)
            .await
            .unwrap();
        let transaction = db
            .scoped(collection.project_id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
            .await
            .unwrap();
        let canceled_lfr = LogicalFunctionRunInsertDB {
            id: LogicalFunctionRunId::new(),
            collection_id: collection.id,
            execution_id: execution.base.id.clone(),
            transaction_id: transaction.base.id.clone(),
            function_id: function.base.function_id,
            function_version_id: function.base.id.clone(),
            trigger: Trigger::Manual,
            triggered_on: TriggeredOn::asserted(*AtTime::now()),
            triggered_by_id: UserId::admin(),
            status: LogicalFunctionRunStatus::Canceled,
            group_id: collection.group_id,
        };
        db.scoped(collection.project_id)
            .insert::<LogicalFunctionRunInsertDB>(&canceled_lfr)
            .await
            .unwrap();
        let canceled_ltdv = LogicalTableDataVersionInsertDB {
            id: LogicalTableDataVersionId::new(),
            collection_id: collection.id,
            execution_id: execution.base.id,
            transaction_id: transaction.base.id,
            table_id,
            producer_logical_function_run_id: Some(canceled_lfr.id.clone()),
            function_param_pos: None,
            private: Private::FALSE,
            triggered_on: canceled_lfr.triggered_on,
            triggered_by_id: canceled_lfr.triggered_by_id,
            group_id: collection.group_id,
        };
        db.scoped(collection.project_id)
            .insert::<LogicalTableDataVersionInsertDB>(&canceled_ltdv)
            .await
            .unwrap();
        canceled_ltdv
    }

    #[td_test::test(db)]
    async fn test_resolve_logical_skips_canceled_producer(db: TdPool) -> Result<(), TdError> {
        let (collection, function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
        let good = &ltdvs[0];

        seed_canceled_ltdv(&db, &collection, &function, table_id).await;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::None)
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].as_ref().unwrap().base.base.id,
            good.id,
            "canceled-producer version must disappear; HEAD falls back to the previous live version",
        );
        Ok(())
    }

    /// A `Fixed` pin is filtered too (not exempt from the active view).
    #[td_test::test(db)]
    async fn test_resolve_logical_fixed_skips_canceled_producer(db: TdPool) -> Result<(), TdError> {
        let (collection, function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
        let good = &ltdvs[0];

        let canceled_ltdv = seed_canceled_ltdv(&db, &collection, &function, table_id).await;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());

        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::Single(Version::Fixed(good.id.clone())))
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].as_ref().unwrap().base.base.id, good.id);

        let err = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::Single(Version::Fixed(canceled_ltdv.id.clone())))
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await
            .unwrap_err();
        assert!(matches!(
            err.domain_err::<VersionResolverError>(),
            VersionResolverError::FixedTableDataVersionsNotFound(_)
        ));
        Ok(())
    }
}
