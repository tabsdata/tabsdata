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
//! | `New`                 | LTDVs minted after the requesting function's last not-Canceled/Yanked run   |
//!
//! ### Canceled/Yanked-produced LTDVs disappear
//!
//! The planner pins the latest LTDV whatever its producer's status; the
//! scheduler's readiness and initial-status queries judge what that status
//! means. A Canceled/Yanked *producer* produced no data, so its LTDV
//! disappears: all reads go through `logical_table_data_versions__active`, so
//! `Head(0)` falls back to the previous live version and a `Fixed` pin to
//! such an LTDV resolves to `FixedTableDataVersionsNotFound`.

use td_database::access::{AccessMode, SeesProjectRows};

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

    /// What `Versions::None` resolves to, for each of `tables` at once: its
    /// newest active version at `triggered_on`, when it has one.
    pub async fn resolve_heads<S: SeesProjectRows, M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, S, M>,
        tables: &[TableId],
        triggered_on: &AtTime,
    ) -> Result<HashMap<TableId, ActiveLogicalTableDataVersionDB>, TdError> {
        let heads: Vec<ActiveLogicalTableDataVersionDB> = conn
            .timeline()
            .heads_at(triggered_on, tables)
            .fetch_all()
            .await?;
        Ok(heads
            .into_iter()
            .map(|ltdv| (ltdv.table_id, ltdv))
            .collect())
    }

    /// Resolves the requested versions to LTDV rows.
    ///
    /// Returns one entry per requested version; the entry is `None` when
    /// the requested coordinate doesn't exist (e.g. an `Initial(0)` on an
    /// empty timeline).
    pub async fn resolve<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
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
    async fn fetch_ftdv_for_fixed<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
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
    async fn fixed_to_head_offset<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
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

    async fn resolve_list<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
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
                    .fetch_all::<ActiveLogicalTableDataVersionDB>(&Versions::Range(
                        min.clone(),
                        max.clone(),
                    ))
                    .await?;
                if reverse {
                    found.reverse();
                }
                // `found` starts at the newest head or the oldest initial asked for.
                let map: HashMap<_, _> = versions
                    .iter()
                    .map(|(i, v)| {
                        let pos = match (v, &min, &max) {
                            (Version::Head(back), _, Version::Head(newest)) => newest - back,
                            (Version::Initial(forward), Version::Initial(oldest), _) => {
                                forward - oldest
                            }
                            _ => unreachable!(),
                        };
                        (*i, found.get(pos as usize).cloned())
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
    async fn resolve_range<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
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
    /// previous run that was not Canceled or Yanked, so such a run's
    /// versions go to the next run. Cutoff is the prior LFR for
    /// `self.function_id` —
    /// LFRs partition on the function actually running, while `executions`
    /// partitions on the plan's trigger function, so only the LFR source
    /// surfaces a downstream consumer's prior runs.
    async fn resolve_new<S: SeesProjectRows, M: AccessMode>(
        &self,
        conn: &mut DomainConnection<Apiserver, S, M>,
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
                    .at(&last.triggered_on, table_id)
                    .fetch_optional::<ActiveLogicalTableDataVersionDB>(&Versions::None)
                    .await?
            }
            None => None,
        };

        let resolved = match last_logical {
            Some(boundary) => {
                // Strictly newer than the boundary: range from one-newer to Head(0).
                let boundary_offset = self.fixed_to_head_offset(conn, &boundary.id).await?;
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
mod tests;
