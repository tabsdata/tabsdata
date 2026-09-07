//
// Copyright 2026 Tabsdata Inc.
//

//! [`DomainPools`] — the serving pools of every registered component, keyed by
//! domain, with the per-mode lookup services reach them through.

use crate::access::AccessMode;
#[cfg(any(test, feature = "test-utils"))]
use crate::domain::Apiserver;
#[cfg(any(test, feature = "test-utils"))]
use crate::domain::DbDomain;
use crate::domain::Supports;
use crate::error::DbError;
use crate::pool::ComponentPools;
use crate::pool::TdPool;
use std::collections::BTreeMap;

/// A service's pools as one parameter, keyed by domain (built at boot via
/// [`ServingPools::domain_pools`](crate::components::ServingPools::domain_pools)).
#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Default))]
pub struct DomainPools(BTreeMap<&'static str, ComponentPools>);

impl DomainPools {
    pub(crate) fn from_map(map: BTreeMap<&'static str, ComponentPools>) -> Self {
        Self(map)
    }

    /// Test-only assembly, one domain at a time: bless `pool` as `D`'s (every
    /// declared mode slot) — the caller asserts the provenance.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with<D: DbDomain>(mut self, pool: TdPool) -> Self {
        let mut pools = ComponentPools::default();
        for kind in D::POOLS {
            pools.set(*kind, pool.clone());
        }
        self.0.insert(D::SCHEMA_KEY, pools);
        self
    }

    /// Test-only assembly from a bare apiserver pool, filling every mode slot
    /// (no production sizing or read-only posture).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn of_apiserver(pool: TdPool) -> Self {
        Self::default().with::<Apiserver>(pool)
    }

    /// Mode `M`'s pool within domain `D` — reachable only for modes the
    /// domain declares. Errors only if the deployment never registered `D`'s
    /// component.
    pub(crate) fn get<M: AccessMode, D: Supports<M>>(&self) -> Result<&TdPool, DbError> {
        let pools = self
            .0
            .get(D::SCHEMA_KEY)
            .ok_or_else(|| DbError::UnknownSchemaEntry(D::SCHEMA_KEY.to_owned()))?;
        pools
            .get(M::KIND)
            .ok_or(DbError::MissingComponentPool(D::SCHEMA_KEY, M::KIND))
    }
}
