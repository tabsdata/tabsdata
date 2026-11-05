//
// Copyright 2026 Tabsdata Inc.
//

//! [`DomainPools`] — the serving pools of every registered component, keyed by
//! domain, with the per-mode lookup services reach them through.

use crate::access::lock::{InProcessLocker, Locker};
#[cfg(any(test, feature = "test-utils"))]
use crate::domain::Apiserver;
use crate::domain::DbDomain;
use crate::error::DbError;
use crate::pool::{ComponentPools, PoolKind, TdPool};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A service's pools as one parameter, keyed by domain (built at boot via
/// [`ServingPools::domain_pools`](crate::components::ServingPools::domain_pools)),
/// and the [`Locker`] their units lock with.
#[derive(Clone)]
pub struct DomainPools {
    pools: BTreeMap<&'static str, ComponentPools>,
    locker: Arc<dyn Locker>,
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for DomainPools {
    fn default() -> Self {
        Self::from_map(BTreeMap::new())
    }
}

impl DomainPools {
    pub(crate) fn from_map(pools: BTreeMap<&'static str, ComponentPools>) -> Self {
        Self {
            pools,
            locker: Arc::new(InProcessLocker),
        }
    }

    /// How many connections domain `D`'s pool of `kind` holds at most;
    /// `None` if the deployment never registered `D`'s component.
    pub fn max_connections<D: DbDomain>(&self, kind: PoolKind) -> Option<u32> {
        self.get::<D>(kind)
            .ok()
            .map(|pool| pool.pool.options().get_max_connections())
    }

    pub(crate) fn locker(&self) -> &dyn Locker {
        self.locker.as_ref()
    }

    /// Test-only assembly, one domain at a time: bless `pool` as `D`'s, for
    /// every kind — the caller asserts the provenance.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with<D: DbDomain>(mut self, pool: TdPool) -> Self {
        self.pools.insert(
            D::SCHEMA_KEY,
            ComponentPools {
                regular: pool,
                system: None,
            },
        );
        self
    }

    /// Test-only assembly from a bare apiserver pool (no production sizing or
    /// read-only posture).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn of_apiserver(pool: TdPool) -> Self {
        Self::default().with::<Apiserver>(pool)
    }

    /// Domain `D`'s pool of `kind`. Errors only if the deployment never
    /// registered `D`'s component.
    pub(crate) fn get<D: DbDomain>(&self, kind: PoolKind) -> Result<&TdPool, DbError> {
        self.pools
            .get(D::SCHEMA_KEY)
            .map(|pools| pools.get(kind))
            .ok_or_else(|| DbError::UnknownSchemaEntry(D::SCHEMA_KEY.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    /// Never connects: the tests only ask which pool a kind draws from.
    fn pool(component: &'static str, max: u32) -> TdPool {
        TdPool {
            pool: PgPoolOptions::new()
                .max_connections(max)
                .connect_lazy("postgres://127.0.0.1/none")
                .unwrap(),
            component,
            acquire_timeout: Duration::from_secs(1),
        }
    }

    fn pools(system: Option<TdPool>) -> DomainPools {
        DomainPools::from_map(BTreeMap::from([(
            Apiserver::SCHEMA_KEY,
            ComponentPools {
                regular: pool("regular", 10),
                system,
            },
        )]))
    }

    fn drawn_from(pools: &DomainPools, kind: PoolKind) -> &'static str {
        pools.get::<Apiserver>(kind).unwrap().component
    }

    #[tokio::test]
    async fn each_kind_draws_from_its_pool() {
        let pools = pools(Some(pool("system", 4)));
        assert_eq!(drawn_from(&pools, PoolKind::Regular), "regular");
        assert_eq!(drawn_from(&pools, PoolKind::System), "system");
        assert_eq!(
            pools.max_connections::<Apiserver>(PoolKind::Regular),
            Some(10)
        );
        assert_eq!(
            pools.max_connections::<Apiserver>(PoolKind::System),
            Some(4)
        );
    }

    #[tokio::test]
    async fn without_a_system_pool_the_system_kind_draws_from_the_regular_one() {
        let pools = pools(None);
        assert_eq!(drawn_from(&pools, PoolKind::System), "regular");
    }
}
