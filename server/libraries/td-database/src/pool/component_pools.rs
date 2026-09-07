//
// Copyright 2026 Tabsdata Inc.
//

//! [`ComponentPools`] — one component's serving pools, one per pool kind it
//! declares.

use crate::pool::PoolKind;
use crate::pool::TdPool;

/// The serving pools of one schema component — a slot per pool kind,
/// populated exactly for the kinds the component declares
/// ([`DbDomain::POOLS`](crate::domain::DbDomain::POOLS)).
#[derive(Debug, Clone, Default)]
pub struct ComponentPools {
    pub regular: Option<TdPool>,
    pub exclusive: Option<TdPool>,
}

impl ComponentPools {
    pub fn get(&self, kind: PoolKind) -> Option<&TdPool> {
        match kind {
            PoolKind::Regular => self.regular.as_ref(),
            PoolKind::Exclusive => self.exclusive.as_ref(),
        }
    }

    pub(crate) fn set(&mut self, kind: PoolKind, pool: TdPool) {
        match kind {
            PoolKind::Regular => self.regular = Some(pool),
            PoolKind::Exclusive => self.exclusive = Some(pool),
        }
    }

    /// Close every connected pool — on shutdown.
    pub async fn shutdown(&self) {
        for pool in [&self.regular, &self.exclusive].into_iter().flatten() {
            pool.shutdown().await;
        }
    }
}
