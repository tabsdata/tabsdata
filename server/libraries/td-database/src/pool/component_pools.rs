//
// Copyright 2026 Tabsdata Inc.
//

//! [`ComponentPools`] — one component's serving pools: the requests' and,
//! where the component declares one, the system's.

use crate::pool::PoolKind;
use crate::pool::TdPool;

#[derive(Debug, Clone)]
pub struct ComponentPools {
    pub regular: TdPool,
    pub system: Option<TdPool>,
}

impl ComponentPools {
    pub fn get(&self, kind: PoolKind) -> &TdPool {
        match kind {
            PoolKind::Regular => &self.regular,
            PoolKind::System => self.system.as_ref().unwrap_or(&self.regular),
        }
    }

    /// Close every connected pool — on shutdown.
    pub async fn shutdown(&self) {
        self.regular.shutdown().await;
        if let Some(system) = &self.system {
            system.shutdown().await;
        }
    }
}
